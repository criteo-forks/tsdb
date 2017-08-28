// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
)

var (
	// ErrNotFound is returned if a looked up resource was not found.
	ErrNotFound = errors.Errorf("not found")

	// ErrOutOfOrderSample is returned if an appended sample has a
	// timestamp larger than the most recent sample.
	ErrOutOfOrderSample = errors.New("out of order sample")

	// ErrAmendSample is returned if an appended sample has the same timestamp
	// as the most recent sample but a different value.
	ErrAmendSample = errors.New("amending sample")

	// ErrOutOfBounds is returned if an appended sample is out of the
	// writable time range.
	ErrOutOfBounds = errors.New("out of bounds")
)

// Head handles reads and writes of time series data within a time window.
type Head struct {
	mtx sync.RWMutex

	lowTimestamp  int64
	highTimestamp int64

	// descs holds all chunk descs for the head block. Each chunk implicitly
	// is assigned the index as its ID.
	series []*memSeries
	// hashes contains a collision map of label set hashes of chunks
	// to their chunk descs.
	hashes map[uint64][]*memSeries

	symbols  map[string]struct{}
	values   map[string]stringset // label names to possible values
	postings *memPostings         // postings lists for terms

	tombstones tombstoneReader
}

// NewHead opens the head block in dir.
func NewHead(l log.Logger, wal WALReader) (*Head, error) {
	h := &Head{
		series:     []*memSeries{nil}, // 0 is not a valid posting, filled with nil.
		hashes:     map[uint64][]*memSeries{},
		values:     map[string]stringset{},
		symbols:    map[string]struct{}{},
		postings:   &memPostings{m: make(map[term][]uint32)},
		tombstones: newEmptyTombstoneReader(),
	}
	return h, h.init(wal)
}

func (h *Head) init(r WALReader) error {
	seriesFunc := func(series []labels.Labels) error {
		for _, lset := range series {
			h.create(lset.Hash(), lset)
		}

		return nil
	}
	samplesFunc := func(samples []RefSample) error {
		for _, s := range samples {
			if int(s.Ref) >= len(h.series) {
				return errors.Errorf("unknown series reference %d (max %d); abort WAL restore",
					s.Ref, len(h.series))
			}
			h.series[s.Ref].append(s.T, s.V)
		}

		return nil
	}
	deletesFunc := func(stones []Stone) error {
		for _, s := range stones {
			for _, itv := range s.intervals {
				h.tombstones.add(s.ref, itv)
			}
		}

		return nil
	}

	if err := r.Read(seriesFunc, samplesFunc, deletesFunc); err != nil {
		return errors.Wrap(err, "consume WAL")
	}

	return nil
}

// Tombstones returns the TombstoneReader against the block.
func (h *Head) Tombstones() TombstoneReader {
	return h.tombstones
}

// Delete implements headBlock.
func (h *Head) Delete(mint int64, maxt int64, ms ...labels.Matcher) error {
	ir := h.Index()

	pr := newPostingsReader(ir)
	p, absent := pr.Select(ms...)

	var stones []Stone

Outer:
	for p.Next() {
		ref := p.At()
		lset := h.series[ref].lset
		for _, abs := range absent {
			if lset.Get(abs) != "" {
				continue Outer
			}
		}

		// Delete only until the current values and not beyond.
		tmin, tmax := clampInterval(mint, maxt, h.series[ref].chunks[0].minTime, h.series[ref].head().maxTime)
		stones = append(stones, Stone{ref, Intervals{{tmin, tmax}}})
	}

	if p.Err() != nil {
		return p.Err()
	}
	for _, s := range stones {
		h.tombstones.add(s.ref, s.intervals[0])
	}

	return nil
}

// Index returns an IndexReader against the block.
func (h *Head) Index() IndexReader {
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	return &headIndexReader{head: h, maxSeries: uint32(len(h.series) - 1)}
}

// Chunks returns a ChunkReader against the block.
func (h *Head) Chunks() ChunkReader { return &headChunkReader{head: h} }

// HighTimestamp returns the highest inserted sample timestamp.
func (h *Head) HighTimestamp() int64 {
	return atomic.LoadInt64(&h.highTimestamp)
}

type headChunkReader struct {
	head *Head
}

func (h *headChunkReader) Close() error {
	return nil
}

// Chunk returns the chunk for the reference number.
func (h *headChunkReader) Chunk(ref uint64) (chunks.Chunk, error) {
	h.head.mtx.RLock()
	defer h.head.mtx.RUnlock()

	si := ref >> 32
	ci := (ref << 32) >> 32

	s := h.head.series[si]
	c := s.chunks[ci]

	// Chunks before the lowest timestamp are remains of already compacted
	// chunks that have not been GC'd yet. Hide them.
	if c.minTime < h.head.lowTimestamp {
		return nil, ErrNotFound
	}

	return &safeChunk{
		Chunk: c.chunk,
		s:     s,
		i:     int(ci),
	}, nil
}

type safeChunk struct {
	chunks.Chunk
	s *memSeries
	i int
}

func (c *safeChunk) Iterator() chunks.Iterator {
	c.s.mtx.RLock()
	defer c.s.mtx.RUnlock()
	return c.s.iterator(c.i)
}

// func (c *safeChunk) Appender() (chunks.Appender, error) { panic("illegal") }
// func (c *safeChunk) Bytes() []byte                      { panic("illegal") }
// func (c *safeChunk) Encoding() chunks.Encoding          { panic("illegal") }

type headIndexReader struct {
	head *Head
	// Highest series that existed when the index reader was instantiated.
	maxSeries uint32
}

func (h *headIndexReader) Close() error {
	return nil
}

func (h *headIndexReader) Symbols() (map[string]struct{}, error) {
	return h.head.symbols, nil
}

// LabelValues returns the possible label values
func (h *headIndexReader) LabelValues(names ...string) (StringTuples, error) {
	h.head.mtx.RLock()
	defer h.head.mtx.RUnlock()

	if len(names) != 1 {
		return nil, errInvalidSize
	}
	var sl []string

	for s := range h.head.values[names[0]] {
		sl = append(sl, s)
	}
	sort.Strings(sl)

	return &stringTuples{l: len(names), s: sl}, nil
}

// Postings returns the postings list iterator for the label pair.
func (h *headIndexReader) Postings(name, value string) (Postings, error) {
	h.head.mtx.RLock()
	defer h.head.mtx.RUnlock()

	return h.head.postings.get(term{name: name, value: value}), nil
}

func (h *headIndexReader) SortedPostings(p Postings) Postings {
	h.head.mtx.RLock()
	defer h.head.mtx.RUnlock()

	ep := make([]uint32, 0, 1024)

	for p.Next() {
		// Skip posting entries that include series added after we
		// instantiated the index reader.
		if p.At() > h.maxSeries {
			break
		}
		ep = append(ep, p.At())
	}
	if err := p.Err(); err != nil {
		return errPostings{err: errors.Wrap(err, "expand postings")}
	}

	sort.Slice(ep, func(i, j int) bool {
		return labels.Compare(h.head.series[ep[i]].lset, h.head.series[ep[j]].lset) < 0
	})
	return newListPostings(ep)
}

// Series returns the series for the given reference.
func (h *headIndexReader) Series(ref uint32, lbls *labels.Labels, chks *[]ChunkMeta) error {
	h.head.mtx.RLock()
	defer h.head.mtx.RUnlock()

	if ref > h.maxSeries {
		return ErrNotFound
	}

	s := h.head.series[ref]
	if s == nil {
		return ErrNotFound
	}
	*lbls = append((*lbls)[:0], s.lset...)

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	*chks = (*chks)[:0]

	for i, c := range s.chunks {
		// Chunks before the lowest timestamp are remains of already compacted
		// chunks that have not been GC'd yet. Hide them.
		if c.minTime < h.head.lowTimestamp {
			continue
		}
		*chks = append(*chks, ChunkMeta{
			MinTime: c.minTime,
			MaxTime: c.maxTime,
			Ref:     (uint64(ref) << 32) | uint64(i),
		})
	}

	return nil
}

func (h *headIndexReader) LabelIndices() ([][]string, error) {
	h.head.mtx.RLock()
	defer h.head.mtx.RUnlock()

	res := [][]string{}

	for s := range h.head.values {
		res = append(res, []string{s})
	}
	return res, nil
}

// get retrieves the chunk with the hash and label set and creates
// a new one if it doesn't exist yet.
func (h *Head) get(hash uint64, lset labels.Labels) *memSeries {
	series := h.hashes[hash]

	for _, s := range series {
		if s.lset.Equals(lset) {
			return s
		}
	}
	return nil
}

func (h *Head) create(hash uint64, lset labels.Labels) *memSeries {
	s := newMemSeries(lset, uint32(len(h.series)))

	// Allocate empty space until we can insert at the given index.
	h.series = append(h.series, s)

	h.hashes[hash] = append(h.hashes[hash], s)

	for _, l := range lset {
		valset, ok := h.values[l.Name]
		if !ok {
			valset = stringset{}
			h.values[l.Name] = valset
		}
		valset.set(l.Value)

		h.postings.add(s.ref, term{name: l.Name, value: l.Value})

		h.symbols[l.Name] = struct{}{}
		h.symbols[l.Value] = struct{}{}
	}

	h.postings.add(s.ref, term{})

	return s
}

type sample struct {
	t int64
	v float64
}

type memSeries struct {
	mtx sync.RWMutex

	ref    uint32
	lset   labels.Labels
	chunks []*memChunk

	nextAt    int64 // timestamp at which to cut the next chunk.
	maxt      int64 // maximum timestamp for the series.
	lastValue float64
	sampleBuf [4]sample

	app chunks.Appender // Current appender for the chunk.
}

func (s *memSeries) cut(mint int64) *memChunk {
	c := &memChunk{
		chunk:   chunks.NewXORChunk(),
		minTime: mint,
		maxTime: math.MinInt64,
	}
	s.chunks = append(s.chunks, c)

	app, err := c.chunk.Appender()
	if err != nil {
		panic(err)
	}
	s.app = app
	return c
}

func newMemSeries(lset labels.Labels, id uint32) *memSeries {
	s := &memSeries{
		lset:   lset,
		ref:    id,
		nextAt: math.MinInt64,
	}
	return s
}

func (s *memSeries) append(t int64, v float64) bool {
	const samplesPerChunk = 120

	s.mtx.Lock()
	defer s.mtx.Unlock()

	var c *memChunk

	if len(s.chunks) == 0 {
		c = s.cut(t)
	}
	c = s.head()
	if c.maxTime >= t {
		return false
	}
	if c.samples > samplesPerChunk/4 && t >= s.nextAt {
		c = s.cut(t)
	}
	s.app.Append(t, v)

	c.maxTime = t
	c.samples++

	if c.samples == samplesPerChunk/4 {
		s.nextAt = computeChunkEndTime(c.minTime, c.maxTime, s.maxt)
	}

	s.lastValue = v

	s.sampleBuf[0] = s.sampleBuf[1]
	s.sampleBuf[1] = s.sampleBuf[2]
	s.sampleBuf[2] = s.sampleBuf[3]
	s.sampleBuf[3] = sample{t: t, v: v}

	return true
}

// computeChunkEndTime estimates the end timestamp based the beginning of a chunk,
// its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
func computeChunkEndTime(start, cur, max int64) int64 {
	a := (max - start) / ((cur - start + 1) * 4)
	if a == 0 {
		return max
	}
	return start + (max-start)/a
}

func (s *memSeries) iterator(i int) chunks.Iterator {
	c := s.chunks[i]

	if i < len(s.chunks)-1 {
		return c.chunk.Iterator()
	}

	it := &memSafeIterator{
		Iterator: c.chunk.Iterator(),
		i:        -1,
		total:    c.samples,
		buf:      s.sampleBuf,
	}
	return it
}

func (s *memSeries) head() *memChunk {
	return s.chunks[len(s.chunks)-1]
}

type memChunk struct {
	chunk            chunks.Chunk
	minTime, maxTime int64
	samples          int
}

type memSafeIterator struct {
	chunks.Iterator

	i     int
	total int
	buf   [4]sample
}

func (it *memSafeIterator) Next() bool {
	if it.i+1 >= it.total {
		return false
	}
	it.i++
	if it.total-it.i > 4 {
		return it.Iterator.Next()
	}
	return true
}

func (it *memSafeIterator) At() (int64, float64) {
	if it.total-it.i > 4 {
		return it.Iterator.At()
	}
	s := it.buf[4-(it.total-it.i)]
	return s.t, s.v
}
