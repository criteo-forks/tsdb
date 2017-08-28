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

// Package tsdb implements a time series storage for float64 sample data.
package tsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sync/errgroup"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/go-kit/kit/log"
	"github.com/nightlyone/lockfile"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
)

// DefaultOptions used for the DB. They are sane for setups using
// millisecond precision timestampdb.
var DefaultOptions = &Options{
	WALFlushInterval:  5 * time.Second,
	RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
	BlockRanges:       ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
	NoLockfile:        false,
}

// Options of the DB storage.
type Options struct {
	// The interval at which the write ahead log is flushed to disc.
	WALFlushInterval time.Duration

	// Duration of persisted data to keep.
	RetentionDuration uint64

	// The sizes of the Blocks.
	BlockRanges []int64

	// NoLockfile disables creation and consideration of a lock file.
	NoLockfile bool
}

// Appender allows appending a batch of data. It must be completed with a
// call to Commit or Rollback and must not be reused afterwards.
//
// Operations on the Appender interface are not goroutine-safe.
type Appender interface {
	// Add adds a sample pair for the given series. A reference number is
	// returned which can be used to add further samples in the same or later
	// transactions.
	// Returned reference numbers are ephemeral and may be rejected in calls
	// to AddFast() at any point. Adding the sample via Add() returns a new
	// reference number.
	// If the reference is the empty string it must not be used for caching.
	Add(l labels.Labels, t int64, v float64) (string, error)

	// Add adds a sample pair for the referenced series. It is generally faster
	// than adding a sample by providing its full label set.
	AddFast(ref string, t int64, v float64) error

	// Commit submits the collected samples and purges the batch.
	Commit() error

	// Rollback rolls back all modifications made in the appender so far.
	Rollback() error
}

// DB handles reads and writes of time series falling into
// a hashed partition of a seriedb.
type DB struct {
	dir   string
	lockf *lockfile.Lockfile

	logger     log.Logger
	metrics    *dbMetrics
	opts       *Options
	chunkPool  chunks.Pool
	appendPool sync.Pool
	compactor  Compactor
	wal        WAL

	// Mutex for that must be held when modifying the general block layout.
	mtx    sync.RWMutex
	blocks []DiskBlock

	head *Head

	compactc chan struct{}
	donec    chan struct{}
	stopc    chan struct{}

	// cmtx is used to control compactions and deletions.
	cmtx               sync.Mutex
	compactionsEnabled bool
}

type dbMetrics struct {
	activeAppenders      prometheus.Gauge
	loadedBlocks         prometheus.GaugeFunc
	reloads              prometheus.Counter
	reloadsFailed        prometheus.Counter
	reloadDuration       prometheus.Summary
	samplesAppended      prometheus.Counter
	compactionsTriggered prometheus.Counter
}

func newDBMetrics(db *DB, r prometheus.Registerer) *dbMetrics {
	m := &dbMetrics{}

	m.activeAppenders = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tsdb_active_appenders",
		Help: "Number of currently active appender transactions",
	})
	m.loadedBlocks = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "tsdb_blocks_loaded",
		Help: "Number of currently loaded data blocks",
	}, func() float64 {
		db.mtx.RLock()
		defer db.mtx.RUnlock()
		return float64(len(db.blocks))
	})
	m.reloads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tsdb_reloads_total",
		Help: "Number of times the database reloaded block data from disk.",
	})
	m.reloadsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tsdb_reloads_failures_total",
		Help: "Number of times the database failed to reload black data from disk.",
	})
	m.reloadDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "tsdb_reload_duration_seconds",
		Help: "Duration of block reloads.",
	})
	m.samplesAppended = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tsdb_samples_appended_total",
		Help: "Total number of appended sampledb.",
	})
	m.compactionsTriggered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tsdb_compactions_triggered_total",
		Help: "Total number of triggered compactions for the partition.",
	})

	if r != nil {
		r.MustRegister(
			m.activeAppenders,
			m.loadedBlocks,
			m.reloads,
			m.reloadsFailed,
			m.reloadDuration,
			m.samplesAppended,
			m.compactionsTriggered,
		)
	}
	return m
}

// Open returns a new DB in the given directory.
func Open(dir string, l log.Logger, r prometheus.Registerer, opts *Options) (db *DB, err error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	if l == nil {
		l = log.NewLogfmtLogger(os.Stdout)
		l = log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	}
	if opts == nil {
		opts = DefaultOptions
	}

	wal, err := OpenSegmentWAL(filepath.Join(dir, "wal"), l, 10*time.Second)
	if err != nil {
		return nil, err
	}

	db = &DB{
		dir:                dir,
		logger:             l,
		opts:               opts,
		wal:                wal,
		compactc:           make(chan struct{}, 1),
		donec:              make(chan struct{}),
		stopc:              make(chan struct{}),
		compactionsEnabled: true,
		chunkPool:          chunks.NewPool(),
	}
	db.metrics = newDBMetrics(db, r)

	if !opts.NoLockfile {
		absdir, err := filepath.Abs(dir)
		if err != nil {
			return nil, err
		}
		lockf, err := lockfile.New(filepath.Join(absdir, "lock"))
		if err != nil {
			return nil, err
		}
		if err := lockf.TryLock(); err != nil {
			return nil, errors.Wrapf(err, "open DB in %s", dir)
		}
		db.lockf = &lockf
	}

	copts := &LeveledCompactorOptions{
		blockRanges: opts.BlockRanges,
		chunkPool:   db.chunkPool,
	}

	if len(copts.blockRanges) == 0 {
		return nil, errors.New("at least one block-range must exist")
	}

	for float64(copts.blockRanges[len(copts.blockRanges)-1])/float64(opts.RetentionDuration) > 0.2 {
		if len(copts.blockRanges) == 1 {
			break
		}
		// Max overflow is restricted to 20%.
		copts.blockRanges = copts.blockRanges[:len(copts.blockRanges)-1]
	}

	db.compactor = NewLeveledCompactor(r, l, copts)

	if err := db.reloadBlocks(); err != nil {
		return nil, err
	}
	db.head, err = NewHead(l, db.wal.Reader())
	if err != nil {
		return nil, err
	}

	go db.run()

	return db, nil
}

// Dir returns the directory of the database.
func (db *DB) Dir() string {
	return db.dir
}

func (db *DB) run() {
	defer close(db.donec)

	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			select {
			case db.compactc <- struct{}{}:
			default:
			}
		case <-db.compactc:
			db.metrics.compactionsTriggered.Inc()

			changes1, err := db.retentionCutoff()
			if err != nil {
				db.logger.Log("msg", "retention cutoff failed", "err", err)
			}

			changes2, err := db.compact()
			if err != nil {
				db.logger.Log("msg", "compaction failed", "err", err)
			}

			if changes1 || changes2 {
				if err := db.reloadBlocks(); err != nil {
					db.logger.Log("msg", "reloading blocks failed", "err", err)
				}
			}

		case <-db.stopc:
			return
		}
	}
}

func (db *DB) retentionCutoff() (bool, error) {
	if db.opts.RetentionDuration == 0 {
		return false, nil
	}

	db.mtx.RLock()
	defer db.mtx.RUnlock()

	if len(db.blocks) == 0 {
		return false, nil
	}

	last := db.blocks[len(db.blocks)-1]
	mint := last.Meta().MaxTime - int64(db.opts.RetentionDuration)

	return retentionCutoff(db.dir, mint)
}

func (db *DB) compact() (changes bool, err error) {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	if !db.compactionsEnabled {
		return false, nil
	}

	// Check whether we have pending head blocks that are ready to be persisted.
	// They have the highest priority.
	for {
		select {
		case <-db.stopc:
			return changes, nil
		default:
		}
		if db.head.lowTimestamp+(db.opts.BlockRanges[0]/2*3) <= db.head.highTimestamp {
			break
		}
		mint, maxt := rangeForTimestamp(db.head.lowTimestamp, db.opts.BlockRanges[0])

		if err = db.compactor.Write(db.dir, db.head, mint, maxt); err != nil {
			return changes, errors.Wrap(err, "persist head block")
		}
		db.head.lowTimestamp = maxt

		changes = true

		// TODO: front-truncate WAL
		runtime.GC()
	}

	// Check for compactions of multiple blocks.
	for {
		plan, err := db.compactor.Plan(db.dir)
		if err != nil {
			return changes, errors.Wrap(err, "plan compaction")
		}
		if len(plan) == 0 {
			break
		}

		select {
		case <-db.stopc:
			return changes, nil
		default:
		}

		if err := db.compactor.Compact(db.dir, plan...); err != nil {
			return changes, errors.Wrapf(err, "compact %s", plan)
		}
		changes = true

		for _, pd := range plan {
			if err := os.RemoveAll(pd); err != nil {
				return changes, errors.Wrap(err, "delete compacted block")
			}
		}
		runtime.GC()
	}

	return changes, nil
}

// retentionCutoff deletes all directories of blocks in dir that are strictly
// before mint.
func retentionCutoff(dir string, mint int64) (bool, error) {
	df, err := fileutil.OpenDir(dir)
	if err != nil {
		return false, errors.Wrapf(err, "open directory")
	}
	defer df.Close()

	dirs, err := blockDirs(dir)
	if err != nil {
		return false, errors.Wrapf(err, "list block dirs %s", dir)
	}

	changes := false

	for _, dir := range dirs {
		meta, err := readMetaFile(dir)
		if err != nil {
			return changes, errors.Wrapf(err, "read block meta %s", dir)
		}
		// The first block we encounter marks that we crossed the boundary
		// of deletable blocks.
		if meta.MaxTime >= mint {
			break
		}
		changes = true

		if err := os.RemoveAll(dir); err != nil {
			return changes, err
		}
	}

	return changes, fileutil.Fsync(df)
}

func (db *DB) getBlock(id ulid.ULID) (DiskBlock, bool) {
	for _, b := range db.blocks {
		if b.Meta().ULID == id {
			return b, true
		}
	}
	return nil, false
}

func (db *DB) reloadBlocks() (err error) {
	defer func(t time.Time) {
		if err != nil {
			db.metrics.reloadsFailed.Inc()
		}
		db.metrics.reloads.Inc()
		db.metrics.reloadDuration.Observe(time.Since(t).Seconds())
	}(time.Now())

	var cs []io.Closer
	defer func() { closeAll(cs...) }()

	db.mtx.Lock()
	defer db.mtx.Unlock()

	dirs, err := blockDirs(db.dir)
	if err != nil {
		return errors.Wrap(err, "find blocks")
	}
	var (
		blocks []DiskBlock
		exist  = map[ulid.ULID]struct{}{}
	)

	for _, dir := range dirs {
		meta, err := readMetaFile(dir)
		if err != nil {
			return errors.Wrapf(err, "read meta information %s", dir)
		}

		b, ok := db.getBlock(meta.ULID)
		if !ok {
			b, err = newPersistedBlock(dir, db.chunkPool)
			if err != nil {
				return errors.Wrapf(err, "open block %s", dir)
			}
		}

		blocks = append(blocks, b)
		exist[meta.ULID] = struct{}{}
	}

	if err := validateBlockSequence(blocks); err != nil {
		return errors.Wrap(err, "invalid block sequence")
	}

	// Close all opened blocks that no longer exist after we returned all locks.
	for _, b := range db.blocks {
		if _, ok := exist[b.Meta().ULID]; !ok {
			cs = append(cs, b)
		}
	}

	db.blocks = blocks

	return nil
}

func validateBlockSequence(bs []DiskBlock) error {
	if len(bs) == 0 {
		return nil
	}
	sort.Slice(bs, func(i, j int) bool {
		return bs[i].Meta().MinTime < bs[j].Meta().MinTime
	})
	prev := bs[0]
	for _, b := range bs[1:] {
		if b.Meta().MinTime < prev.Meta().MaxTime {
			return errors.Errorf("block time ranges overlap (%d, %d)", b.Meta().MinTime, prev.Meta().MaxTime)
		}
	}
	return nil
}

// Close the partition.
func (db *DB) Close() error {
	close(db.stopc)
	<-db.donec

	db.mtx.Lock()
	defer db.mtx.Unlock()

	var g errgroup.Group

	// blocks also contains all head blocks.
	for _, pb := range db.blocks {
		g.Go(pb.Close)
	}

	var merr MultiError

	merr.Add(g.Wait())
	if db.lockf != nil {
		merr.Add(db.lockf.Unlock())
	}

	return merr.Err()
}

// DisableCompactions disables compactions.
func (db *DB) DisableCompactions() {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	db.compactionsEnabled = false
	db.logger.Log("msg", "compactions disabled")
}

// EnableCompactions enables compactions.
func (db *DB) EnableCompactions() {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	db.compactionsEnabled = true
	db.logger.Log("msg", "compactions enabled")
}

// Snapshot writes the current data to the directory.
func (db *DB) Snapshot(dir string) error {
	// if dir == db.dir {
	// 	return errors.Errorf("cannot snapshot into base directory")
	// }
	// db.cmtx.Lock()
	// defer db.cmtx.Unlock()

	// db.mtx.Lock() // To block any appenders.
	// defer db.mtx.Unlock()

	// blocks := db.blocks[:]
	// for _, b := range blocks {
	// 	db.logger.Log("msg", "snapshotting block", "block", b)
	// 	if err := b.Snapshot(dir); err != nil {
	// 		return errors.Wrap(err, "error snapshotting headblock")
	// 	}
	// }

	return nil
}

// Querier returns a new querier over the data partition for the given time range.
// A goroutine must not handle more than one open Querier.
func (s *DB) Querier(mint, maxt int64) Querier {
	s.mtx.RLock()

	blocks := s.blocksForInterval(mint, maxt)

	sq := &querier{
		blocks: make([]Querier, 0, len(blocks)),
		db:     s,
	}
	for _, b := range blocks {
		sq.blocks = append(sq.blocks, &blockQuerier{
			mint:       mint,
			maxt:       maxt,
			index:      b.Index(),
			chunks:     b.Chunks(),
			tombstones: b.Tombstones(),
		})
	}

	return sq
}

// Appender returns a new Appender on the database.
func (db *DB) Appender() Appender {
	db.metrics.activeAppenders.Inc()

	db.mtx.RLock()
	db.head.mtx.RLock()

	return &dbAppender{
		db:      db,
		head:    db.head,
		wal:     db.wal,
		mint:    db.head.HighTimestamp() - db.opts.BlockRanges[0]/2,
		samples: db.getAppendBuffer(),
	}
}

func (db *DB) getAppendBuffer() []RefSample {
	b := db.appendPool.Get()
	if b == nil {
		return make([]RefSample, 0, 512)
	}
	return b.([]RefSample)
}

func (db *DB) putAppendBuffer(b []RefSample) {
	db.appendPool.Put(b[:0])
}

type dbAppender struct {
	db   *DB
	head *Head
	wal  WAL
	mint int64

	newSeries []*hashedLabels
	newLabels []labels.Labels
	newHashes map[uint64]uint64

	samples       []RefSample
	highTimestamp int64
}

type hashedLabels struct {
	ref    uint64
	hash   uint64
	labels labels.Labels
}

func (a *dbAppender) Add(lset labels.Labels, t int64, v float64) (string, error) {
	if t < a.mint {
		return "", ErrOutOfBounds
	}
	hash := lset.Hash()
	refb := make([]byte, 8)

	// Series exists already in the block.
	if ms := a.head.get(hash, lset); ms != nil {
		binary.BigEndian.PutUint64(refb, uint64(ms.ref))
		return string(refb), a.AddFast(string(refb), t, v)
	}
	// Series was added in this transaction previously.
	if ref, ok := a.newHashes[hash]; ok {
		binary.BigEndian.PutUint64(refb, ref)
		// XXX(fabxc): there's no fast path for multiple samples for the same new series
		// in the same transaction. We always return the invalid empty ref. It's has not
		// been a relevant use case so far and is not worth the trouble.
		return "", a.AddFast(string(refb), t, v)
	}

	// The series is completely new.
	if a.newSeries == nil {
		a.newHashes = map[uint64]uint64{}
	}
	// First sample for new series.
	ref := uint64(len(a.newSeries))

	a.newSeries = append(a.newSeries, &hashedLabels{
		ref:    ref,
		hash:   hash,
		labels: lset,
	})
	// First bit indicates its a series created in this transaction.
	ref |= (1 << 63)

	a.newHashes[hash] = ref
	binary.BigEndian.PutUint64(refb, ref)

	return "", a.AddFast(string(refb), t, v)
}

func (a *dbAppender) AddFast(ref string, t int64, v float64) error {
	if len(ref) != 8 {
		return errors.Wrap(ErrNotFound, "invalid ref length")
	}
	if t < a.mint {
		return ErrOutOfBounds
	}
	var (
		refn = binary.BigEndian.Uint64(yoloBytes(ref))
		id   = (refn << 1) >> 1
		inTx = refn&(1<<63) != 0
	)
	// Distinguish between existing series and series created in
	// this transaction.
	if inTx {
		if id > uint64(len(a.newSeries)-1) {
			return errors.Wrap(ErrNotFound, "transaction series ID too high")
		}
		// TODO(fabxc): we also have to validate here that the
		// sample sequence is valid.
		// We also have to revalidate it as we switch locks and create
		// the new series.
	} else if id > uint64(len(a.head.series)) {
		return errors.Wrap(ErrNotFound, "transaction series ID too high")
	} else {
		ms := a.head.series[id]
		if ms == nil {
			return errors.Wrap(ErrNotFound, "nil series")
		}
		// TODO(fabxc): memory series should be locked here already.
		// Only problem is release of locks in case of a rollback.
		c := ms.head()

		if t < c.maxTime {
			return ErrOutOfOrderSample
		}

		// We are allowing exact duplicates as we can encounter them in valid cases
		// like federation and erroring out at that time would be extremely noisy.
		if c.maxTime == t && math.Float64bits(ms.lastValue) != math.Float64bits(v) {
			return ErrAmendSample
		}
	}

	if t > a.highTimestamp {
		a.highTimestamp = t
	}

	a.samples = append(a.samples, RefSample{
		Ref: refn,
		T:   t,
		V:   v,
	})
	return nil
}

func (a *dbAppender) createSeries() error {
	if len(a.newSeries) == 0 {
		return nil
	}
	a.newLabels = make([]labels.Labels, 0, len(a.newSeries))
	base0 := len(a.head.series)

	a.head.mtx.RUnlock()
	defer a.head.mtx.RLock()
	a.head.mtx.Lock()
	defer a.head.mtx.Unlock()

	base1 := len(a.head.series)

	for _, l := range a.newSeries {
		// We switched locks and have to re-validate that the series were not
		// created by another goroutine in the meantime.
		if base1 > base0 {
			if ms := a.head.get(l.hash, l.labels); ms != nil {
				l.ref = uint64(ms.ref)
				continue
			}
		}
		// Series is still new.
		a.newLabels = append(a.newLabels, l.labels)
		l.ref = uint64(len(a.head.series))

		a.head.create(l.hash, l.labels)
	}

	// Write all new series to the WAL.
	if err := a.wal.LogSeries(a.newLabels); err != nil {
		return errors.Wrap(err, "WAL log series")
	}

	return nil
}

func (a *dbAppender) Commit() error {
	defer a.Rollback()

	if err := a.createSeries(); err != nil {
		return err
	}

	// We have to update the refs of samples for series we just created.
	for i := range a.samples {
		s := &a.samples[i]
		if s.Ref&(1<<63) != 0 {
			s.Ref = a.newSeries[(s.Ref<<1)>>1].ref
		}
	}

	// Write all new samples to the WAL and add them to the
	// in-mem database on success.
	if err := a.wal.LogSamples(a.samples); err != nil {
		return errors.Wrap(err, "WAL log samples")
	}

	total := uint64(len(a.samples))

	for _, s := range a.samples {
		if !a.head.series[s.Ref].append(s.T, s.V) {
			total--
		}
	}

	for {
		ht := a.head.HighTimestamp()
		if a.highTimestamp <= ht {
			break
		}
		if atomic.CompareAndSwapInt64(&a.head.highTimestamp, ht, a.highTimestamp) {
			break
		}
	}

	return nil
}

func (a *dbAppender) Rollback() error {
	a.head.mtx.RUnlock()
	a.db.mtx.RUnlock()

	a.db.metrics.activeAppenders.Dec()
	a.db.putAppendBuffer(a.samples)

	return nil
}

func rangeForTimestamp(t int64, width int64) (mint, maxt int64) {
	mint = (t / width) * width
	return mint, mint + width
}

// Delete implements deletion of metrics.
func (db *DB) Delete(mint, maxt int64, ms ...labels.Matcher) error {
	// db.cmtx.Lock()
	// defer db.cmtx.Unlock()

	// db.mtx.Lock()
	// defer db.mtx.Unlock()

	// blocks := db.blocksForInterval(mint, maxt)

	// var g errgroup.Group

	// for _, b := range blocks {
	// 	g.Go(func(b Block) func() error {
	// 		return func() error { return b.Delete(mint, maxt, ms...) }
	// 	}(b))
	// }

	// if err := g.Wait(); err != nil {
	// 	return err
	// }

	return nil
}

func intervalOverlap(amin, amax, bmin, bmax int64) bool {
	// Checks Overlap: http://stackoverflow.com/questions/3269434/
	return amin <= bmax && bmin <= amax
}

func intervalContains(min, max, t int64) bool {
	return t >= min && t <= max
}

// blocksForInterval returns all blocks within the partition that may contain
// data for the given time range.
func (db *DB) blocksForInterval(mint, maxt int64) []BlockReader {
	var bs []BlockReader

	for _, b := range db.blocks {
		m := b.Meta()
		if intervalOverlap(mint, maxt, m.MinTime, m.MaxTime) {
			bs = append(bs, b)
		}
	}
	if maxt >= db.head.lowTimestamp {
		bs = append(bs, db.head)
	}

	return bs
}

func isBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.Parse(fi.Name())
	return err == nil
}

func blockDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var dirs []string

	for _, fi := range files {
		if isBlockDir(fi) {
			dirs = append(dirs, filepath.Join(dir, fi.Name()))
		}
	}
	return dirs, nil
}

func sequenceFiles(dir, prefix string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string

	for _, fi := range files {
		if isSequenceFile(fi, prefix) {
			res = append(res, filepath.Join(dir, fi.Name()))
		}
	}
	return res, nil
}

func isSequenceFile(fi os.FileInfo, prefix string) bool {
	if !strings.HasPrefix(fi.Name(), prefix) {
		return false
	}
	if _, err := strconv.ParseUint(fi.Name()[len(prefix):], 10, 32); err != nil {
		return false
	}
	return true
}

func nextSequenceFile(dir, prefix string) (string, int, error) {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return "", 0, err
	}

	i := uint64(0)
	for _, n := range names {
		if !strings.HasPrefix(n, prefix) {
			continue
		}
		j, err := strconv.ParseUint(n[len(prefix):], 10, 32)
		if err != nil {
			continue
		}
		i = j
	}
	return filepath.Join(dir, fmt.Sprintf("%s%0.6d", prefix, i+1)), int(i + 1), nil
}

// The MultiError type implements the error interface, and contains the
// Errors used to construct it.
type MultiError []error

// Returns a concatenated string of the contained errors
func (es MultiError) Error() string {
	var buf bytes.Buffer

	if len(es) > 1 {
		fmt.Fprintf(&buf, "%d errors: ", len(es))
	}

	for i, err := range es {
		if i != 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(err.Error())
	}

	return buf.String()
}

// Add adds the error to the error list if it is not nil.
func (es *MultiError) Add(err error) {
	if err == nil {
		return
	}
	if merr, ok := err.(MultiError); ok {
		*es = append(*es, merr...)
	} else {
		*es = append(*es, err)
	}
}

// Err returns the error list as an error or nil if it is empty.
func (es MultiError) Err() error {
	if len(es) == 0 {
		return nil
	}
	return es
}

func yoloString(b []byte) string { return *((*string)(unsafe.Pointer(&b))) }
func yoloBytes(s string) []byte  { return *((*[]byte)(unsafe.Pointer(&s))) }

func closeAll(cs ...io.Closer) error {
	var merr MultiError

	for _, c := range cs {
		merr.Add(c.Close())
	}
	return merr.Err()
}
