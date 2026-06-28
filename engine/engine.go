package engine

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// Config configures an [Engine].
type Config struct {
	// OOOWindow rejects samples older than newest-OOOWindow (nanoseconds). 0 disables.
	OOOWindow int64
	// WAL, when non-nil, durably logs series and samples for crash recovery. nil is the
	// ephemeral in-memory engine.
	WAL *wal.SegmentWriter
	// Backend stores flushed parts. Required for [Engine.Flush]; nil is a head-only engine.
	Backend backend.Backend
	// Prefix is the backend key prefix under which this engine's parts are written
	// (typically "{tenant}/metrics").
	Prefix string
	// Obs is the observability handle (spans + metrics). nil ⇒ a no-op handle, so an engine
	// constructed without one logs/spans/counts nothing.
	Obs *obs.Obs
	// DecodeCacheBytes enables a cross-fetch cache of decoded part columns, sized to this many
	// bytes (LRU). It skips the column re-decode that the backend read cache cannot, and applies to
	// every backend (a decode is CPU even when the read is RAM-fast). Zero disables it.
	DecodeCacheBytes int64
	// MaxPartBytes caps an immutable part's (approximate, uncompressed) size: flush and merge split
	// their output into multiple parts so no single part exceeds it. 0 ⇒ unlimited (one part).
	MaxPartBytes int64
	// AggregateStats writes a per-series aggregate sidecar (count/sum/min/max) alongside each part,
	// so [Engine.AggregateRange] answers a range-covering aggregate from it without decoding the
	// value column. It costs a little storage per series; off by default. AggregateRange works
	// without it (via decoding), just without the fast path.
	AggregateStats bool
}

// Engine is a single tenant's storage engine. Safe for concurrent use.
type Engine struct {
	cfg     Config
	mu      sync.RWMutex
	head    *head
	parts   []*part
	nextSeq int
	// retiring holds parts removed from the live set by flush/merge, pending backend deletion once
	// their in-flight fetch readers drain (deferred reclamation; see reclaim.go).
	retiring []*part
	// flushing holds the sample buffers detached from the head by an in-progress flush, kept readable
	// by fetch until the flushed part is published (then cleared, atomically with adding the part) — so
	// a fetch never loses sight of records mid-flush. nil when no flush is in flight.
	flushing map[signal.SeriesID]*sampleBuf
	// walB groups a durable AppendBatch's WAL frames by series (reused under e.mu); nil head-only.
	walB *walBatch
	// decodeCache memoizes decoded part columns across fetches (LRU); nil ⇒ decode every fetch.
	decodeCache *decodeCache
	// decPool recycles decodedPart column buffers on the no-cross-fetch-cache path: a fetch borrows
	// them to decode a part and returns them on releaseParts (safe — the merge copies values out, so
	// no result aliases them). This kills the per-query decode-buffer allocation (chunk.resize).
	decPool sync.Pool
	// i64Pool / f64Pool recycle a fetched batch's result timestamp / value buffers, holding the
	// buffers as *[]int64 / *[]float64. i64Box / f64Box hold the spare *[]T *boxes* themselves so a
	// Put reuses a box instead of `Put(&local)` (which heap-escapes a fresh box on every recycle):
	// a box circulates Pool→(Get)→Box→(Put)→Pool, so steady-state recycling allocates no boxes.
	// They are fed only when a caller calls [fetch.Batch.Release]; a caller that never releases leaves
	// them empty, so collect makes fresh slices exactly as before — the default path takes nothing.
	i64Pool sync.Pool
	f64Pool sync.Pool
	i64Box  sync.Pool
	f64Box  sync.Pool
	// recycle is the shared per-engine [fetch.Batch.Release] hook (allocated once), so setting it on
	// a batch costs nothing per batch. It returns the batch's ts/value buffers to the pools above.
	recycle func(*fetch.Batch)
}

var _ fetch.Fetcher = (*Engine)(nil)

// New returns an engine with an empty head.
func New(cfg Config) *Engine {
	if cfg.Obs == nil {
		cfg.Obs = obs.NewNop()
	}

	e := &Engine{cfg: cfg, head: newHead()}
	e.decPool.New = func() any { return &decodedPart{} }
	// One shared release hook for every batch this engine produces (no per-batch closure alloc).
	e.recycle = func(b *fetch.Batch) {
		e.putI64(b.Timestamps)
		e.putF64(b.Values)
	}

	if cfg.WAL != nil {
		e.walB = newWALBatch()
	}

	if cfg.DecodeCacheBytes > 0 {
		e.decodeCache = newDecodeCache(cfg.DecodeCacheBytes)
	}

	return e
}

// metricSignal is the signal label for this engine's observability (it is the metrics engine).
const metricSignal = "metric"

// Append ingests one sample for series s, logging to the WAL when durable. It returns
// whether the sample was accepted (false ⇒ rejected as out-of-order beyond the window).
func (e *Engine) Append(s signal.Series, ts int64, value float64) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	id, accepted, isNew := e.head.append(s, ts, value, e.cfg.OOOWindow)
	if !accepted {
		return false, nil
	}

	if e.cfg.WAL != nil {
		if isNew {
			if err := e.cfg.WAL.WriteSeries(id, s); err != nil {
				return true, err
			}
		}

		if err := e.cfg.WAL.WriteSamples(id, []int64{ts}, []float64{value}); err != nil {
			return true, err
		}
	}

	return true, nil
}

// AppendBatch ingests a run of samples whose content ids are already computed (by the
// projection layer, on a reused buffer). ids[i], ts[i], values[i] describe sample i;
// materialize(i) returns sample i's full identity and is called only when its series is new
// (first sight), so a repeat series costs just a map probe and a buffer append, with no
// per-point [signal.Series] construction or hashing. The whole run is appended under a single
// lock. limits caps cardinality and in-flight memory (0 fields ⇒ unlimited). It returns an
// [AppendResult] breaking accepted/rejected down by reason, so the caller can report an exact
// OTLP partial-success. sf carries each sample's lossy-sampling weight (nil ⇒ every weight is 1);
// it is non-nil only when the caller's admission layer sampled the batch. Safe for concurrent use.
func (e *Engine) AppendBatch(
	ids []signal.SeriesID, ts []int64, values, sf []float64, materialize func(i int) signal.Series, limits AppendLimits,
) (AppendResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// One closure for the whole run (not one per point): bi selects the current sample for
	// the lazy materializer, which fires only on a newly-seen series.
	var bi int

	mat := func() signal.Series { return materialize(bi) }

	var res AppendResult

	for i := range ids {
		bi = i

		w := float64(1)
		if sf != nil {
			w = sf[i]
		}

		out, effID, isNew, s := e.head.appendByID(ids[i], ts[i], values[i], w, e.cfg.OOOWindow, limits, mat)

		switch out {
		case admitted:
			res.Accepted++
		case admittedOverflow:
			res.Accepted++
			res.Overflowed++
		case rejectOOO:
			res.RejectedOOO++

			continue
		case rejectCardinality:
			res.RejectedCardinality++

			continue
		case rejectBytes:
			res.RejectedBytes++

			continue
		}

		// Group the accepted samples by series; the grouped frames are written once after the loop
		// (one WriteSamples per series, not one write+fsync syscall per sample, all under the lock).
		// effID is the original id, or the overflow series' id when the sample was redirected — so
		// the WAL logs the identity the head actually holds, and replay reconstructs it.
		if e.cfg.WAL != nil {
			e.walB.add(effID, ts[i], values[i], w, isNew, s)
		}
	}

	if e.cfg.WAL != nil && !e.walB.empty() {
		if err := e.walB.flush(e.cfg.WAL); err != nil {
			return res, err
		}
	}

	return res, nil
}

// HeadBytes returns the head's current buffered sample bytes — the in-flight memory measure a
// consumer compares against a per-tenant cap (see [AppendLimits.MaxInFlightBytes]) and the basis
// for a size-triggered flush.
func (e *Engine) HeadBytes() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.head.bytes
}

// Stats is an in-memory snapshot of an engine's state for introspection (no backend I/O, no decode).
type Stats struct {
	Series      int64 // distinct series ever seen (index span: head ∪ flushed)
	HeadSamples int64 // samples currently buffered in the head (unflushed)
	HeadBytes   int64 // head's buffered sample bytes (the in-flight memory measure)
	Parts       int   // flushed immutable parts
	MinTime     int64 // oldest flushed sample time (unix ns); 0 when no parts
	MaxTime     int64 // newest sample time across parts and the head (unix ns); 0 when empty
}

// Stats returns an in-memory snapshot of the engine's state under a single read lock. It does no
// backend I/O and decodes nothing, so it is safe to poll at dashboard cadence without touching the
// hot path. Part byte sizes are not included (they would require backend stat calls).
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s := Stats{
		Series:    int64(e.head.series.Len()),
		HeadBytes: e.head.bytes,
		Parts:     len(e.parts),
		MaxTime:   e.head.newest,
	}

	for _, buf := range e.head.samples {
		s.HeadSamples += int64(len(buf.ts))
	}

	for i, p := range e.parts {
		if i == 0 || p.minTime < s.MinTime {
			s.MinTime = p.minTime
		}

		if p.maxTime > s.MaxTime {
			s.MaxTime = p.maxTime
		}
	}

	return s
}

// Fetch implements [fetch.Fetcher] over the head ∪ flushed parts: it resolves the
// request's matchers to series (the index spans every series ever seen, flushed or not)
// and returns one batch per series with its samples in the window, merged across the head
// buffer and every part by timestamp.
func (e *Engine) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	ctx = e.cfg.Obs.Base(ctx)
	ctx, span := e.cfg.Obs.Tracer.Start(ctx, "engine.fetch",
		trace.WithAttributes(attribute.String("storage.prefix", e.cfg.Prefix)))
	defer span.End()

	ctx, pf := profile.Begin(ctx, "engine.fetch")
	defer pf.End()

	startNs := time.Now()
	log := zctx.From(ctx)
	log.Debug("fetch start",
		zap.String("prefix", e.cfg.Prefix), zap.Int("matchers", len(r.Matchers)),
		zap.Int64("start", r.Start), zap.Int64("end", r.End))

	// The label index sorts lazily on first read after a write, mutating in place. Reads run
	// under the shared lock (concurrent fetches are allowed — e.g. split-by-interval), so do
	// that one-time sort under the exclusive lock first: hold the read lock, and while the
	// index is still unsorted, upgrade to sort and re-check. Once we hold the read lock with a
	// sorted index, no writer can be running, so resolve only reads.
	e.mu.RLock()
	for !e.head.indexSorted() {
		e.mu.RUnlock()
		e.mu.Lock()
		e.head.ensureIndexSorted()
		e.mu.Unlock()
		e.mu.RLock()
	}

	_, rpf := profile.Begin(ctx, "resolve-matchers")
	ids := e.head.resolve(r.Matchers)
	rpf.Add("series_matched", int64(len(ids)))
	rpf.End()

	partsScanned := len(e.parts)

	// Plan under the read lock: acquire the in-window parts to read and snapshot each series' head
	// (and any mid-flush) samples + identity, so the part reads below run lock-free — appends and
	// flush/merge proceed concurrently, and the acquired parts can't be reclaimed until released.
	plan := e.planFetch(ids, r)
	e.mu.RUnlock()

	defer plan.releaseParts()

	// Prefetch: decode the parts this fetch will touch concurrently (and cache them), so their
	// backend reads + decodes overlap instead of happening one part at a time during the merge.
	e.prefetch(ctx, plan)

	_, spf := profile.Begin(ctx, "scan")

	var (
		batches []*fetch.Batch
		rows    int
	)

	for _, id := range ids {
		m, err := plan.mergeSeries(ctx, id)
		if err != nil {
			span.RecordError(err)
			spf.End()

			return nil, err
		}

		// Buffer pooling is opt-in (Request.Recycle): only then do we touch the pool or set the
		// release hook, so the default path is exactly as before — no sync.Pool.Get overhead.
		var tsBuf []int64

		var valBuf []float64

		if r.Recycle {
			tsBuf, valBuf = e.getI64(), e.getF64()
		}

		ts, values, sf := m.collect(tsBuf, valBuf)
		if len(ts) == 0 {
			if r.Recycle {
				e.putI64(ts)
				e.putF64(values)
			}

			continue
		}

		b := &fetch.Batch{ID: id, Series: plan.series[id], Timestamps: ts, Values: values, ScaleFactors: sf}
		if r.Recycle {
			b.SetRelease(e.recycle) // caller will Release to recycle the ts/value buffers
		}

		batches = append(batches, b)
		rows += len(ts)
	}

	spf.Add("parts_scanned", int64(partsScanned))
	spf.Add("rows", int64(rows))
	spf.End()

	pf.Add("series_matched", int64(len(ids)))
	pf.Add("parts_scanned", int64(partsScanned))
	pf.Add("rows", int64(rows))

	span.SetAttributes(
		attribute.Int("storage.series_matched", len(ids)),
		attribute.Int("storage.parts_scanned", partsScanned),
		attribute.Int("storage.rows", rows),
	)
	e.cfg.Obs.Fetch.Record(ctx, metricSignal, time.Since(startNs), int64(len(ids)), int64(partsScanned), int64(rows))
	log.Debug("fetch done",
		zap.String("prefix", e.cfg.Prefix), zap.Int("series_matched", len(ids)),
		zap.Int("parts_scanned", partsScanned), zap.Int("rows", rows),
		zap.Duration("took", time.Since(startNs)))

	return fetch.NewSliceIterator(batches), nil
}

// enginePlan is the lock-free-readable plan a fetch builds under the engine read lock: the acquired
// (ref-held) in-window parts still to read, plus each series' identity and its already-snapshotted head
// and mid-flush samples. Its [enginePlan.mergeSeries] does the part reads off the lock.
type enginePlan struct {
	ids        []signal.SeriesID
	series     map[signal.SeriesID]signal.Series
	headB      map[signal.SeriesID]*fetch.Batch // head-window samples, copied under the lock
	flushB     map[signal.SeriesID]*fetch.Batch // mid-flush detached samples (not yet a part)
	liveParts  []*part
	decoded    partDecodeCache // per-fetch decode memo so each part decodes once, not once per series
	decodeFn   decodeFunc      // how a part is decoded on a per-fetch miss (cache-aware)
	engine     *Engine         // for returning pooled decode buffers on release
	start, end int64
}

// mergeSeries gathers series id's samples lock-free: each acquired part oldest→newest, then the
// mid-flush samples, then the head samples last — so on a duplicate timestamp the freshest value wins.
func (p *enginePlan) mergeSeries(ctx context.Context, id signal.SeriesID) (sampleMerge, error) {
	var m sampleMerge

	for _, part := range p.liveParts {
		rng, ok := part.ranges[id]
		if !ok {
			continue
		}

		d, err := p.decoded.get(ctx, part, p.decodeFn)
		if err != nil {
			return m, err
		}

		d.mergeSeriesInto(rng, &m, p.start, p.end)
	}

	if fb := p.flushB[id]; fb != nil {
		m.add(fb.Timestamps, fb.Values, fb.ScaleFactors, p.start, p.end)
	}

	if hb := p.headB[id]; hb != nil {
		m.add(hb.Timestamps, hb.Values, hb.ScaleFactors, p.start, p.end)
	}

	return m, nil
}

// releaseParts releases the fetch's hold on its acquired parts, letting a retired part be reclaimed.
func (p *enginePlan) releaseParts() {
	for _, part := range p.liveParts {
		part.release()
	}

	// Return pooled decode buffers (no-cross-fetch-cache path). The merge has already copied the
	// values out into the result batches, so these slices are dead and safe to recycle.
	for _, dp := range p.decoded {
		if dp.pooled {
			dp.pooled = false
			dp.ts, dp.vals, dp.sf = dp.ts[:0], dp.vals[:0], dp.sf[:0]
			p.engine.decPool.Put(dp)
		}
	}
}

// tsRun is one source's in-window, ascending samples feeding a merge. sf is nil when every weight
// is 1. The slices alias their source (a pooled decoded part, or a head/flush copy), so collect
// copies into fresh result buffers — it never returns a run's backing array.
type tsRun struct {
	ts   []int64
	vals []float64
	sf   []float64
}

func (r tsRun) weight(i int) float64 {
	if r.sf == nil {
		return 1
	}

	return r.sf[i]
}

// sampleMerge merges one series' samples from several already-sorted sources, deduplicating by
// timestamp with **freshest-wins**: sources are added oldest → newest, and on a timestamp tie the
// latest-added source's value (and weight) is kept. It holds the sources as zero-copy run views and
// merges them once in collect — no per-series map (which dominated the read-path allocations).
type sampleMerge struct {
	runs []tsRun // oldest → newest; a higher index wins a timestamp tie
}

// add registers a source's [start, end] window as a run. ts must be ascending; the window bounds are
// found by binary search (a no-op clip for an already-windowed head/flush source). Empty windows are
// skipped. sf carries each sample's weight (nil ⇒ every weight is 1).
func (m *sampleMerge) add(ts []int64, values, sf []float64, start, end int64) {
	lo := lowerBound(ts, start) // first i with ts[i] >= start
	hi := upperBound(ts, end)   // first i with ts[i] > end
	if lo >= hi {
		return
	}

	var sfw []float64
	if sf != nil {
		sfw = sf[lo:hi]
	}

	m.runs = append(m.runs, tsRun{ts: ts[lo:hi], vals: values[lo:hi], sf: sfw})
}

// collect merges the runs into the result columns sorted ascending by timestamp. tsBuf/valsBuf are
// reusable destination buffers (from the engine's pool, or nil to allocate fresh); collect grows
// them to the needed size. The returned sf slice is nil when every weight is 1 (the unsampled common
// case), else len == len(ts).
func (m *sampleMerge) collect(tsBuf []int64, valsBuf []float64) (tsOut []int64, values, sf []float64) {
	switch len(m.runs) {
	case 0:
		return tsBuf[:0], valsBuf[:0], nil
	case 1:
		return collectOne(m.runs[0], tsBuf, valsBuf)
	default:
		return collectMany(m.runs, tsBuf, valsBuf)
	}
}

// ensureCap returns s truncated to length 0 if it already has capacity n, else a fresh slice of
// capacity n. Lets a decode/merge reuse a pooled buffer while keeping exact pre-sizing.
func ensureCap[T any](s []T, n int) []T {
	if cap(s) >= n {
		return s[:0]
	}

	return make([]T, 0, n)
}

// collectOne copies a single source's run into the destination buffers, dropping any adjacent
// duplicate timestamps (keeping the last — matching the map's last-write-wins). No merge needed.
func collectOne(r tsRun, tsBuf []int64, valsBuf []float64) (tsOut []int64, values, sf []float64) {
	n := len(r.ts)
	tsOut = ensureCap(tsBuf, n)
	values = ensureCap(valsBuf, n)

	for i := range n {
		if i+1 < n && r.ts[i+1] == r.ts[i] { // keep the last of an equal-ts run
			continue
		}

		tsOut = append(tsOut, r.ts[i])
		values = append(values, r.vals[i])
		sf = appendWeight(sf, r.weight(i), len(values), n)
	}

	return tsOut, values, sf
}

// collectMany k-way-merges several sorted runs into the destination buffers: at each step it emits
// the smallest timestamp once, taking the value/weight from the highest-indexed (freshest) run that
// holds it and advancing every run positioned there. O(rows × runs); runs is tiny (parts + flush +
// head), so the linear min-scan beats a heap's overhead and allocations.
func collectMany(runs []tsRun, tsBuf []int64, valsBuf []float64) (tsOut []int64, values, sf []float64) {
	total := 0
	for i := range runs {
		total += len(runs[i].ts)
	}

	tsOut = ensureCap(tsBuf, total)
	values = ensureCap(valsBuf, total)

	// Per-run cursors. Stack-allocated for the common small fan-in; heap only for a huge one.
	var curArr [16]int

	var cur []int
	if len(runs) <= len(curArr) {
		cur = curArr[:len(runs)]
	} else {
		cur = make([]int, len(runs))
	}

	for {
		minTs := int64(0)
		found := false

		for i := range runs {
			if cur[i] < len(runs[i].ts) {
				if t := runs[i].ts[cur[i]]; !found || t < minTs {
					minTs, found = t, true
				}
			}
		}

		if !found {
			break
		}

		var winVal, winW float64 = 0, 1

		for i := range runs {
			if cur[i] < len(runs[i].ts) && runs[i].ts[cur[i]] == minTs {
				winVal, winW = runs[i].vals[cur[i]], runs[i].weight(cur[i])
				cur[i]++
			}
		}

		tsOut = append(tsOut, minTs)
		values = append(values, winVal)
		sf = appendWeight(sf, winW, len(values), total)
	}

	return tsOut, values, sf
}

// appendWeight appends w to the lazily-materialized sf column: it stays nil until the first non-unit
// weight (backfilling 1 for the n-1 rows already emitted), keeping the unsampled path allocation-free.
// n is the result length after this append; capHint sizes the slice on first materialization.
func appendWeight(sf []float64, w float64, n, capHint int) []float64 {
	if sf == nil {
		if w == 1 {
			return nil
		}

		sf = make([]float64, n-1, capHint)
		for j := range sf {
			sf[j] = 1
		}
	}

	return append(sf, w)
}

// lowerBound returns the first index i in the ascending slice s with s[i] >= x (len(s) if none).
func lowerBound(s []int64, x int64) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s[mid] < x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// upperBound returns the first index i in the ascending slice s with s[i] > x (len(s) if none).
func upperBound(s []int64, x int64) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s[mid] <= x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// Flush writes the head's buffered samples to a new immutable part and clears the buffers
// (the series index is retained). It is a no-op if the head holds no samples. Requires a
// [Config.Backend].
func (e *Engine) Flush(ctx context.Context) error {
	ctx = e.cfg.Obs.Base(ctx)
	ctx, span := e.cfg.Obs.Tracer.Start(ctx, "engine.flush",
		trace.WithAttributes(attribute.String("storage.prefix", e.cfg.Prefix)))
	defer span.End()

	startNs := time.Now()
	log := zctx.From(ctx)
	log.Debug("flush requested", zap.String("prefix", e.cfg.Prefix))

	rows, err := e.flush(ctx)
	if err != nil {
		span.RecordError(err)
		log.Error("flush failed", zap.String("prefix", e.cfg.Prefix), zap.Error(err))

		return err
	}

	if rows > 0 {
		span.SetAttributes(attribute.Int("storage.rows", rows))
		e.cfg.Obs.Flush.Record(ctx, metricSignal, time.Since(startNs), int64(rows))
		log.Debug("flushed head to part",
			zap.String("prefix", e.cfg.Prefix), zap.Int("rows", rows),
			zap.Duration("took", time.Since(startNs)))
	} else {
		log.Debug("flush no-op (empty head)", zap.String("prefix", e.cfg.Prefix))
	}

	return nil
}

// Reset discards all of the engine's data — the in-memory head (samples + series index)
// and every flushed part — returning it to the empty state of a freshly [New]'d engine,
// without reallocating the engine itself. Flushed part objects are deleted from the backend
// so none are orphaned. It is destructive (it wipes this engine's parts under
// [Config.Prefix]) and is meant for the ephemeral in-memory engine in tests and benchmarks,
// letting a long-lived engine be reused across runs. Safe for concurrent use.
func (e *Engine) Reset(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.head = newHead()
	e.parts = nil
	e.retiring = nil // the List+Delete below removes their objects too
	e.nextSeq = 0

	if e.cfg.Backend == nil {
		return nil
	}

	// Delete every object this engine wrote: all part keys are "{Prefix}/{seq}/...", so the
	// "{Prefix}/" scope catches them without touching a sibling engine's keys.
	keys, err := e.cfg.Backend.List(ctx, e.cfg.Prefix+"/")
	if err != nil {
		return errors.Wrap(err, "list parts")
	}

	for _, k := range keys {
		if err := e.cfg.Backend.Delete(ctx, k); err != nil && !errors.Is(err, backend.ErrNotExist) {
			return errors.Wrapf(err, "delete %q", k)
		}
	}

	return nil
}

// PartCount returns the number of flushed parts (testing/introspection).
func (e *Engine) PartCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return len(e.parts)
}

// Replay rebuilds the head from the WAL segments in dir (durable restart).
func (e *Engine) Replay(dir string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.ReplayDir(dir, e.replayHandlers())
}

// ApplyPrimary applies a write as the shard's **primary**: it runs each sample through the
// admission-checked append path (the single OOO decision for the shard, plus the cardinality
// and in-flight-memory valves from limits) and re-frames the *accepted* samples into a WAL
// payload to replicate to the secondary owners. It returns that accepted payload and an
// [AppendResult] breaking the disposition down by reason, so the clustered ingest path can
// attribute OTLP partial-success exactly like the single-node path. Because only the primary
// admission-checks and it dictates the accepted set, every replica converges on the same data
// regardless of concurrent writers. Safe for concurrent use.
func (e *Engine) ApplyPrimary(data []byte, limits AppendLimits) (accepted []byte, res AppendResult, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var (
		buf     bytes.Buffer
		w       = wal.NewWriter(&buf)
		byID    = make(map[signal.SeriesID]signal.Series)
		written = make(map[signal.SeriesID]struct{})
	)

	err = wal.Replay(data, wal.Handlers{
		OnSeries: func(id signal.SeriesID, s signal.Series) error {
			byID[id] = s

			return nil
		},
		OnSamples: func(id signal.SeriesID, ts []int64, values []float64) error {
			s := byID[id] // the series record precedes its samples in the frame

			var accTs []int64

			var accVals []float64

			for i := range ts {
				// The primary is the shard's single authority, so it makes the admission decision
				// here (OOO window + cardinality + in-flight memory); secondaries apply the accepted
				// set verbatim via ApplyReplicated.
				// The cluster primary path does not set limits.Overflow, so a new series past the cap
				// is hard-rejected here (overflow routing is single-node metrics today); effID == id.
				out, _, _, _ := e.head.appendByID(id, ts[i], values[i], 1, e.cfg.OOOWindow,
					limits, func() signal.Series { return s })

				switch out {
				case admitted, admittedOverflow: // primary path sets no Overflow, so only `admitted` occurs
					accTs = append(accTs, ts[i])
					accVals = append(accVals, values[i])
					res.Accepted++
				case rejectOOO:
					res.RejectedOOO++
				case rejectCardinality:
					res.RejectedCardinality++
				case rejectBytes:
					res.RejectedBytes++
				}
			}

			if len(accTs) == 0 {
				return nil
			}

			if _, ok := written[id]; !ok {
				written[id] = struct{}{}
				if err := w.WriteSeries(id, s); err != nil {
					return err
				}
			}

			return w.WriteSamples(id, accTs, accVals)
		},
	})

	return buf.Bytes(), res, err
}

// ApplyReplicated applies a replicated write from the shard's primary to this secondary's head:
// it registers each series and appends its samples **verbatim** (no OOO re-check — the primary
// already decided the accepted set, the same way WAL [Engine.Replay] trusts the log), so all
// replicas hold identical data. A replica holds the unflushed head this way; after a flush the
// shared object store reconciles them. Safe for concurrent use.
func (e *Engine) ApplyReplicated(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.Replay(data, e.replayHandlers())
}

// HeadSampleCount returns the number of samples currently buffered in the head (across all
// series) — for introspection and tests (e.g. to observe replica head trimming).
func (e *Engine) HeadSampleCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	n := 0
	for _, buf := range e.head.samples {
		n += len(buf.ts)
	}

	return n
}

// SeriesCount returns the number of distinct series in the head.
func (e *Engine) SeriesCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.head.series.Len()
}

// flush drains the head to a new immutable part, returning the rows flushed (0 ⇒ empty head). Phased
// so the part write and read-back happen off the engine lock (appends and fetches proceed), while the
// head detach and the metadata publish run under it. Only the background maintenance task (or Close)
// calls flush, so the parts mutation has a single writer.
func (e *Engine) flush(ctx context.Context) (int, error) {
	// Plan (under lock): detach the head's sample buffers, keeping them readable via e.flushing so a
	// concurrent fetch never loses them, and reserve the part sequence.
	e.mu.Lock()
	detached := e.head.detach()
	if detached == nil {
		e.mu.Unlock()
		e.reclaimRetired(ctx) // nothing to flush, but still sweep pending deletions

		return 0, nil
	}

	e.flushing = detached
	seq := e.nextSeq
	e.mu.Unlock()

	// Build (lock-free): lay out the detached buffers and write the part. Flush writes freshly-ingested
	// (warm) data with the default codec-only framing; recompression of cold data happens at merge.
	cols := buildFlushColumns(detached)
	if cols == nil { // every detached buffer was empty (defensive — detach guarantees ≥1 row)
		e.mu.Lock()
		e.flushing = nil
		e.mu.Unlock()

		return 0, nil
	}

	rows := len(cols.ts)

	// Split the flushed columns into one or more parts, each kept under MaxPartBytes (a single part
	// when unlimited). Flush writes freshly-ingested data with codec-only framing (no recompression).
	ranges := chunkRanges(rows, maxRowsPerPart(e.cfg.MaxPartBytes))

	newParts := make([]*part, 0, len(ranges))
	for i, rg := range ranges {
		sub := cols.slice(rg[0], rg[1])
		prefix := e.partPrefix(seq + i)

		if err := writePart(ctx, e.cfg.Backend, prefix, sub, nil, 0, e.cfg.AggregateStats); err != nil {
			return 0, err
		}

		p, err := openPart(ctx, e.cfg.Backend, prefix)
		if err != nil {
			return 0, err
		}

		p.minTime, p.maxTime = colsTimeRange(sub)
		newParts = append(newParts, p)
	}

	// Publish (under lock): add the parts copy-on-write and clear e.flushing in the same critical
	// section, so a fetch sees the samples either in e.flushing or in a part — never neither (no gap)
	// and never both (no double count). The small index writes and WAL checkpoint stay under the lock
	// so the parts swap and the durable commit remain atomic.
	e.mu.Lock()
	for _, p := range newParts {
		e.parts = appendPart(e.parts, p)
	}
	e.flushing = nil
	e.nextSeq = seq + len(ranges)
	err := e.publishLocked(ctx)
	e.mu.Unlock()

	if err != nil {
		return rows, err
	}

	e.reclaimRetired(ctx)

	return rows, nil
}

// publishLocked persists the engine's part set (bucket index + series identity index) and checkpoints
// the WAL — the now-durable part makes its WAL records obsolete. Caller holds e.mu.
func (e *Engine) publishLocked(ctx context.Context) error {
	if err := e.updateIndexLocked(ctx); err != nil {
		return err
	}

	if err := e.writeSeriesIndexLocked(ctx); err != nil {
		return err
	}

	if e.cfg.WAL != nil {
		return e.cfg.WAL.Checkpoint()
	}

	return nil
}

// decodeOf decodes p through the cross-fetch decode cache when enabled: a hit returns the shared
// (immutable) decoded columns; a miss decodes and caches them. Without a cache it decodes plainly.
func (e *Engine) decodeOf(ctx context.Context, p *part) (*decodedPart, error) {
	if e.decodeCache == nil {
		// No cross-fetch cache: borrow buffers from the pool, decode into them, and mark the result
		// pooled so the fetch returns it on releaseParts. The merge copies values out, so the buffers
		// are free to reuse once the fetch ends.
		dp := e.decPool.Get().(*decodedPart)

		dp, err := p.decodeInto(ctx, dp)
		if err != nil {
			e.decPool.Put(dp)

			return nil, err
		}

		dp.pooled = true

		return dp, nil
	}

	if dp, ok := e.decodeCache.get(p.prefix); ok {
		return dp, nil
	}

	dp, err := p.decode(ctx)
	if err != nil {
		return nil, err
	}

	e.decodeCache.put(p.prefix, dp)

	return dp, nil
}

// prefetchConcurrency bounds the parallel part decodes a single fetch's prefetch issues.
const prefetchConcurrency = 8

// prefetch concurrently decodes (and caches) the parts this fetch will actually touch — those
// holding at least one matched series — so the per-part backend reads and decodes overlap instead
// of running sequentially as the merge first reaches each part. It is a no-op without a decode
// cache or with fewer than two parts to touch (the lazy path is already optimal). Best-effort: a
// decode error here is ignored; the merge re-decodes and surfaces it.
func (e *Engine) prefetch(ctx context.Context, plan *enginePlan) {
	if e.decodeCache == nil || len(plan.liveParts) < 2 {
		return
	}

	var todo []*part

	for _, pt := range plan.liveParts {
		for _, id := range plan.ids {
			if _, ok := pt.ranges[id]; ok {
				todo = append(todo, pt)

				break
			}
		}
	}

	if len(todo) < 2 {
		return
	}

	sem := make(chan struct{}, prefetchConcurrency)

	var wg sync.WaitGroup

	for _, pt := range todo {
		wg.Add(1)
		sem <- struct{}{}

		go func(p *part) {
			defer wg.Done()
			defer func() { <-sem }()

			_, _ = e.decodeOf(ctx, p)
		}(pt)
	}

	wg.Wait()
}

// planFetch selects and acquires the in-window parts and snapshots each series' head + mid-flush
// samples and identity — all under the lock — so the part reads run lock-free. Caller holds e.mu (read
// lock). The acquired parts must be released with releaseParts.
func (e *Engine) planFetch(ids []signal.SeriesID, r fetch.Request) *enginePlan {
	p := &enginePlan{
		ids:      ids,
		series:   make(map[signal.SeriesID]signal.Series, len(ids)),
		headB:    make(map[signal.SeriesID]*fetch.Batch, len(ids)),
		flushB:   make(map[signal.SeriesID]*fetch.Batch, len(ids)),
		decoded:  make(partDecodeCache),
		decodeFn: e.decodeOf,
		engine:   e,
		start:    r.Start,
		end:      r.End,
	}

	for _, part := range e.parts {
		if part.maxTime < r.Start || part.minTime > r.End { // time-prune
			continue
		}

		part.acquire()
		p.liveParts = append(p.liveParts, part)
	}

	for _, id := range ids {
		if s, ok := e.head.series.Get(id); ok {
			p.series[id] = s
		}

		if hb := e.head.batch(id, r.Start, r.End); hb != nil {
			p.headB[id] = hb
		}

		if buf := e.flushing[id]; buf != nil {
			if fb := bufBatch(buf, id, p.series[id], r.Start, r.End); fb != nil {
				p.flushB[id] = fb
			}
		}
	}

	return p
}

// replayHandlers returns the WAL handlers that rebuild the head from logged records — registering
// each series and appending its samples (plain or scale-factor-carrying) verbatim. Shared by the
// durable-restart [Engine.Replay] and the trusting [Engine.ApplyReplicated]. The caller holds e.mu.
func (e *Engine) replayHandlers() wal.Handlers {
	return wal.Handlers{
		OnSeries: func(_ signal.SeriesID, s signal.Series) error {
			e.head.registerSeries(s)

			return nil
		},
		OnSamples: func(id signal.SeriesID, ts []int64, values []float64) error {
			e.head.replaySamples(id, ts, values)

			return nil
		},
		OnSamplesSF: func(id signal.SeriesID, ts []int64, values, sf []float64) error {
			e.head.replaySamplesSF(id, ts, values, sf)

			return nil
		},
	}
}

// getI64 returns a reusable []int64 (len 0) from the pool, or nil when the pool is empty — so a
// caller that never releases makes fresh slices (no behavior change). The caller appends into it.
func (e *Engine) getI64() []int64 {
	v := e.i64Pool.Get()
	if v == nil {
		return nil
	}

	p := v.(*[]int64)
	s := *p
	*p = nil
	e.i64Box.Put(p) // recycle the emptied box for the next putI64

	return s[:0]
}

// getF64 is [Engine.getI64] for float64 value buffers.
func (e *Engine) getF64() []float64 {
	v := e.f64Pool.Get()
	if v == nil {
		return nil
	}

	p := v.(*[]float64)
	s := *p
	*p = nil
	e.f64Box.Put(p)

	return s[:0]
}

// putI64 returns a buffer to its pool (only meaningfully reused if it has capacity). It refills a
// recycled box from i64Box rather than `Put(&s)`, so no *[]int64 box escapes per recycle in steady
// state. This is the opt-in Release path, never the default.
func (e *Engine) putI64(s []int64) {
	if cap(s) == 0 {
		return
	}

	p, _ := e.i64Box.Get().(*[]int64)
	if p == nil {
		p = new([]int64)
	}

	*p = s
	e.i64Pool.Put(p)
}

// putF64 is [Engine.putI64] for float64 value buffers.
func (e *Engine) putF64(s []float64) {
	if cap(s) == 0 {
		return
	}

	p, _ := e.f64Box.Get().(*[]float64)
	if p == nil {
		p = new([]float64)
	}

	*p = s
	e.f64Pool.Put(p)
}
