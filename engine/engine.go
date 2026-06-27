package engine

import (
	"bytes"
	"context"
	"slices"
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
}

var _ fetch.Fetcher = (*Engine)(nil)

// New returns an engine with an empty head.
func New(cfg Config) *Engine {
	if cfg.Obs == nil {
		cfg.Obs = obs.NewNop()
	}

	e := &Engine{cfg: cfg, head: newHead()}
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

		out, isNew, s := e.head.appendByID(ids[i], ts[i], values[i], w, e.cfg.OOOWindow, limits, mat)

		switch out {
		case admitted:
			res.Accepted++
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
		if e.cfg.WAL != nil {
			e.walB.add(ids[i], ts[i], values[i], isNew, s)
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

		if ts, values, sf := m.collect(); len(ts) > 0 {
			batches = append(batches, &fetch.Batch{ID: id, Series: plan.series[id], Timestamps: ts, Values: values, ScaleFactors: sf})
			rows += len(ts)
		}
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
}

// sfval is a sample's value paired with its lossy-sampling weight.
type sfval struct {
	value float64
	sf    float64
}

// sampleMerge merges samples from multiple sources for one series, deduplicating by
// timestamp. Sources are added oldest → newest, so a later add overwrites an earlier value
// (and its weight) at the same timestamp.
type sampleMerge struct {
	byTs map[int64]sfval
}

// add merges the samples whose timestamps fall in [start, end]. sf carries each sample's weight
// (nil ⇒ every weight is 1).
func (m *sampleMerge) add(ts []int64, values, sf []float64, start, end int64) {
	if m.byTs == nil {
		m.byTs = make(map[int64]sfval, len(ts))
	}

	for i := range ts {
		if ts[i] < start || ts[i] > end {
			continue
		}

		w := float64(1)
		if sf != nil {
			w = sf[i]
		}

		m.byTs[ts[i]] = sfval{value: values[i], sf: w}
	}
}

// collect returns the merged samples sorted ascending by timestamp. The returned sf slice is nil
// when every weight is 1 (the unsampled common case), else len == len(ts).
func (m *sampleMerge) collect() (tsOut []int64, values, sf []float64) {
	if len(m.byTs) == 0 {
		return nil, nil, nil
	}

	tsOut = make([]int64, 0, len(m.byTs))
	for t := range m.byTs {
		tsOut = append(tsOut, t)
	}

	slices.Sort(tsOut)

	values = make([]float64, len(tsOut))

	for i, t := range tsOut {
		v := m.byTs[t]
		values[i] = v.value

		if v.sf != 1 && sf == nil {
			sf = make([]float64, i, len(tsOut))
			for j := range sf {
				sf[j] = 1
			}
		}

		if sf != nil {
			sf = append(sf, v.sf)
		}
	}

	return tsOut, values, sf
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

	return wal.ReplayDir(dir, wal.Handlers{
		OnSeries: func(_ signal.SeriesID, s signal.Series) error {
			e.head.registerSeries(s)

			return nil
		},
		OnSamples: func(id signal.SeriesID, ts []int64, values []float64) error {
			e.head.replaySamples(id, ts, values)

			return nil
		},
	})
}

// ApplyPrimary applies a write as the shard's **primary**: it appends each sample through the
// out-of-order-checked path (the single OOO decision for the shard) and re-frames the
// *accepted* samples into a WAL payload to replicate to the secondary owners. It returns that
// accepted payload and the number of samples rejected as out-of-order. Because only the
// primary OOO-checks and it dictates the accepted set, every replica converges on the same
// data regardless of concurrent writers. Safe for concurrent use.
func (e *Engine) ApplyPrimary(data []byte) (accepted []byte, rejected int, err error) {
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
				// The replication apply path enforces only the OOO window (the shard primary's
				// authoritative timing decision); cardinality/memory admission is applied at the
				// origin ingest, so pass no limits here.
				if out, _, _ := e.head.appendByID(id, ts[i], values[i], 1, e.cfg.OOOWindow,
					AppendLimits{}, func() signal.Series { return s }); out == admitted {
					accTs = append(accTs, ts[i])
					accVals = append(accVals, values[i])
				} else {
					rejected++
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

	return buf.Bytes(), rejected, err
}

// ApplyReplicated applies a replicated write from the shard's primary to this secondary's head:
// it registers each series and appends its samples **verbatim** (no OOO re-check — the primary
// already decided the accepted set, the same way WAL [Engine.Replay] trusts the log), so all
// replicas hold identical data. A replica holds the unflushed head this way; after a flush the
// shared object store reconciles them. Safe for concurrent use.
func (e *Engine) ApplyReplicated(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.Replay(data, wal.Handlers{
		OnSeries: func(_ signal.SeriesID, s signal.Series) error {
			e.head.registerSeries(s)

			return nil
		},
		OnSamples: func(id signal.SeriesID, ts []int64, values []float64) error {
			e.head.replaySamples(id, ts, values)

			return nil
		},
	})
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
	prefix := e.partPrefix(seq)

	if err := writePart(ctx, e.cfg.Backend, prefix, cols, nil, 0); err != nil {
		return 0, err
	}

	p, err := openPart(ctx, e.cfg.Backend, prefix)
	if err != nil {
		return 0, err
	}

	p.minTime, p.maxTime = colsTimeRange(cols)

	// Publish (under lock): add the part copy-on-write and clear e.flushing in the same critical
	// section, so a fetch sees the samples either in e.flushing or in the part — never neither (no gap)
	// and never both (no double count). The small index writes and WAL checkpoint stay under the lock
	// so the parts swap and the durable commit remain atomic.
	e.mu.Lock()
	e.parts = appendPart(e.parts, p)
	e.flushing = nil
	e.nextSeq = seq + 1
	err = e.publishLocked(ctx)
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
		return p.decode(ctx)
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
