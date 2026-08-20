package engine

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/diskguard"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/pool"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// Config configures an [Engine].
type Config struct {
	// OOOWindow is a per-series lateness bound: a sample older than OOOWindow (nanoseconds) behind
	// the series' own newest admitted sample is rejected. 0 disables.
	OOOWindow int64
	// WAL, when non-nil, durably logs series and samples for crash recovery. nil is the
	// ephemeral in-memory engine.
	WAL *wal.SegmentWriter
	// Backend stores flushed parts. Required for [Engine.Flush]; nil is a head-only engine.
	Backend backend.Backend
	// Prefix is the backend key prefix under which this engine's parts are written
	// (typically "{tenant}/metrics").
	Prefix string
	// Term reports this writer's current ownership term for Prefix — which tenure of the shard
	// this engine is writing as. It is stamped into every bucket index written, so a reader can
	// order two indexes of the same prefix even when neither the part names nor FlushedEpoch
	// moved; see [github.com/oteldb/storage/backend/bucketindex.Generation]. nil is a writer with
	// no cluster, whose generation is then a plain local counter.
	Term func() uint64
	// Obs is the observability handle (spans + metrics). nil ⇒ a no-op handle, so an engine
	// constructed without one logs/spans/counts nothing.
	Obs *obs.Obs
	// DecodeCacheBytes enables a cross-fetch cache of decoded part columns, sized to this many
	// bytes (LRU). It skips the column re-decode that the backend read cache cannot, and applies to
	// every backend (a decode is CPU even when the read is RAM-fast). Zero disables it.
	DecodeCacheBytes int64
	// MaxPartBytes caps a *flushed* part's (approximate, uncompressed) size: a flush splits its
	// output so no single part exceeds it. 0 ⇒ unlimited. A merged part is sized by
	// MergeCeilingBytes instead.
	MaxPartBytes int64
	// MergeCeilingBytes is the upper bound on a merged part's size *on disk*; the effective cap is
	// the least of it, this merge's share of the backend's free space, and — over a backend that
	// takes objects whole — what the merge may hold in memory (see mergecap.go). 0 ⇒
	// defaultMergeCeilingBytes; negative ⇒ unlimited (never seal).
	MergeCeilingBytes int64
	// MergeMemoryBytes is how much memory all concurrent merges together may hold. Over a backend
	// that takes objects whole a merged part is buffered encoded in RAM until it is sealed, so this —
	// not free space — is what stops a part from outgrowing the process on a node whose disk dwarfs
	// its memory limit. Over one implementing backend.ObjectCreator the part streams out as it is
	// encoded, so this bounds the per-series state a merge still holds and the disk sizes the part.
	// 0 ⇒ a share of GOMEMLIMIT, or defaultMergeMemoryBytes when the process declares no limit;
	// negative ⇒ unbounded (only the ceiling and free space then apply).
	MergeMemoryBytes int64
	// MergeConcurrency reports how many merges may run concurrently against this backend, dividing
	// the free space so they cannot collectively exhaust the disk. nil or ≤ 1 ⇒ no division.
	//
	// A callback because the answer moves: fan-out is bounded by the node's engine count as much as
	// by its worker limit, and engines appear lazily. Fixing it at engine creation would divide a
	// single-tenant node's disk by its core count, undoing most of the widening.
	MergeConcurrency func() int
	// AggregateStats writes a per-series aggregate sidecar (count/sum/min/max) alongside each part,
	// so [Engine.AggregateRange] answers a range-covering aggregate from it without decoding the
	// value column. It costs a little storage per series; off by default. AggregateRange works
	// without it (via decoding), just without the fast path.
	AggregateStats bool
	// RecentWindow enables an in-memory recent tier (nanoseconds): the most recent flush window is
	// mirrored in RAM across flushes so a query whose [Start, End] falls inside it is answered from
	// the tier ∪ the head without decoding any file part — first-touch recent-range queries skip the
	// decode path entirely (the decode cache only helps repeats). It trades a bounded uncompressed
	// window of resident memory for that latency. 0 disables it (the default); the head is then just
	// the unflushed tail, drained on every flush as before.
	RecentWindow int64
	// DecodeMemoryBytes caps the total in-flight decoded column bytes across concurrent queries: a
	// query reserves its estimated decode footprint before reading parts and releases it when done,
	// blocking when the budget is exhausted. It bounds the query-concurrency RSS cliff (N heavy
	// queries each materializing whole columns) by serializing decode through the cap rather than
	// letting concurrency multiply resident decoded bytes. 0 ⇒ unlimited (no admission control). A
	// query larger than the whole budget runs alone (it cannot be bounded below its own footprint).
	DecodeMemoryBytes int64
	// DecodeBudget, when non-nil, is a pre-built decode-memory budget this engine reserves from
	// instead of building its own from DecodeMemoryBytes. Share one [DecodeBudget] across engines
	// (one engine per tenant) so the cap bounds the process-wide in-flight decoded bytes rather
	// than multiplying per tenant. Takes precedence over DecodeMemoryBytes.
	DecodeBudget *DecodeBudget
	// MinFreeBytes is the headroom the engine leaves unused on a backend that reports its free
	// space: a flush is refused, and the ingest path starts rejecting, once the medium holds less
	// than the pending part plus this. It leaves a merge room for its output — a merge must write
	// before it can retire the inputs it frees, so a disk at 100% cannot compact its way out. 0 ⇒
	// [diskguard.DefaultReserveBytes]; negative ⇒ the byte axis is not checked.
	MinFreeBytes int64
	// MinFreeInodes is the same headroom on the object-count axis, for a backend that reports free
	// inodes. It is checked separately because a part is many small objects: an inode table can
	// exhaust with the disk half empty, and byte accounting cannot see it. 0 ⇒
	// [diskguard.DefaultReserveInodes]; negative ⇒ the inode axis is not checked.
	MinFreeInodes int64
	// MetricBlockRows sets the row block size for metric part columns (ts/value/sf): the columns are
	// split into independently decodable blocks of this many rows, so a query can decode only the
	// blocks its matched series' row ranges touch (sub-part seek) instead of the whole column, and
	// the block boundaries drive the part's marks granules. 0 ⇒ [DefaultMetricBlockRows]. A finer
	// size skips more on sparse selectors at a small per-block header cost.
	MetricBlockRows int
}

// DefaultMetricBlockRows is the metric part block size used when [Config.MetricBlockRows] is 0. It
// is finer than the historical 8192-row granule so a sparse selector (a small fraction of a part's
// series, scattered by SeriesID hash) touches a smaller fraction of blocks.
const DefaultMetricBlockRows = 1024

// Engine is a single tenant's storage engine. Safe for concurrent use.
type Engine struct {
	cfg Config
	mu  sync.RWMutex
	// flushMu serializes the *whole* body of a flush or merge (both mutate parts off e.mu). The facade
	// drives them from one maintenance goroutine, but the type is exported and Close/Reset are callable
	// from anywhere, so the single-mutator invariant is enforced here rather than assumed. Always taken
	// before e.mu, never while holding it.
	flushMu sync.Mutex
	head    *head
	parts   []*part
	// idleMerges counts consecutive merges that selected nothing, so the selector can waive its
	// write-amplification guard for parts that would otherwise never merge (see pickMergeRun).
	// Written only under flushMu, which a merge holds across its whole body; atomic so
	// [Engine.MergeShape] can read it off that lock.
	idleMerges atomic.Int64
	// lastMergeCap is the seal threshold the most recent merge derived (0 until one has run, or when
	// sealing is disabled). [Engine.MergeShape] reports and reasons against it because deriving the
	// cap reads the backend's free space, which an introspection call must not do.
	lastMergeCap atomic.Int64
	// mergeRunning is true while a [Engine.MergeWith] is executing (introspection liveness; see
	// [Engine.MergeRunning]). Set/cleared around the merge, not held during it.
	mergeRunning atomic.Bool
	// retiring holds parts removed from the live set by flush/merge, pending backend deletion once
	// their in-flight fetch readers drain (deferred reclamation; see reclaim.go).
	retiring []*part
	// flushing holds the sample buffers detached from the head by an in-progress flush, kept readable
	// by fetch until the flushed part is published (then cleared, atomically with adding the part) — so
	// a fetch never loses sight of records mid-flush. nil when no flush is in flight.
	flushing map[signal.SeriesID]*sampleBuf
	// recent is the in-memory recent tier (issue #25 item 4): a read-side mirror of the most recent
	// flush window, persisted across flushes, that lets planFetch skip part decode for a query whose
	// range falls inside it. nil when [Config.RecentWindow] is 0 (disabled).
	recent    map[signal.SeriesID]*sampleBuf
	recentMin int64 // oldest ts retained in the recent tier (maxInt64 ⇒ empty ⇒ short-circuit off)
	// walB groups a durable AppendBatch's WAL frames by series (reused under e.mu); nil head-only.
	walB *walBatch
	// flushedEpoch is the WAL flush watermark: the generation of the most recently flushed head
	// (persisted in the bucket index). Current head records are written to the WAL at flushedEpoch+1,
	// so on recovery the engine replays only WAL segments past flushedEpoch — exactly-once even when
	// the segments outlive their checkpoint (a node that stops being the shard's compaction owner
	// stops checkpointing, but its parts still arrive from the owner).
	flushedEpoch uint64
	// generation is the commit generation of the last bucket index this engine wrote, advanced on
	// every write. It is deliberately *not* reset by Reset: dropping the data does not entitle the
	// engine to write an index a replica would refuse as stale.
	generation bucketindex.Generation
	// indexed is the part set of the last bucket index this engine wrote, and removals the
	// tombstones it carried. Together they turn the next write's diff into a statement: a part
	// that was indexed and is not any more was removed *by this writer*, which is what lets a
	// replica tell a compaction from a loss.
	indexed  map[string]struct{}
	removals []bucketindex.Removal
	// indexVersion is the backend version of the bucket index this engine last read or committed —
	// the token its next commit conditions on, so a rewrite that another writer got in front of is
	// refused rather than silently overwriting it (#392).
	indexVersion backend.Version
	// foreign are index entries a *concurrent* writer committed over this prefix, learned when a
	// commit lost the race and reloaded. They are carried into every later commit: this engine
	// knows nothing about those parts but must not drop them, because the entry is all that keeps
	// the part reachable. Empty for the single-writer case, which never reloads.
	foreign []bucketindex.Entry
	// blockCache memoizes decoded column blocks across fetches (LRU, keyed by part/column/block); nil
	// ⇒ decode every fetch. A fetch caches only the blocks its matched series touch, so the resident
	// set is the useful blocks across live parts rather than every whole part touched.
	blockCache *blockCache
	// decPool recycles decodedPart column buffers on the no-cross-fetch-cache path: a fetch borrows
	// them to decode a part and returns them on releaseParts (safe — the merge copies values out, so
	// no result aliases them). This kills the per-query decode-buffer allocation (chunk.resize). It is
	// a GC-stable [pool.FreeList], not a sync.Pool: sync.Pool is cleared on every GC, so under
	// allocation-driven collection bursts the decode buffers lose their capacity and chunk.resize
	// reallocates from zero each fetch (the disk_io profile showed ~38 GB/35 s of resize churn).
	// FreeList entries are rooted live references, so capacity survives GC.
	decPool *pool.FreeList[decodedPart]
	// i64Res / f64Res recycle a fetched batch's result timestamp / value buffers (and the per-series
	// head/flush window buffers). They are GC-stable [sliceFreeList]s for the same reason as decPool:
	// a sync.Pool is emptied on every GC, so under allocation-driven collection the result buffers
	// lost their capacity and collect re-minted them each fetch (the ensureCap churn in the query
	// alloc profile). They are fed only when a caller calls [fetch.Batch.Release]; a caller that
	// never releases leaves them empty, so collect makes fresh slices exactly as before — the
	// default path takes nothing.
	i64Res resultFreeList[int64]
	f64Res resultFreeList[float64]
	// recycle is the shared per-engine [fetch.Batch.Release] hook (allocated once), so setting it on
	// a batch costs nothing per batch. It returns the batch's ts/value buffers to the pools above.
	recycle func(*fetch.Batch)
	// budget caps the in-flight decoded bytes across concurrent queries (Config.DecodeBudget or
	// Config.DecodeMemoryBytes); possibly shared with other engines; nil ⇒ unlimited.
	budget *DecodeBudget
	// identityDirty is set when a merge dropped rows or parts, so identities may now be dead and an
	// identity prune ([Engine.PruneIdentities]) has something to look for. Identities die no other
	// way, so an engine whose data has only grown skips even the live-set walk.
	identityDirty bool
	// space latches disk pressure: a flush that finds the backend short of bytes or inodes (or one
	// that gets ENOSPC anyway) closes the ingest path until a later flush finds room. Without it a
	// full disk is invisible — the write is acked, the flush fails, and the head grows behind it.
	space *diskguard.Guard
	// planMaps recycles the per-fetch plan maps (series identity + head/flush/recent snapshots) so a
	// fetch reuses cleared maps instead of allocating and growing fresh ones each call.
	planMaps planMapPools
}

var _ fetch.Fetcher = (*Engine)(nil)

// New returns an engine with an empty head.
func New(cfg Config) *Engine {
	if cfg.Obs == nil {
		cfg.Obs = obs.NewNop()
	}

	e := &Engine{cfg: cfg, head: newHead()}
	e.space = diskguard.New(diskguard.Reserve{Bytes: cfg.MinFreeBytes, Inodes: cfg.MinFreeInodes})
	// The decode free list covers the peak in-flight decoded parts: prefetch can decode
	// prefetchConcurrency parts concurrently per fetch, and several fetches overlap, so a
	// small multiple of GOMAXPROCS bounds the live set without over-retaining. Buffers
	// beyond this are dropped (GC'd), bounding memory to ≈ cap × part-size.
	e.decPool = pool.NewFreeList[decodedPart](pool.DefaultCapacity(prefetchConcurrency * 4))
	// One shared release hook for every batch this engine produces (no per-batch closure alloc).
	e.recycle = func(b *fetch.Batch) {
		e.putI64(b.Timestamps)
		e.putF64(b.Values)
	}

	if cfg.WAL != nil {
		e.walB = newWALBatch()
		cfg.WAL.SetEpoch(e.flushedEpoch + 1) // first head generation
	}

	if cfg.DecodeCacheBytes > 0 {
		e.blockCache = newBlockCache(cfg.DecodeCacheBytes)
	}

	switch {
	case cfg.DecodeBudget != nil:
		e.budget = cfg.DecodeBudget
	case cfg.DecodeMemoryBytes > 0:
		e.budget = NewDecodeBudget(cfg.DecodeMemoryBytes)
	}

	if cfg.RecentWindow > 0 {
		e.recent = make(map[signal.SeriesID]*sampleBuf)
		e.recentMin = maxInt64
	}

	return e
}

// metricSignal is the signal label for this engine's observability (it is the metrics engine).
const metricSignal = "metric"

// Append ingests one sample for series s, logging to the WAL when durable. It returns
// whether the sample was accepted (false ⇒ rejected as out-of-order beyond the window).
func (e *Engine) Append(s signal.Series, ts int64, value float64) (bool, error) {
	if err := e.refuseWrite(); err != nil {
		return false, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	id, accepted, logSeries := e.head.append(s, ts, value, e.cfg.OOOWindow)
	if !accepted {
		return false, nil
	}

	if e.cfg.WAL != nil {
		if logSeries {
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
	if err := e.refuseWrite(); err != nil {
		return AppendResult{}, err
	}

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

		out, effID, logSeries, s := e.head.appendByID(ids[i], ts[i], values[i], w, e.cfg.OOOWindow, limits, mat)

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
			e.walB.add(effID, ts[i], values[i], w, logSeries, s)
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

// IdentityBytes returns the resident bytes of the engine's identity state — the symbol table, the
// series index, the postings lists and the per-series out-of-order watermarks. It is reported
// separately from [Engine.HeadBytes] because a flush does not drain it: identities outlive their
// samples and are cleared only by [Engine.Reset], so this number tracks the engine's all-time
// series count, not its buffered data.
func (e *Engine) IdentityBytes() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.head.identityBytes()
}

// Stats is an in-memory snapshot of an engine's state for introspection (no backend I/O, no decode).
type Stats struct {
	Series      int64 // distinct series ever seen (index span: head ∪ flushed)
	HeadSamples int64 // samples currently buffered in the head (unflushed)
	HeadBytes   int64 // head's buffered sample bytes (the in-flight memory measure)
	// IdentityBytes is the resident identity state (symbols + series index + postings + OOO
	// watermarks) — memory a flush does not drain, and which no other counter here reports.
	IdentityBytes int64
	Parts         int   // flushed immutable parts
	MinTime       int64 // oldest flushed sample time (unix ns); 0 when no parts
	MaxTime       int64 // newest sample time across parts and the head (unix ns); 0 when empty
	// OutOfSpace is set while the engine refuses writes because its backend is out of bytes or
	// inodes. Reads still answer from what is on disk; it clears when a flush finds room again.
	OutOfSpace bool
}

// Stats returns an in-memory snapshot of the engine's state under a single read lock. It does no
// backend I/O and decodes nothing, so it is safe to poll at dashboard cadence without touching the
// hot path. Part byte sizes are not included (they would require backend stat calls).
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s := Stats{
		Series:        int64(e.head.series.Len()),
		OutOfSpace:    e.space.Exhausted(),
		HeadBytes:     e.head.bytes,
		IdentityBytes: e.head.identityBytes(),
		Parts:         len(e.parts),
		MaxTime:       e.head.newest,
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
// and yields one batch per series with its samples in the window, merged across the head
// buffer and every part by timestamp.
//
// The iterator is **streaming**: a series' samples are gathered on the [fetch.Iterator.Next] that
// yields them, so a consumer that folds and releases each batch stays O(1) in matched series
// instead of O(matched series x samples). The acquired parts, the decode-memory reservation and
// the profiling/observability accounting therefore live for the whole iteration and are settled by
// [fetch.Iterator.Close] — a caller **must** Close (directly or via [fetch.Drain]), otherwise the
// parts stay pinned and the decode budget stays reserved.
func (e *Engine) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	ctx = e.cfg.Obs.Base(ctx)
	ctx, span := e.cfg.Obs.Tracer.Start(ctx, "engine.fetch",
		trace.WithAttributes(attribute.String("storage.prefix", e.cfg.Prefix)))

	ctx, pf := profile.Begin(ctx, "engine.fetch")

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

	// Plan under the read lock: acquire the in-window parts to read and snapshot each series' head
	// (and any mid-flush) samples + identity, so the part reads below run lock-free — appends and
	// flush/merge proceed concurrently, and the acquired parts can't be reclaimed until released.
	plan := e.planFetch(ids, r)
	e.mu.RUnlock()

	// Reserve this fetch's decode footprint from the memory budget (blocks under concurrency
	// pressure), off the engine lock, before any part is decoded. A fetch materializes ts+value.
	if err := plan.acquireDecodeBudget(ctx, r, colNeed{values: true}); err != nil {
		plan.releaseParts()
		span.RecordError(err)
		span.End()
		pf.End()

		return nil, err
	}

	// Prefetch: decode the parts this fetch will touch concurrently (and cache them), so their
	// backend reads + decodes overlap instead of happening one part at a time during the merge.
	e.prefetch(ctx, plan)

	_, spf := profile.Begin(ctx, "scan")

	return &fetchIter{
		e:       e,
		plan:    plan,
		recycle: r.Recycle,
		ctx:     ctx,
		span:    span,
		pf:      pf,
		spf:     spf,
		log:     log,
		startNs: startNs,
		// Parts actually scanned this fetch — the recent tier may have short-circuited acquisition.
		partsScanned: len(plan.liveParts),
	}, nil
}

// enginePlan is the lock-free-readable plan a fetch builds under the engine read lock: the acquired
// (ref-held) in-window parts still to read, plus each series' identity and its already-snapshotted head
// and mid-flush samples. Its [enginePlan.mergeSeries] does the part reads off the lock.
type enginePlan struct {
	ids       []signal.SeriesID
	series    []signal.Series                  // matched-series identities, parallel to ids (zero value when absent)
	headB     map[signal.SeriesID]*fetch.Batch // head-window samples, copied under the lock
	flushB    map[signal.SeriesID]*fetch.Batch // mid-flush detached samples (not yet a part)
	recentB   map[signal.SeriesID]*fetch.Batch // recent-tier samples (the in-RAM flush window)
	liveParts []*part
	decoded   partDecodeCache // per-fetch decode memo so each part decodes once, not once per series
	// blockReaders streams a part's matched series straight from cached column blocks (no whole-part
	// decodedPart); a part absent from the map (cache off, or a const/legacy-unblocked column) uses
	// the decoded-part path instead. Built once in planFetch.
	blockReaders map[*part]*seriesBlockReader
	engine       *Engine      // for returning pooled decode buffers on release
	budgetBytes  int64        // decode-memory budget reserved for this query; released on releaseParts
	budgetScope  *fetch.Scope // the query scope budgetBytes was charged against, if any
	// partRanges and partBlocks memoize each block-sliced part's matched row runs and the blocks
	// they span — what the budget estimate must know and what the prefetch then warms. Both are
	// filled once, serially, by decodeEstimate and only read afterwards (by the prefetch's per-part
	// goroutines and the scan), so they need no lock; nil maps just mean every reader recomputes.
	partRanges map[*part][]rowRange
	partBlocks map[*part][]int
	start, end int64
	// memActive is the count-shaped plan's replacement for the head/flush/recent batch snapshots:
	// one existence flag per matched id (any in-memory sample in [start, end]), computed under the
	// plan lock by scanning the live buffers directly — no per-series sorted copy, no batch maps.
	// nil on a full fetch plan (which snapshots real batches instead).
	memActive []bool
	// samplesDecoded counts the samples an aggregate read decoded and folded, as opposed to answered
	// from a part's stats sidecar. It is the input quantity that explains the read's cost — the
	// emitted windows say nothing about it — and it is a plain int, not an atomic: an aggregate read
	// drains its plan on one goroutine, and the count must be free to keep in the fold loop.
	samplesDecoded int
}

// mergeSeries gathers series id's samples lock-free: each acquired part oldest→newest, then the
// mid-flush samples, then the head samples last — so on a duplicate timestamp the freshest value wins.
// decodePart returns part's decoded columns for this fetch, memoized so a part decodes once however
// many series read it. On the no-cache path it decodes only the blocks the fetch's matched series
// touch (series-skip): rangesFor gives the part's matched row runs, so a sparse selector decodes a
// fraction of the part's column blocks. With a cross-fetch cache the part decodes whole (the cache
// shares it across queries with different matched series; block-keyed caching is a follow-up).
func (p *enginePlan) decodePart(ctx context.Context, part *part) (*decodedPart, error) {
	if d, ok := p.decoded[part]; ok {
		return d, nil
	}

	ranges, err := p.rangesFor(ctx, part)
	if err != nil {
		return nil, err
	}

	d, err := p.engine.decodeOf(ctx, part, colNeed{values: true}, ranges)
	if err != nil {
		return nil, err
	}

	p.decoded[part] = d

	return d, nil
}

// rangesFor returns the row runs of the plan's matched series that part holds — the input to the
// series-skip decode. The result is the union of the part's per-series ranges; it need not be sorted
// (neededBlocks unions their blocks regardless). The budget estimate already looks these up for a
// block-sliced part, so the answer is served from that memo when it is there rather than repeating
// one index lookup per matched series.
func (p *enginePlan) rangesFor(ctx context.Context, part *part) ([]rowRange, error) {
	if rngs, ok := p.partRanges[part]; ok {
		return rngs, nil
	}

	out := make([]rowRange, 0, len(p.ids))

	for _, id := range p.ids {
		rng, ok, err := part.index.lookup(ctx, id)
		if err != nil {
			return nil, err
		}

		if ok {
			out = append(out, rng)
		}
	}

	return out, nil
}

// blocksFor returns the blocks of pt that the plan's matched series span and that can hold an
// in-window sample, from the budget estimate's memo when it ran (it needs the same set to size the
// reservation) and computed otherwise — a nil budget leaves nothing memoized.
func (p *enginePlan) blocksFor(ctx context.Context, pt *part, r *seriesBlockReader, ranges []rowRange) []int {
	if blks, ok := p.partBlocks[pt]; ok {
		return blks
	}

	blks, _ := windowBlocks(ranges, r.blockRows, pt.rows(), r.granules(ctx), p.start, p.end)

	return blks
}

func (p *enginePlan) mergeSeries(ctx context.Context, id signal.SeriesID) (sampleMerge, error) {
	var m sampleMerge

	for _, part := range p.liveParts {
		rng, ok, err := part.index.lookup(ctx, id)
		if err != nil {
			return m, err
		}

		if !ok {
			continue
		}

		// Block-sliceable part with a cache: stream the series' rows straight from cached blocks (no
		// whole-part decodedPart). Otherwise decode the part once (memoized) and slice it.
		if r := p.blockReaders[part]; r != nil {
			if err := r.addRange(ctx, rng, &m, p.start, p.end); err != nil {
				return m, err
			}

			continue
		}

		d, err := p.decodePart(ctx, part)
		if err != nil {
			return m, err
		}

		d.mergeSeriesInto(rng, &m, p.start, p.end)
	}

	if fb := p.flushB[id]; fb != nil {
		m.add(fb.Timestamps, fb.Values, fb.ScaleFactors, p.start, p.end)
	}

	if rb := p.recentB[id]; rb != nil {
		m.add(rb.Timestamps, rb.Values, rb.ScaleFactors, p.start, p.end)
	}

	if hb := p.headB[id]; hb != nil {
		m.add(hb.Timestamps, hb.Values, hb.ScaleFactors, p.start, p.end)
	}

	return m, nil
}

// releaseParts releases the fetch's hold on its acquired parts, letting a retired part be reclaimed.
// acquireDecodeBudget reserves this query's estimated decode footprint from the engine's
// decode-memory budget, blocking until it fits (or admitting it alone when it exceeds the whole
// budget). It must run off the engine lock and after planFetch (it needs liveParts); releaseParts
// returns the reservation. A nil budget makes it a no-op.
//
// It fails when ctx ends first, holding nothing: the caller must releaseParts and abandon the
// query. Nothing else aborts the wait — a wait that outlasts the budget's force interval is
// admitted over the ceiling instead (counted, and logged here with the estimate that overshot it).
func (p *enginePlan) acquireDecodeBudget(ctx context.Context, r fetch.Request, need colNeed) error {
	if p.engine.budget == nil {
		return nil
	}

	scope := r.Scope
	if scope == nil {
		scope = fetch.ScopeFrom(ctx)
	}

	est, err := p.decodeEstimate(ctx, need)
	if err != nil {
		return err
	}

	p.budgetBytes = est
	p.budgetScope = scope

	forced, err := p.engine.budget.acquireFor(ctx, scope, p.budgetBytes)
	if err != nil {
		// Nothing was reserved; make releaseParts a no-op on the budget rather than a double release.
		p.budgetBytes, p.budgetScope = 0, nil

		return err
	}

	if forced {
		p.engine.cfg.Obs.Fetch.ForcedAdmission(ctx, metricSignal)
		zctx.From(ctx).Warn("decode budget forced admission: waited past the force interval with no progress",
			zap.Int64("estimate_bytes", p.budgetBytes),
			zap.Int64("in_flight_bytes", p.engine.budget.inFlight()),
			zap.Bool("scoped", scope != nil),
		)
	}

	return nil
}

// decodeEstimate is the bytes this query will materialize across the parts it touches: the full ts
// column always, plus the value column (and the scale-factor column when present) when need.values.
// A decoded column is one int64/float64 per row, so 8 bytes/row/column.
//
// What a part costs depends on how it will be read. A whole-part decode is sized to the part's row
// count even for a sparse selector, so the part's rows are the honest number. A block-sliced part
// materializes only the blocks its matched series fall in — it pins each one whole until
// releaseParts, and an evicted-but-pinned block stays live, so the block is the right unit but the
// slice inside it is not (bufFreeList.get dominated the live heap under 8-way load, see
// oteldb/oteldb#1124). Charging those parts for the whole store made the estimate grow with the
// data while the footprint stayed with the query, and any query on a large store then exceeded the
// whole budget and was admitted alone — turning the ceiling into a global query lock.
//
// A block-sliced part is charged twice over, for the blocks it pins *and* for the matched rows
// inside them, because both are live at once: collect copies a series' samples into the result
// buffers while the blocks they came from are still pinned for the rest of the scan. Counting only
// the pins measured 4.9x low under 32-way load — the pins are the smaller half on a selective query
// that spans many blocks.
//
// Both halves are counted over the blocks that survive granule time pruning, which is what keeps a
// narrow query on a time-wide part from reserving the whole part.
//
// It still over-estimates elsewhere: blocks shared between concurrent fetches are counted once per
// fetch. The budget trades that extra queueing for the ceiling actually holding.
func (p *enginePlan) decodeEstimate(ctx context.Context, need colNeed) (int64, error) {
	var total int64

	ranges := make(map[*part][]rowRange, len(p.liveParts))
	blocks := make(map[*part][]int, len(p.liveParts))

	for _, pt := range p.liveParts {
		cols := int64(1) // timestamps
		if need.values {
			cols++ // values
			if pt.hasSF {
				cols++ // scale factors
			}
		}

		r := p.blockReaders[pt]
		if r == nil {
			total += int64(pt.rows()) * 8 * cols

			continue
		}

		rngs, err := p.rangesFor(ctx, pt)
		if err != nil {
			return 0, err
		}

		blks, matched := windowBlocks(rngs, r.blockRows, pt.rows(), r.granules(ctx), p.start, p.end)
		ranges[pt], blocks[pt] = rngs, blks

		pinned := min(int64(len(blks))*int64(r.blockRows), int64(pt.rows()))

		total += (pinned + matched) * 8 * cols
	}

	p.partRanges, p.partBlocks = ranges, blocks

	return total, nil
}

// releaseSeriesPins releases every block reader's per-series pins (keeping the memoized blocks
// pinned). Call it after a series' collect has copied its samples out of the merge — the views the
// pins protected are dead — so evicted blocks' buffers recirculate while the fetch is running.
func (p *enginePlan) releaseSeriesPins() {
	for _, r := range p.blockReaders {
		r.releaseSeriesPins()
	}
}

func (p *enginePlan) releaseParts() {
	p.engine.budget.releaseFor(p.budgetScope, p.budgetBytes)

	// Drop the block-slice readers' remaining references on the cache blocks they viewed (the scan
	// releases per series; this sweeps the memoized leftovers). Any views are dead by now; releasing
	// lets a block the byte budget evicted mid-fetch return its buffer to the decode pool. Must run
	// before part.release below: a released part can be reclaimed (evictPrefix), which touches the
	// same entries.
	for _, r := range p.blockReaders {
		r.releasePins()
	}

	// The fetch stops drawing decode buffers here; shrink the freelist scaling back down.
	if p.blockReaders != nil {
		p.engine.blockCache.fetchEnd()
	}

	// Return pooled decode buffers (no-cross-fetch-cache path). The merge has already copied the
	// values out into the result batches, so these slices are dead and safe to recycle. Cache-path
	// dps are not pooled (the cache owns them until reclaim), so they are skipped here.
	//
	// This MUST run before the part.release() loop below: a cache-path dp is shared with reclaim,
	// which recycles it once the part's refs reach 0. Touching dp (even reading dp.pooled) after
	// releasing the part would race that recycle; doing it while the refs are still held cannot.
	for _, dp := range p.decoded {
		if dp.pooled {
			p.engine.recycleDecoded(dp)
		}
	}

	for _, part := range p.liveParts {
		part.release()
	}

	// Recycle the per-series head/flush window buffers drawn by winBuffers. The merge has likewise
	// copied their samples out, so the backing ts/value slices are dead — returning them to the
	// engine pool is what stops the head fetch path re-allocating a pair per series each fetch.
	for _, b := range p.headB {
		p.engine.putI64(b.Timestamps)
		p.engine.putF64(b.Values)
	}

	for _, b := range p.flushB {
		p.engine.putI64(b.Timestamps)
		p.engine.putF64(b.Values)
	}

	// The plan maps are dead now: the returned batches copied out their identity and samples. Clear
	// and recycle the map structures so the next fetch reuses their capacity instead of re-making them.
	p.engine.putSeriesSlice(p.series)
	p.engine.putBatchMap(p.headB)
	p.engine.putBatchMap(p.flushB)
	p.engine.putBatchMap(p.recentB)
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

// collectMany k-way-merges several sorted runs into the destination buffers, emitting each
// timestamp once and taking its value/weight from the highest-indexed (freshest) run that holds it.
//
// The scan finds the two smallest heads rather than just the smallest, which turns the common case
// into a copy: while the leading run's timestamps stay strictly below every other head, no other run
// can tie or win, so that whole stretch is the answer verbatim and moves in one append. Only an
// actual tie falls back to a per-row step. That matters because the fan-in is not "parts + flush +
// head" — a block-sliced part contributes one run per block ([seriesBlockReader.addRange]), so a
// two-day range over six parts arrives as ~40 runs whose stretches are hundreds of rows long. Per
// row the old O(rows × runs) scan re-derived what one comparison per stretch establishes.
func collectMany(runs []tsRun, tsBuf []int64, valsBuf []float64) (tsOut []int64, values, sf []float64) {
	total := 0
	for i := range runs {
		total += len(runs[i].ts)
	}

	tsOut = ensureCap(tsBuf, total)
	values = ensureCap(valsBuf, total)

	// Per-run cursors. Sized for a block-sliced fan-in so the usual query stays off the heap.
	var curArr [64]int

	var cur []int
	if len(runs) <= len(curArr) {
		cur = curArr[:len(runs)]
	} else {
		cur = make([]int, len(runs))
	}

	for {
		lead, leadTs, rivalTs, rival := leadRun(runs, cur)
		if lead < 0 {
			break
		}

		if rival && leadTs == rivalTs {
			// Tie: every run sitting on this timestamp advances, and the freshest supplies the value.
			var winVal, winW float64 = 0, 1

			for i := range runs {
				if cur[i] < len(runs[i].ts) && runs[i].ts[cur[i]] == leadTs {
					winVal, winW = runs[i].vals[cur[i]], runs[i].weight(cur[i])
					cur[i]++
				}
			}

			tsOut = append(tsOut, leadTs)
			values = append(values, winVal)
			sf = appendWeight(sf, winW, len(values), total)

			continue
		}

		r, lo := &runs[lead], cur[lead]

		hi := len(r.ts)
		if rival {
			hi = lo + lowerBound(r.ts[lo:], rivalTs)
		}

		tsOut, values, sf = emitRange(r, lo, hi, tsOut, values, sf, total)
		cur[lead] = hi
	}

	return tsOut, values, sf
}

// leadRun returns the run holding the smallest unconsumed timestamp, that timestamp, and the
// smallest timestamp among the other runs — the exclusive bound the leader may run to unchallenged.
// lead is -1 when every run is consumed; rival is false when only the leader has rows left.
func leadRun(runs []tsRun, cur []int) (lead int, leadTs, rivalTs int64, rival bool) {
	lead = -1

	for i := range runs {
		if cur[i] >= len(runs[i].ts) {
			continue
		}

		switch t := runs[i].ts[cur[i]]; {
		case lead < 0 || t < leadTs:
			if lead >= 0 && (!rival || leadTs < rivalTs) {
				rivalTs, rival = leadTs, true
			}

			lead, leadTs = i, t
		case !rival || t < rivalTs:
			rivalTs, rival = t, true
		}
	}

	return lead, leadTs, rivalTs, rival
}

// emitRange appends r's [lo, hi) rows, which the caller has established no other run can tie or
// beat. Unweighted rows go out as a bulk copy; weights force the per-row path that materializes sf.
func emitRange(
	r *tsRun, lo, hi int, tsOut []int64, values, sf []float64, capHint int,
) ([]int64, []float64, []float64) {
	if r.sf == nil && sf == nil {
		return append(tsOut, r.ts[lo:hi]...), append(values, r.vals[lo:hi]...), nil
	}

	for k := lo; k < hi; k++ {
		tsOut = append(tsOut, r.ts[k])
		values = append(values, r.vals[k])
		sf = appendWeight(sf, r.weight(k), len(values), capHint)
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
// without reallocating the engine itself. It waits for an in-flight flush or merge to finish
// first, so that operation cannot publish its part into the reset engine. Flushed part objects
// are deleted from the backend so none are orphaned — except those a concurrent fetch is still
// reading, which are retired and deleted by the deferred reclaim once the reader drains. It is
// destructive (it wipes this engine's parts under [Config.Prefix]) and is meant for the ephemeral
// in-memory engine in tests and benchmarks,
// letting a long-lived engine be reused across runs. Safe for concurrent use.
func (e *Engine) Reset(ctx context.Context) error {
	e.flushMu.Lock() // drain an in-flight flush/merge; it would otherwise publish into the empty engine
	defer e.flushMu.Unlock()

	e.mu.Lock()
	e.head = newHead()
	e.flushing = nil        // discarded with the head: Reset drops the samples, it does not flush them
	e.identityDirty = false // nothing is left to prune

	if e.cfg.Backend == nil {
		e.parts, e.retiring = nil, nil
		e.mu.Unlock()

		return nil
	}

	e.retireLocked(e.parts)
	e.parts = nil
	e.mu.Unlock()

	// Delete the drained parts' objects (and re-queue the ones a fetch still holds), then sweep
	// whatever is left: all part keys are "{Prefix}/{seq}/...", so the "{Prefix}/" scope catches them
	// — plus the index objects — without touching a sibling engine's keys.
	e.reclaimRetired(ctx)

	e.mu.RLock()
	pending := make([]string, 0, len(e.retiring))

	for _, p := range e.retiring {
		pending = append(pending, p.prefix+"/")
	}

	e.mu.RUnlock()

	keys, err := e.cfg.Backend.List(ctx, e.cfg.Prefix+"/")
	if err != nil {
		return errors.Wrap(err, "list parts")
	}

	for _, k := range keys {
		if slices.ContainsFunc(pending, func(p string) bool { return strings.HasPrefix(k, p) }) {
			continue // a fetch still holds this part; reclaimRetired deletes it once the reader drains
		}

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

// Replay rebuilds the head from the WAL segments in dir (durable restart). It skips segments at or
// below the flush watermark recovered by [Engine.LoadParts] (call LoadParts first), so records
// already in a flushed part are not re-applied — exactly-once recovery.
func (e *Engine) Replay(dir string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return wal.ReplayDirFrom(dir, e.flushedEpoch, e.replayHandlers())
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
	if err := e.refuseWrite(); err != nil {
		return nil, AppendResult{}, err
	}

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

	// The accepted frames are the primary's durable copy of the shard's unflushed head, the one the
	// quorum ack counts on: a restart replays them, instead of serving a hole for everything written
	// since the last flush. They are already framed for replication, so the log takes them verbatim.
	if err == nil && e.cfg.WAL != nil {
		err = e.cfg.WAL.WriteFrames(buf.Bytes())
	}

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
	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	if err := e.admitFlush(ctx); err != nil {
		return 0, err
	}

	// Plan (under lock): detach the head's sample buffers, keeping them readable via e.flushing so a
	// concurrent fetch never loses them.
	e.mu.Lock()
	detached := e.head.detach()
	if detached == nil {
		e.mu.Unlock()
		e.reclaimRetired(ctx) // nothing to flush, but still sweep pending deletions

		return 0, nil
	}

	e.flushing = detached
	// Snapshot the flushed series' identities while still under the lock: the part write runs
	// off-lock, and the resident index keeps being mutated by ingest.
	idents := e.head.identitiesOf(detached)
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
	for _, rg := range ranges {
		sub := cols.slice(rg[0], rg[1])
		prefix := e.newPartPrefix()

		if err := writePart(ctx, e.cfg.Backend, prefix, sub, idents,
			compressProfile{}, 0, e.cfg.AggregateStats, e.cfg.MetricBlockRows); err != nil {
			e.space.Observe(err)

			return 0, err
		}

		p, err := openPart(ctx, e.cfg.Backend, prefix)
		if err != nil {
			e.space.Observe(err)

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
	if e.recentEnabled() {
		e.populateRecent(detached)
	}
	e.flushing = nil
	// The parts about to be committed supersede every WAL record logged so far, so the watermark
	// they carry retires that generation and the next head records open a new one.
	e.flushedEpoch++
	err := e.publishLocked(ctx)
	e.mu.Unlock()

	if err != nil {
		return rows, err
	}

	e.reclaimRetired(ctx)

	return rows, nil
}

// publishLocked persists the engine's part set (the bucket index) and checkpoints the WAL — the
// now-durable part makes its WAL records obsolete. Caller holds e.mu.
func (e *Engine) publishLocked(ctx context.Context) error {
	// Ordering is a durability invariant: a part's own objects — including the identity object
	// naming its series — are all written before the bucket index, which is what makes the part
	// durably visible, so a readable part always has the identities its rows resolve through. The
	// reverse leftover is harmless: a part whose objects were written but never committed is an
	// orphan the next open sweeps, and its identities were never loaded.
	if err := e.updateIndexLocked(ctx); err != nil {
		return err
	}

	if e.cfg.WAL != nil {
		e.cfg.WAL.SetEpoch(e.flushedEpoch + 1)

		return e.cfg.WAL.Checkpoint()
	}

	return nil
}

// decodeOf decodes p through the cross-fetch decode cache when enabled: a hit returns the shared
// (immutable) decoded columns; a miss decodes and caches them. Without a cache it decodes plainly.
func (e *Engine) decodeOf(ctx context.Context, p *part, need colNeed, ranges []rowRange) (*decodedPart, error) {
	// Borrow a pooled decodedPart (a reclaimed part's recycled buffers when available); the fetch
	// returns it on releaseParts. The merge copies values out, so the buffers are free to reuse once
	// the fetch ends. Get returns nil when empty — allocate then. ranges (the matched series' row
	// runs) lets the decode skip the part's untouched blocks; nil ranges decodes the whole part.
	dp := e.decPool.Get()
	if dp == nil {
		dp = &decodedPart{}
	}

	var err error
	if e.blockCache != nil {
		// Cross-fetch block cache: assemble the matched blocks from the cache (decoding+caching the
		// misses), so a column is decoded once across fetches and only for the blocks it touches.
		err = e.assembleFromBlocks(ctx, dp, p, need, ranges)
	} else {
		// No cache: decode the matched blocks fresh into the pooled buffers each fetch.
		dp, err = p.decodeRangesInto(ctx, dp, need, ranges)
	}

	if err != nil {
		e.recycleDecoded(dp)

		return nil, err
	}

	dp.pooled = true

	return dp, nil
}

// recycleDecoded returns a decodedPart's column buffers to the decode pool (capacity preserved,
// contents discarded) for the next decode to reuse. Only call it on a dp no fetch can still be
// reading: a pool-path dp on releaseParts, or a cache dp whose part has been reclaimed (refs == 0).
func (e *Engine) recycleDecoded(dp *decodedPart) {
	if dp == nil {
		return
	}

	dp.pooled = false
	dp.haveValues = false
	dp.ts, dp.vals, dp.sf = dp.ts[:0], dp.vals[:0], dp.sf[:0]
	e.decPool.Put(dp)
}

// prefetchConcurrency bounds the parallel part decodes a single fetch's prefetch issues.
const prefetchConcurrency = 8

// prefetch concurrently decodes (and caches) the parts this fetch will actually touch — those
// holding at least one matched series — so the per-part backend reads and decodes overlap instead
// of running sequentially as the merge first reaches each part. It is a no-op without a decode
// cache or with fewer than two parts to touch (the lazy path is already optimal). Best-effort: a
// decode error here is ignored; the merge re-decodes and surfaces it.
func (e *Engine) prefetch(ctx context.Context, plan *enginePlan) {
	if e.blockCache == nil || len(plan.liveParts) < 2 {
		return
	}

	var todo []*part

	for _, pt := range plan.liveParts {
		// Best-effort membership probe: an index read error here includes the part anyway — the
		// warm/decode below (and ultimately the merge) surfaces any real failure.
		touched := false

		for _, id := range plan.ids {
			ok, err := pt.index.has(ctx, id)
			if err != nil || ok {
				touched = true

				break
			}
		}

		if touched {
			todo = append(todo, pt)
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

			// Warm only the blocks this fetch's matched series touch. A block-sliceable part decodes
			// its blocks straight into the cache (no whole-part buffer); otherwise fall back to the
			// decoded-part path and return the throwaway buffer to the pool.
			ranges, err := plan.rangesFor(ctx, p)
			if err != nil {
				return // best-effort: the merge re-reads and surfaces it
			}

			if r := plan.blockReaders[p]; r != nil {
				_ = r.warm(ctx, plan.blocksFor(ctx, p, r, ranges))

				return
			}

			dp, err := e.decodeOf(ctx, p, colNeed{values: true}, ranges)
			if err == nil {
				e.recycleDecoded(dp)
			}
		}(pt)
	}

	wg.Wait()
}

// acquireWindowParts acquires every live part overlapping [start, end] into the plan (time-pruning
// the rest) and, when withReaders and the block cache is on, builds a per-part block reader for
// each block-sliceable part. A count-shaped plan passes withReaders=false — it never merges block
// views (edge parts decode timestamps through the plan decode cache), so the per-part readers and
// the freelist concurrency registration would be dead weight. Caller holds e.mu.
func (e *Engine) acquireWindowParts(p *enginePlan, start, end int64, withReaders bool) {
	if withReaders && e.blockCache != nil {
		p.blockReaders = make(map[*part]*seriesBlockReader, len(e.parts))
		// Registered until releaseParts (which keys off p.blockReaders != nil), scaling the decode
		// freelists with fetch concurrency.
		e.blockCache.fetchStart()
	}

	for _, pt := range e.parts {
		if pt.maxTime < start || pt.minTime > end { // time-prune
			continue
		}

		pt.acquire()
		p.liveParts = append(p.liveParts, pt)

		if p.blockReaders != nil {
			if br := e.newSeriesBlockReader(pt); br != nil {
				p.blockReaders[pt] = br
			}
		}
	}
}

// planFetch selects and acquires the in-window parts and snapshots each series' head + mid-flush
// samples and identity — all under the lock — so the part reads run lock-free. Caller holds e.mu (read
// lock). The acquired parts must be released with releaseParts.
func (e *Engine) planFetch(ids []signal.SeriesID, r fetch.Request) *enginePlan {
	p := &enginePlan{
		ids:     ids,
		series:  e.getSeriesSlice(len(ids)),
		headB:   e.getBatchMap(len(ids)),
		flushB:  e.getBatchMap(len(ids)),
		decoded: make(partDecodeCache),
		engine:  e,
		start:   r.Start,
		end:     r.End,
	}

	// The recent tier short-circuit: when the query range falls inside the tier's window, the tier ∪
	// the mid-flush buffers ∪ the head cover it entirely, so no part is acquired or decoded.
	if !e.recentEnabled() || r.Start < e.recentMin {
		e.acquireWindowParts(p, r.Start, r.End, true)
	}

	for i, id := range ids {
		if s, ok := e.head.series.Get(id); ok {
			p.series[i] = s
		}

		if e.recentEnabled() {
			if rb := bufBatch(e.recent[id], id, p.series[i], r.Start, r.End, e); rb != nil {
				if p.recentB == nil {
					p.recentB = e.getBatchMap(len(ids))
				}

				p.recentB[id] = rb
			}
		}

		if hb := e.head.batch(id, r.Start, r.End, e); hb != nil {
			p.headB[id] = hb
		}

		if buf := e.flushing[id]; buf != nil {
			if fb := bufBatch(buf, id, p.series[i], r.Start, r.End, e); fb != nil {
				p.flushB[id] = fb
			}
		}
	}

	return p
}

// planExistence is [Engine.planFetch] for count-shaped reads, which need only per-series
// *existence* in the window: instead of snapshotting a sorted per-series copy of every head/flush/
// recent buffer into batch maps and materializing the full identity slab (fetch-shaped state a
// count never reads — under concurrent broad counts those slabs alone were the top live-heap
// entry), it computes one in-memory existence flag per matched id by scanning the live buffers
// directly under the lock. Identity is snapshotted only when the caller groups by it (CountBy).
// Parts are acquired without block readers (edge parts decode timestamps via the plan decode
// cache). Caller holds e.mu (read lock); release with releaseParts as usual.
func (e *Engine) planExistence(ids []signal.SeriesID, r fetch.Request, withIdentity bool) *enginePlan {
	p := &enginePlan{
		ids:       ids,
		decoded:   make(partDecodeCache),
		engine:    e,
		start:     r.Start,
		end:       r.End,
		memActive: make([]bool, len(ids)),
	}

	if withIdentity {
		p.series = e.getSeriesSlice(len(ids))
	}

	// The recent tier short-circuit, as in planFetch: a window inside the tier is fully covered by
	// the in-memory buffers, so no part is acquired or decoded.
	if !e.recentEnabled() || r.Start < e.recentMin {
		e.acquireWindowParts(p, r.Start, r.End, false)
	}

	recent := e.recentEnabled()

	for i, id := range ids {
		if withIdentity {
			if s, ok := e.head.series.Get(id); ok {
				p.series[i] = s
			}
		}

		p.memActive[i] = bufHasInWindow(e.head.samples[id], r.Start, r.End) ||
			bufHasInWindow(e.flushing[id], r.Start, r.End) ||
			(recent && bufHasInWindow(e.recent[id], r.Start, r.End))
	}

	return p
}

// bufHasInWindow reports whether buf holds any sample in [start, end]. The buffer may be unsorted
// (head buffers sort lazily on read), so this is a linear scan — but it copies and sorts nothing,
// which is the point: existence is the only thing a count needs from the in-memory tiers.
func bufHasInWindow(buf *sampleBuf, start, end int64) bool {
	if buf == nil {
		return false
	}

	for _, ts := range buf.ts {
		if ts >= start && ts <= end {
			return true
		}
	}

	return false
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
func (e *Engine) getI64() []int64 { return e.i64Res.get() }

// getF64 is [Engine.getI64] for float64 value buffers.
func (e *Engine) getF64() []float64 { return e.f64Res.get() }

// putI64 returns a buffer to its pool (only meaningfully reused if it has capacity). This is the
// opt-in Release path, never the default.
func (e *Engine) putI64(s []int64) { e.i64Res.put(s) }

// putF64 is [Engine.putI64] for float64 value buffers.
func (e *Engine) putF64(s []float64) { e.f64Res.put(s) }

// term is this writer's ownership term, or 0 with no cluster to ask.
func (e *Engine) term() uint64 {
	if e.cfg.Term == nil {
		return 0
	}

	return e.cfg.Term()
}
