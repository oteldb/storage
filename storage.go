package storage

import (
	"context"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/scale"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/tenant"
	"github.com/oteldb/storage/wal"
)

// Storage is the embeddable entry point (DESIGN.md §5). It is safe for concurrent use.
// Construct with [Open] or [InMemory]; never with a literal.
//
// All ingestion is push-based and OTLP-shaped: methods accept the library's internal,
// []byte-based, zero-alloc ingest batches (e.g. [metric.Metrics]) and return an [Accepted]
// carrying per-OTLP partial-success counts. OTel-Go users convert pdata to these via the
// optional otlp/pdataconv bridge, which keeps pdata off this hot path. Reads go through the
// language-agnostic [Storage.Fetcher] seam; query languages live in the embedder.
//
// All four signals are wired end-to-end: metrics ([Storage.WriteMetrics]/[Storage.Fetcher]) on the
// float-sample engine, and logs, traces, and profiles ([Storage.WriteLogs]/[Storage.WriteTraces]/
// [Storage.WriteProfiles] and their fetchers) on the shared record engine.
type Storage struct {
	opts    Options
	backend backend.Backend
	tenant  tenant.Resolver
	closed  atomic.Bool

	tmu            sync.Mutex
	tenants        map[signal.TenantID]*engine.Engine
	logTenants     map[signal.TenantID]*recordengine.Engine
	traceTenants   map[signal.TenantID]*recordengine.Engine
	profileTenants map[signal.TenantID]*recordengine.Engine

	cluster *clusterNode // cluster runtime (membership + replica server + routed writes); nil ⇒ single-node

	queryCache scale.Cache // shared results cache for Fetcher; nil ⇒ caching disabled

	// decodeBudget is the process-wide decode-memory admission budget ([Options.DecodeMemoryBytes]),
	// shared by every tenant engine so concurrent queries cannot multiply resident decoded bytes
	// past the cap. nil ⇒ unlimited.
	decodeBudget *engine.DecodeBudget

	admitMu sync.Mutex                           // guards admit
	admit   map[signal.TenantID]*tenantAdmission // per-tenant admission state (rate valve + counters)
	now     func() int64                         // unix-nano clock for admission; overridable in tests

	obs *obs.Obs // injected logging/tracing/metrics (no-op by default); never nil after Open

	stopCh chan struct{}  // closed by Close to stop the maintenance loop
	wg     sync.WaitGroup // tracks the maintenance goroutine
}

// Open constructs a [Storage] from [Options] (DESIGN.md §5). If [Options.Backend] is
// nil it defaults to [backend.Memory]; if the backend is ephemeral and no durability
// is chosen, it defaults to [DurabilityEphemeral] (the in-memory engine).
func Open(ctx context.Context, o Options, opts ...Option) (*Storage, error) {
	for _, opt := range opts {
		opt(&o)
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	o.applyDefaults()
	s := &Storage{
		opts:           o,
		backend:        o.Backend,
		tenant:         o.Tenancy,
		tenants:        make(map[signal.TenantID]*engine.Engine),
		logTenants:     make(map[signal.TenantID]*recordengine.Engine),
		traceTenants:   make(map[signal.TenantID]*recordengine.Engine),
		profileTenants: make(map[signal.TenantID]*recordengine.Engine),
		admit:          make(map[signal.TenantID]*tenantAdmission),
		now:            func() int64 { return time.Now().UnixNano() },
	}
	if s.tenant == nil {
		s.tenant = tenant.Default()
	}

	if o.DecodeMemoryBytes > 0 {
		s.decodeBudget = engine.NewDecodeBudget(o.DecodeMemoryBytes)
	}

	observer, err := obs.New(obs.Config{Logger: o.Logger, TracerProvider: o.TracerProvider, MeterProvider: o.MeterProvider})
	if err != nil {
		return nil, errors.Wrap(err, "init observability")
	}

	s.obs = observer

	// Meter the backend only when a meter is configured, so the default path is the bare backend.
	if o.MeterProvider != nil {
		s.backend = instrumentBackend(s.backend, s.obs.Backend)
	}

	// Wrap the cold tier in the read cache outermost, so a cache hit skips both metering and the
	// backend read. An ephemeral (in-memory) backend is already RAM, so it is never cached.
	if o.ReadCacheBytes > 0 && !s.backend.IsEphemeral() {
		s.backend = backend.Cached(s.backend, o.ReadCacheBytes)
	}

	if o.QueryCacheEntries > 0 {
		s.queryCache = scale.NewMemoryCache(o.QueryCacheEntries)
	}

	// Recover previously-flushed data from a durable backend so a fresh process serves it.
	if err := s.recover(ctx); err != nil {
		return nil, err
	}

	// Join the cluster (membership + replica server + routed writes) when configured.
	if o.Cluster != nil {
		if err := s.startCluster(ctx, o.Cluster); err != nil {
			return nil, errors.Wrap(err, "start cluster")
		}
	}

	walSyncEvery := o.walSyncInterval()
	if o.FlushInterval > 0 || walSyncEvery > 0 {
		s.stopCh = make(chan struct{})
	}

	if o.FlushInterval > 0 {
		s.wg.Add(1)

		// The maintenance loop's context is created inside the goroutine and scoped to
		// its own lifetime (stopped via stopCh), not to this Open call.
		go s.runMaintenance(time.Duration(o.FlushInterval)) //nolint:gosec,contextcheck // G118: loop-scoped context, see runMaintenance
	}

	if walSyncEvery > 0 {
		s.wg.Add(1)
		go s.runWALSync(walSyncEvery)
	}

	s.obs.Logger(ctx).Info("storage opened",
		zap.Bool("clustered", o.Cluster != nil),
		zap.Bool("ephemeral", o.Durability == DurabilityEphemeral),
		zap.Duration("flush_interval", time.Duration(o.FlushInterval)))

	return s, nil
}

// InMemory returns a fully in-memory, ephemeral [Storage] (DESIGN.md §5): equivalent
// to [Open] with [backend.Memory] and [DurabilityEphemeral]. It is the default in
// tests — a full ingest+query path with no disk or object store.
func InMemory(opts ...Option) (*Storage, error) {
	all := make([]Option, 0, 2+len(opts))
	all = append(all,
		WithBackend(backend.Memory()),
		WithDurability(DurabilityEphemeral),
	)
	all = append(all, opts...)
	return Open(context.Background(), Options{}, all...)
}

// Close releases all resources. It is idempotent. After [Close], every method on s
// returns [ErrClosed].
func (s *Storage) Close(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	s.obs.Logger(ctx).Debug("storage closing")

	if s.stopCh != nil {
		close(s.stopCh)
		s.wg.Wait()
	}

	// Final flush: drain every engine's head to a durable part.
	var firstErr error

	// Leave the cluster first (revoke lease, stop the replica server) so peers stop routing here.
	if s.cluster != nil {
		if err := s.cluster.close(ctx); err != nil {
			firstErr = err
		}
	}

	for _, eng := range s.engineSnapshot() {
		if err := eng.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, eng := range s.logEngineSnapshot() {
		if err := eng.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, eng := range s.traceEngineSnapshot() {
		if err := eng.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, eng := range s.profileEngineSnapshot() {
		if err := eng.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		s.obs.Logger(ctx).Warn("storage closed with errors", zap.Error(firstErr))
	} else {
		s.obs.Logger(ctx).Info("storage closed")
	}

	return firstErr
}

// Reset discards all ingested data, returning every tenant engine to empty (head and
// flushed parts) without reconstructing the [Storage]. It is intended for tests and
// benchmarks that reuse one store across runs instead of rebuilding it. Reset is only valid
// on an ephemeral (in-memory) backend; on a durable backend it returns [ErrNotEphemeral]
// rather than wipe persisted data. The tenant engines themselves are retained (emptied, not
// dropped), so subsequent writes reuse them.
func (s *Storage) Reset(ctx context.Context) error {
	if s.closed.Load() {
		return errors.Wrap(ErrClosed, "reset")
	}

	if !s.backend.IsEphemeral() {
		return ErrNotEphemeral
	}

	for _, eng := range s.engineSnapshot() {
		if err := eng.Reset(ctx); err != nil {
			return errors.Wrap(err, "reset engine")
		}
	}

	for _, eng := range s.logEngineSnapshot() {
		if err := eng.Reset(ctx); err != nil {
			return errors.Wrap(err, "reset log engine")
		}
	}

	for _, eng := range s.traceEngineSnapshot() {
		if err := eng.Reset(ctx); err != nil {
			return errors.Wrap(err, "reset trace engine")
		}
	}

	for _, eng := range s.profileEngineSnapshot() {
		if err := eng.Reset(ctx); err != nil {
			return errors.Wrap(err, "reset profile engine")
		}
	}

	return nil
}

// WriteMetrics ingests a metrics batch. It projects the internal model, derives each
// point's tenant from its Resource+Scope, and appends to that tenant's engine (indexing
// labels + buffering samples). Returns per-OTLP partial-success counts: rejected counts
// out-of-order drops. (Unsupported point kinds and value-less points never reach here:
// they are filtered and counted by the producer — e.g. the otlp/pdataconv bridge.)
func (s *Storage) WriteMetrics(ctx context.Context, md metric.Metrics) (acc Accepted, err error) {
	ctx, finish := s.writeSpan(ctx, "storage.write.metrics")
	defer finish(&acc, &err)

	if s.closed.Load() {
		return Accepted{}, errors.Wrap(ErrClosed, "write metrics")
	}

	if s.cluster != nil {
		return s.writeMetricsClustered(ctx, md)
	}

	var (
		rej            rejectTally
		sampledDropped int64
		overflowed     int64
	)

	// The tenant (hence the engine) is derived from Resource+Scope, which are constant within
	// a metric, so points arrive in tenant-contiguous runs. Cache the last resolution to skip
	// the locked engine-map lookup and policy resolve per metric.
	var (
		lastTenant   signal.TenantID
		lastEng      *engine.Engine
		lastAdmit    *tenantAdmission
		lastLimits   tenant.Limits
		lastSampling int64
	)

	var firstErr error

	emitted := metric.Project(md, func(b *metric.Batch) {
		if firstErr != nil {
			return
		}

		tid := s.tenantFor(b.Resource(), b.Scope())
		if lastEng == nil || tid != lastTenant {
			eng, err := s.engineFor(tid)
			if err != nil {
				firstErr = err

				return
			}

			pol := s.tenant.Resolve(s.normalizeTenant(tid))
			lastTenant, lastEng = tid, eng
			lastAdmit = s.admissionFor(tid)
			lastLimits = pol.Limits
			lastSampling = pol.Sampling.MaxRowsPerSecond
		}

		// Admission stage (between tenant resolution and the engine, DESIGN §8a): the ingest-rate
		// valve sheds a whole over-budget batch up front; cardinality and in-flight-memory limits
		// are enforced per sample inside the engine (race-free under its lock).
		if !lastAdmit.allowRate(lastLimits, int64(b.Len())*engine.SampleBytes, s.now()) {
			rej.rate += int64(b.Len())
			lastAdmit.addRate(int64(b.Len()))

			return
		}

		// Budgeted (lossy) sampling: keep a representative subset and weight each kept sample with a
		// scale factor so the embedder's count/sum/rate stays unbiased. Under budget this window the
		// sampler returns no weights and the batch passes through unchanged (no scale-factor column).
		ids, tss, vals, sf := b.IDs, b.Ts, b.Values, []float64(nil)
		mat := b.Series

		if weights, active := lastAdmit.sample(lastSampling, s.now(), b.IDs, b.Ts); active {
			fids := make([]signal.SeriesID, 0, len(weights))
			fts := make([]int64, 0, len(weights))
			fvals := make([]float64, 0, len(weights))
			fsf := make([]float64, 0, len(weights))
			kept := make([]int, 0, len(weights))

			var dropped int64

			for i, w := range weights {
				if w == 0 {
					dropped++

					continue
				}

				fids = append(fids, b.IDs[i])
				fts = append(fts, b.Ts[i])
				fvals = append(fvals, b.Values[i])
				fsf = append(fsf, w)
				kept = append(kept, i)
			}

			ids, tss, vals, sf = fids, fts, fvals, fsf
			mat = func(j int) signal.Series { return b.Series(kept[j]) }
			lastAdmit.recordSampledDropped(dropped)
			sampledDropped += dropped
		}

		res, err := lastEng.AppendBatch(ids, tss, vals, sf, mat, engine.AppendLimits{
			MaxSeries:        lastLimits.MaxSeries,
			MaxSeriesSoft:    lastLimits.MaxSeriesSoft,
			Overflow:         metricOverflow,
			MaxInFlightBytes: lastLimits.MaxInFlightBytes,
		})
		if err != nil {
			firstErr = err

			return
		}

		rej.ooo += int64(res.RejectedOOO)
		rej.cardinality += int64(res.RejectedCardinality)
		rej.inflight += int64(res.RejectedBytes)
		lastAdmit.record(int64(res.Accepted), int64(res.RejectedOOO), int64(res.RejectedCardinality), int64(res.RejectedBytes))

		if res.Overflowed > 0 {
			overflowed += int64(res.Overflowed)
			lastAdmit.recordOverflowed(int64(res.Overflowed))
		}
	})

	if firstErr != nil {
		return Accepted{}, firstErr
	}

	total := rej.total()
	accepted := int64(emitted) - total
	s.emitAdmission(ctx, signal.Metric, accepted, rej, sampledDropped, overflowed)

	return Accepted{
		Accepted:       accepted,
		Rejected:       total,
		RejectedReason: rej.reason(),
	}, nil
}

// Machine-readable admission rejection reasons, shared by the OTLP partial-success reason
// ([rejectTally.reason]) and the admission meta-metrics ([Storage.emitAdmission]).
const (
	reasonOutOfOrder       = "out_of_order"
	reasonRateLimit        = "rate_limit"
	reasonMaxSeries        = "max_series"
	reasonMaxInFlightBytes = "max_in_flight_bytes"
)

// defaultMaxPartBytes is the per-part size cap applied when a tenant's policy leaves MaxPartSize
// unset (0). It bounds a part's approximate uncompressed size (real parts compress well below it) so
// size-tiered compaction can seal the top tier, keeping a continuously-ingesting tenant's merge
// working set bounded instead of growing with the dataset (issue 22). An embedder that wants a
// different cap — or the unbounded single-part behavior — sets tenant.Limits.MaxPartSize explicitly.
const defaultMaxPartBytes = 64 << 20 // 64 MiB

// partSizeOrDefault returns the configured per-part cap, or defaultMaxPartBytes when unset. A
// negative value is treated as unset.
func partSizeOrDefault(maxPartSize int64) int64 {
	if maxPartSize <= 0 {
		return defaultMaxPartBytes
	}

	return maxPartSize
}

// metricOverflowLabel marks the synthetic series that absorbs a tenant's metric series past the
// soft cardinality budget (Track 3a).
var metricOverflowLabel = []byte("__overflow__")

// metricOverflow builds the overflow-series identity for a metric series that crossed the soft
// cardinality budget: it keeps only the metric name and adds __overflow__="true", dropping every
// other label (and resource/scope), so all overflowing series of a metric collapse into one bucket.
// That bounds cardinality to ~one extra series per metric name while keeping the metric's sum/count
// approximately right; the embedder treats __overflow__ like any label. It is invoked only on the
// degraded path (a new series past the soft line), so it never costs the steady-state hot path.
func metricOverflow(s signal.Series) signal.Series {
	name, _ := s.Attributes.Get(metric.LabelName)

	return signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: metric.LabelName, Value: name},
		signal.KeyValue{Key: metricOverflowLabel, Value: signal.StringValue([]byte("true"))},
	)}
}

// rejectTally accumulates per-reason rejection counts during a write and renders the dominant
// OTLP partial-success reason.
type rejectTally struct {
	ooo         int64
	rate        int64
	cardinality int64
	inflight    int64
}

func (r rejectTally) total() int64 { return r.ooo + r.rate + r.cardinality + r.inflight }

// reason returns a machine-readable reason for the rejections. When several reasons fired it
// reports the largest contributor (suffixed to signal it was not the only one), so a producer
// sees the principal valve without losing the fact that others tripped.
func (r rejectTally) reason() string {
	type kv struct {
		name string
		n    int64
	}

	all := []kv{
		{reasonOutOfOrder, r.ooo},
		{reasonRateLimit, r.rate},
		{reasonMaxSeries, r.cardinality},
		{reasonMaxInFlightBytes, r.inflight},
	}

	var top kv

	var nonzero int

	for _, c := range all {
		if c.n > 0 {
			nonzero++
		}

		if c.n > top.n {
			top = c
		}
	}

	switch nonzero {
	case 0:
		return ""
	case 1:
		return top.name
	default:
		return top.name + "+others"
	}
}

// Fetcher returns the read seam — a [fetch.Fetcher] over the named tenants' data (head ∪
// flushed parts). It is the storage library's query surface: this is a language-agnostic
// columnar store, so the embedder drives its own query engines (PromQL/LogQL/TraceQL) over
// the fetch contract. The optional query/promql package bridges this to the Prometheus
// storage.Queryable for embedders using the Prometheus engine.
//
// Tenant scoping:
//   - Fetcher(t) — one tenant.
//   - Fetcher(a, b, …) — a fan-out over several tenants (multi-tenant query): results are
//     merged by series id, so a series with equal labels in more than one tenant is federated
//     into one (see [fetch.Merge]). Add a tenant label upstream if per-tenant separation is
//     wanted.
//   - Fetcher() — all tenants that have ingested data (a cross-tenant query).
//
// The returned Fetcher is always usable: with no matching tenant (or after [Close]) it is an
// empty fetcher that yields no series, so callers need not special-case "no data yet".
//
// Scale-out: when [Options.QuerySplitInterval] and/or [Options.QueryCacheEntries] are set, the
// returned fetcher is wrapped with split-by-interval and/or a results cache (the query/scale
// decorators). The cache keys on the explicit tenant set, so it applies only to named-tenant
// queries — a no-arg cross-tenant query is never cached (its tenant membership is dynamic).
func (s *Storage) Fetcher(tenants ...signal.TenantID) fetch.Fetcher {
	if s.closed.Load() {
		return fetch.Merge() // empty
	}

	return seedFetcher{inner: s.scaleWrap(s.baseFetcher(tenants), tenants), obs: s.obs, signal: signal.Metric.String()}
}

// AggregateMetrics returns a per-series aggregate (count/sum/min/max — enough for avg) of one
// tenant's metric series over the request window. An embedder's PromQL engine can use it to
// evaluate `*_over_time` cheaply.
//
// Single-node it is the storage-side **pushdown**: with [WithAggregateStats] a range-covering
// aggregate is answered from precomputed per-part stats without decoding the value column (one row
// per series instead of every sample), else by decoding. In **cluster** mode it gathers across the
// tenant's shards through the owner-aware read fan-out and folds at the coordinator — correct across
// all shards, though it transfers raw samples rather than pushing the aggregate to each node (a
// compact per-node aggregate RPC is a planned optimization).
func (s *Storage) AggregateMetrics(ctx context.Context, t signal.TenantID, r fetch.Request) (map[signal.SeriesID]engine.SeriesAgg, error) {
	if s.closed.Load() {
		return map[signal.SeriesID]engine.SeriesAgg{}, nil
	}

	if s.cluster != nil {
		bucketed, err := s.clusterAggregateFor(ctx, t, r, 0) // step 0 ⇒ one whole-range bucket per series
		if err != nil {
			return nil, err
		}

		out := make(map[signal.SeriesID]engine.SeriesAgg, len(bucketed))
		for id, buckets := range bucketed {
			if len(buckets) > 0 {
				out[id] = buckets[0].SeriesAgg
			}
		}

		return out, nil
	}

	eng, ok := s.lookupEngine(s.normalizeTenant(t))
	if !ok {
		return map[signal.SeriesID]engine.SeriesAgg{}, nil
	}

	return eng.AggregateRange(ctx, r)
}

// SeriesAggregate pairs a series identity with its whole-range [engine.SeriesAgg]: the labeled
// form of [Storage.AggregateMetrics]. It is computed from the per-part stats sidecar where the plan
// is pushdown-safe, so a range-covering aggregate costs ~one row per series rather than a decode of
// every sample — and because the identity rides along, an embedder can render the result as a
// PromQL vector (labels + value) without a second, value-decoding fetch.
type SeriesAggregate struct {
	engine.SeriesAgg

	Series signal.Series
}

// AggregateMetricsNamed is the labeled form of [Storage.AggregateMetrics]: it returns each matching
// series' identity alongside its whole-range aggregate (count/sum/min/max — enough for avg). Use it
// when the caller needs to render the result with labels (e.g. an embedder's PromQL
// `*_over_time` pushdown); use [Storage.AggregateMetrics] when only the aggregate is needed. Series
// with no sample in the window are omitted.
func (s *Storage) AggregateMetricsNamed(ctx context.Context, t signal.TenantID, r fetch.Request) ([]SeriesAggregate, error) {
	if s.closed.Load() {
		return nil, nil
	}

	var named []engine.NamedAgg
	if s.cluster != nil {
		got, err := s.clusterAggregateNamedFor(ctx, t, r, 0) // step 0 ⇒ one whole-range bucket per series
		if err != nil {
			return nil, err
		}

		named = got
	} else {
		eng, ok := s.lookupEngine(s.normalizeTenant(t))
		if !ok {
			return nil, nil
		}

		got, err := eng.AggregateStepNamed(ctx, r, 0)
		if err != nil {
			return nil, err
		}

		named = got
	}

	out := make([]SeriesAggregate, 0, len(named))
	for i := range named {
		na := &named[i]
		if len(na.Buckets) == 0 { // SeriesAgg.Count == 0; omitted upstream, defended anyway.
			continue
		}

		out = append(out, SeriesAggregate{Series: na.Series, SeriesAgg: na.Buckets[0].SeriesAgg})
	}

	return out, nil
}

// seedFetcher is the outermost read wrapper: it installs the injected logger as the zctx base so
// every downstream fetcher (fan-out, remote, engine) can log a trace-correlated line, and emits one
// Debug at the query boundary. It does not touch the request.
type seedFetcher struct {
	inner  fetch.Fetcher
	obs    *obs.Obs
	signal string
}

func (f seedFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	ctx = f.obs.Base(ctx)
	f.obs.Logger(ctx).Debug("query fetch",
		zap.String("signal", f.signal), zap.Int("matchers", len(r.Matchers)),
		zap.Int64("start", r.Start), zap.Int64("end", r.End))

	return f.inner.Fetch(ctx, r)
}

// Unwrap exposes the decorated fetcher so [fetch.CounterOf] can reach the engine's Count through
// the seed layer (which only adds observability) for the count() pushdown.
func (f seedFetcher) Unwrap() fetch.Fetcher { return f.inner }

// baseFetcher builds the unwrapped read seam for the tenant set: owner-aware per tenant in
// cluster mode, otherwise the local engines (or a cross-tenant snapshot when none are named).
func (s *Storage) baseFetcher(tenants []signal.TenantID) fetch.Fetcher {
	// In cluster mode a named tenant is served owner-aware (local if owned, else fanned out to
	// an owner). Without named tenants we fall back to a local cross-tenant snapshot.
	if s.cluster != nil && len(tenants) > 0 {
		fetchers := make([]fetch.Fetcher, 0, len(tenants))
		for _, t := range tenants {
			fetchers = append(fetchers, s.clusterFetcherFor(t))
		}

		return fetch.Merge(fetchers...)
	}

	var fetchers []fetch.Fetcher

	if len(tenants) == 0 {
		for _, eng := range s.engineSnapshot() {
			fetchers = append(fetchers, eng)
		}
	} else {
		for _, t := range tenants {
			if e, ok := s.lookupEngine(s.normalizeTenant(t)); ok {
				fetchers = append(fetchers, e)
			}
		}
	}

	return fetch.Merge(fetchers...)
}

// scaleWrap decorates the base fetcher with the configured query/scale layers. The cache (when
// enabled and the query names an explicit tenant set) sits inside a scope-stamping wrapper so
// its keys carry the tenant set and never collide across scopes; split-by-interval (when
// enabled) wraps the outside, so each aligned sub-window is cached independently.
func (s *Storage) scaleWrap(f fetch.Fetcher, tenants []signal.TenantID) fetch.Fetcher {
	if s.queryCache != nil && len(tenants) > 0 {
		cached := scale.CacheFetcher{Inner: f, Cache: s.queryCache, Freshness: s.opts.QueryCacheFreshness}
		f = scopedFetcher{inner: cached, scope: s.tenantScope(tenants)}
	}

	if s.opts.QuerySplitInterval > 0 {
		f = scale.SplitFetcher{Inner: f, Interval: s.opts.QuerySplitInterval}
	}

	return f
}

// tenantScope is a stable token identifying a tenant set, used to namespace cache entries so a
// query over {a,b} never reads a cached result for {c}. Order-independent (sorted) so the token
// is the same regardless of argument order.
func (s *Storage) tenantScope(tenants []signal.TenantID) signal.TenantID {
	norm := make([]string, len(tenants))
	for i, t := range tenants {
		norm[i] = string(s.normalizeTenant(t))
	}

	sort.Strings(norm)

	return signal.TenantID(strings.Join(norm, "\x00"))
}

// scopedFetcher stamps a stable tenant-scope token onto each request before delegating, so a
// downstream [scale.CacheFetcher] keys on the tenant set the fetcher was built for (the merge
// children ignore Request.Tenant, so overwriting it does not affect the actual fetch).
type scopedFetcher struct {
	inner fetch.Fetcher
	scope signal.TenantID
}

func (f scopedFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	r.Tenant = f.scope

	return f.inner.Fetch(ctx, r)
}

// Unwrap exposes the decorated fetcher so [fetch.CounterOf] can reach the engine's Count through
// the tenant-scoping layer for the count() pushdown.
func (f scopedFetcher) Unwrap() fetch.Fetcher { return f.inner }

// lookupEngine returns the tenant's engine if it exists, without creating one (reads must not
// materialize empty engines for unknown tenants).
func (s *Storage) lookupEngine(tid signal.TenantID) (*engine.Engine, bool) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e, ok := s.tenants[tid]

	return e, ok
}

// tenantFor derives a record's tenant from its resource and scope via the configured
// callback, defaulting to "default".
func (s *Storage) tenantFor(r signal.Resource, sc signal.Scope) signal.TenantID {
	if s.opts.Tenant != nil {
		if tid := s.opts.Tenant(r, sc); tid != "" {
			return tid
		}
	}

	return "default"
}

func (s *Storage) normalizeTenant(t signal.TenantID) signal.TenantID {
	if t == "" {
		return "default"
	}

	return t
}

// metricsPrefix is the per-tenant key prefix under which a tenant's metric parts and indexes
// live. It must match the prefix engineFor builds.
const metricsPrefix = "/metrics"

// recover reconstructs each tenant's engine from a durable backend — the flushed parts and
// the identity index (the object-store-native, stateless read path) — so a fresh process
// serves data written by a previous one. It is a no-op for an ephemeral backend (a new
// process starts with an empty store). Tenants are discovered by their bucket-index objects.
//
// It then replays each per-tenant WAL ([Storage.recoverWAL]) to restore the *unflushed* head a
// crash would otherwise lose.
func (s *Storage) recover(ctx context.Context) error {
	if s.backend.IsEphemeral() {
		return nil
	}

	ctx = s.obs.Base(ctx)
	log := s.obs.Logger(ctx)
	log.Debug("recovery: scanning backend for flushed parts")

	keys, err := s.backend.List(ctx, "")
	if err != nil {
		return errors.Wrap(err, "list backend for recovery")
	}

	// load creates an engine (propagating a creation error) and loads its flushed parts.
	load := func(e partLoader, err error) error {
		if err != nil {
			return err
		}

		return e.LoadParts(ctx)
	}

	metricSuffix := metricsPrefix + "/" + bucketindex.Object
	logSuffix := logsPrefix + "/" + bucketindex.Object
	traceSuffix := tracesPrefix + "/" + bucketindex.Object
	profileSuffix := profilesPrefix + "/" + bucketindex.Object

	for _, k := range keys {
		switch {
		case strings.HasSuffix(k, metricSuffix):
			tid := signal.TenantID(strings.TrimSuffix(k, metricSuffix))
			if err := load(s.engineFor(tid)); err != nil {
				return errors.Wrapf(err, "recover metrics tenant %q", tid)
			}
		case strings.HasSuffix(k, logSuffix):
			tid := signal.TenantID(strings.TrimSuffix(k, logSuffix))
			if err := load(s.logEngineFor(tid)); err != nil {
				return errors.Wrapf(err, "recover logs tenant %q", tid)
			}
		case strings.HasSuffix(k, traceSuffix):
			tid := signal.TenantID(strings.TrimSuffix(k, traceSuffix))
			if err := load(s.traceEngineFor(tid)); err != nil {
				return errors.Wrapf(err, "recover traces tenant %q", tid)
			}
		case strings.HasSuffix(k, profileSuffix):
			tid := signal.TenantID(strings.TrimSuffix(k, profileSuffix))
			if err := load(s.profileEngineFor(tid)); err != nil {
				return errors.Wrapf(err, "recover profiles tenant %q", tid)
			}
		}
	}

	log.Debug("recovery: parts loaded, replaying WAL", zap.Int("backend_keys", len(keys)))

	return s.recoverWAL(ctx)
}

// partLoader is the LoadParts surface shared by both engine types, so recovery can load a freshly
// created engine's flushed parts uniformly.
type partLoader interface {
	LoadParts(ctx context.Context) error
}

// recoverWAL replays each per-tenant WAL directory under [Options.WALDir] into its engine, restoring
// the unflushed head a crash would otherwise lose. The directory layout mirrors the engine prefixes
// ({WALDir}/{tenant}/{signal}); creating the engine (via the *EngineFor constructors) re-attaches a
// fresh resumed WAL before replay. No-op when no WAL directory is configured or present.
func (s *Storage) recoverWAL(ctx context.Context) error {
	if s.opts.WALDir == "" {
		return nil
	}

	if _, err := os.Stat(s.opts.WALDir); os.IsNotExist(err) {
		return nil
	}

	return filepath.WalkDir(s.opts.WALDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			return nil
		}

		replay, ok := s.walReplayerFor(d.Name())
		if !ok {
			return nil // a tenant dir or the root, not a signal leaf
		}

		switch has, herr := dirHasSegments(path); {
		case herr != nil:
			return herr
		case !has:
			return nil
		}

		rel, err := filepath.Rel(s.opts.WALDir, filepath.Dir(path))
		if err != nil {
			return err
		}

		return replay(ctx, signal.TenantID(filepath.ToSlash(rel)), path)
	})
}

// walReplayerFor maps a WAL leaf-directory name (a signal) to a function that creates that signal's
// engine for the tenant and replays the directory into it.
func (s *Storage) walReplayerFor(name string) (func(context.Context, signal.TenantID, string) error, bool) {
	switch "/" + name {
	case metricsPrefix:
		return func(_ context.Context, tid signal.TenantID, dir string) error {
			e, err := s.engineFor(tid)
			if err != nil {
				return err
			}

			return e.Replay(dir)
		}, true
	case logsPrefix:
		return func(_ context.Context, tid signal.TenantID, dir string) error {
			e, err := s.logEngineFor(tid)
			if err != nil {
				return err
			}

			return e.Replay(dir)
		}, true
	case tracesPrefix:
		return func(_ context.Context, tid signal.TenantID, dir string) error {
			e, err := s.traceEngineFor(tid)
			if err != nil {
				return err
			}

			return e.Replay(dir)
		}, true
	case profilesPrefix:
		return func(_ context.Context, tid signal.TenantID, dir string) error {
			e, err := s.profileEngineFor(tid)
			if err != nil {
				return err
			}

			return e.Replay(dir)
		}, true
	default:
		return nil, false
	}
}

// dirHasSegments reports whether dir contains any WAL segment file.
func dirHasSegments(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}

	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
			return true, nil
		}
	}

	return false, nil
}

// engineFor returns the engine for a tenant, creating it (with a WAL when [Options.WALDir] is set)
// on first use.
func (s *Storage) engineFor(tid signal.TenantID) (*engine.Engine, error) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	if e := s.tenants[tid]; e != nil {
		return e, nil
	}

	prefix := string(s.normalizeTenant(tid)) + metricsPrefix

	w, err := s.walFor(prefix)
	if err != nil {
		return nil, err
	}

	e := engine.New(engine.Config{
		OOOWindow:        s.opts.OOOWindow,
		Backend:          s.backend,
		Prefix:           prefix,
		WAL:              w,
		Obs:              s.obs,
		DecodeCacheBytes: s.opts.DecodeCacheBytes,
		// One budget for every tenant engine, so the decode-admission cap bounds the process-wide
		// in-flight decoded bytes instead of multiplying per tenant. nil ⇒ unlimited.
		DecodeBudget:   s.decodeBudget,
		AggregateStats: s.opts.AggregateStats,
		// MaxPartBytes caps each flushed/merged part; resolved from the tenant's policy, falling back
		// to defaultMaxPartBytes when the policy leaves it unset. It is an operational/structural cap
		// fixed at engine creation (unlike the per-write admission limits). A bounded part size is what
		// lets size-tiered compaction seal the top tier — without it a continuously-ingesting tenant's
		// merge would grow to materialize the whole dataset (see github.com/oteldb/storage issue 22).
		MaxPartBytes: partSizeOrDefault(s.tenant.Resolve(s.normalizeTenant(tenantOfShard(tid))).Limits.MaxPartSize),
	})
	s.tenants[tid] = e

	return e, nil
}

// walFor returns a per-engine durable WAL writer rooted at [Options.WALDir]/prefix, or a nil writer
// when no WAL directory is configured (the engine then runs without durable head logging — flushed
// parts are still recovered from the backend). The directory is created on first use and resumed on
// restart (see [wal.Create]).
//
//nolint:nilnil // a nil writer is the documented "no WAL configured" sentinel, checked by the engine
func (s *Storage) walFor(prefix string) (*wal.SegmentWriter, error) {
	if s.opts.WALDir == "" || s.backend.IsEphemeral() {
		return nil, nil
	}

	w, err := wal.Create(filepath.Join(s.opts.WALDir, filepath.FromSlash(prefix)), 0)
	if err != nil {
		return nil, errors.Wrapf(err, "create wal for %q", prefix)
	}

	if s.opts.WALSync == WALSyncAlways {
		w.SetSync(true)
	}

	if s.opts.MeterProvider != nil {
		w.SetObs(s.obs.WAL)
	}

	w.SetLogger(s.obs.Log)

	return w, nil
}

// runMaintenance periodically flushes and compacts every tenant engine until Close stops
// it. It is the single background loop driving flush (age) and merge+retention.
func (s *Storage) runMaintenance(interval time.Duration) {
	defer s.wg.Done()

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			// The loop owns its lifetime (stopped via stopCh, not a request), so a
			// background context is correct here.
			s.maintain(context.Background())
		}
	}
}

// runWALSync periodically fsyncs every engine's WAL until Close stops it ([WALSyncInterval] mode).
func (s *Storage) runWALSync(interval time.Duration) {
	defer s.wg.Done()

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			s.syncWALs()
		}
	}
}

// syncWALs fsyncs every tenant engine's WAL across all signals (background interval sync). Errors are
// swallowed; the next tick retries.
func (s *Storage) syncWALs() {
	// Each engine's WAL is an independent file; fsyncs across engines parallelize. Gather them as a
	// single list and fan out under the same bound as maintenance.
	metrics := s.engineSnapshot()
	logs := s.logEngineSnapshot()
	traces := s.traceEngineSnapshot()
	profiles := s.profileEngineSnapshot()

	wals := make([]walSyncer, 0, len(metrics)+len(logs)+len(traces)+len(profiles))
	for _, e := range metrics {
		wals = append(wals, e)
	}

	for _, e := range logs {
		wals = append(wals, e)
	}

	for _, e := range traces {
		wals = append(wals, e)
	}

	for _, e := range profiles {
		wals = append(wals, e)
	}

	parallel.ForEach(len(wals), s.maintenanceConcurrency(), func(i int) { _ = wals[i].SyncWAL() })
}

// walSyncer is the subset of an engine the WAL-sync loop needs: both the metric engine and the
// record engine expose SyncWAL, so the loop fans out over them uniformly.
type walSyncer interface{ SyncWAL() error }

// maintain flushes then merges (with retention) every tenant engine once. Errors are
// swallowed: a transient backend failure must not crash the background loop, and the next
// maintTask is one engine's maintenance work plus its flush pressure (head bytes), used to order
// the cycle so the fullest heads flush first (fair scheduling — see maintain).
type maintTask struct {
	run      func()
	pressure int64
}

// tick retries. In cluster mode only the tenants this node owns (its compaction claims) are
// flushed/merged, so exactly one node writes a tenant's parts to the shared object store.
func (s *Storage) maintain(ctx context.Context) {
	ctx = s.obs.Base(ctx)

	metricEngines := s.engineSnapshotByTenant()
	logEngines := s.logEngineSnapshotByTenant()
	traceEngines := s.traceEngineSnapshotByTenant()
	profileEngines := s.profileEngineSnapshotByTenant()

	s.obs.Logger(ctx).Debug("maintenance cycle start",
		zap.Int("metric_tenants", len(metricEngines)), zap.Int("log_tenants", len(logEngines)),
		zap.Int("trace_tenants", len(traceEngines)), zap.Int("profile_tenants", len(profileEngines)))

	// Compaction ownership is per-tenant and shared across signals, so reconcile it once over the
	// union of all signals' tenants (reconciling per-signal would have each release the others'
	// claims). nil ⇒ single-node: own everything.
	tids := make(map[signal.TenantID]struct{}, len(metricEngines)+len(logEngines)+len(traceEngines)+len(profileEngines))
	for tid := range metricEngines {
		tids[tid] = struct{}{}
	}

	for tid := range logEngines {
		tids[tid] = struct{}{}
	}

	for tid := range traceEngines {
		tids[tid] = struct{}{}
	}

	for tid := range profileEngines {
		tids[tid] = struct{}{}
	}

	owned := s.ownedTenants(ctx, tids)

	// maintainEngine flushes then merges one engine, unless this node is a non-owning replica (then
	// it only refreshes from the shared store). merge is signal-specific: metrics carry downsampling
	// (engine.MergeWith), the record signals carry retention only.
	//
	// With a private (per-node) backend, flushed parts are not visible through a shared store, so
	// the replica first mirrors the owner's backend objects (partsync) and then refreshes from its
	// own backend; the owner backfills strictly-newer peer parts before compacting, so a
	// newly-gained owner never restarts a shard's part sequence from scratch.
	maintainEngine := func(tid signal.TenantID, signalPrefix string, flush, merge, refresh func() error) {
		if owned != nil {
			if _, ok := owned[tid]; !ok {
				// A replica, not the compaction owner: pull the owner's flushed parts and trim the
				// head to the unflushed window, bounding memory.
				s.syncParts(ctx, tid, signalPrefix, false)
				_ = refresh()

				return
			}
		}

		if s.syncParts(ctx, tid, signalPrefix, true) {
			// Backfilled parts from a peer (this node just gained the shard): reload them before
			// flushing so the part sequence advances past the synced parts.
			_ = refresh()
		}

		_ = flush()
		_ = merge()
	}

	// Each engine is an independent shard (per tenant, per signal) with its own lock, so flush/merge
	// across engines parallelizes freely. Build one flat work list and fan out under a bound — a
	// sequential pass would take the sum of every engine's compaction I/O.
	//
	// Fair scheduling: the work list is ordered by flush pressure (head bytes) descending, and
	// parallel.ForEach dispatches in index order, so the fullest heads flush first within a cycle.
	// This keeps one noisy tenant from delaying the relief of others when the work exceeds the
	// concurrency bound, and drains the most in-flight memory soonest.
	tasks := make([]maintTask, 0, len(metricEngines)+len(logEngines)+len(traceEngines)+len(profileEngines))

	for tid, eng := range metricEngines {
		tasks = append(tasks, maintTask{pressure: eng.HeadBytes(), run: func() {
			maintainEngine(tid, metricsPrefix, func() error { return eng.Flush(ctx) },
				func() error { return eng.MergeWith(ctx, s.metricMergeOptions(tid)) },
				func() error { return eng.RefreshReplica(ctx) })
		}})
	}

	addRecord := func(engines map[signal.TenantID]*recordengine.Engine, signalPrefix string) {
		for tid, eng := range engines {
			tasks = append(tasks, maintTask{pressure: eng.HeadBytes(), run: func() {
				maintainEngine(tid, signalPrefix, func() error { return eng.Flush(ctx) },
					func() error { return eng.Merge(ctx, s.retainFrom(tid)) },
					func() error { return eng.RefreshReplica(ctx) })
			}})
		}
	}

	addRecord(logEngines, logsPrefix)
	addRecord(traceEngines, tracesPrefix)
	addRecord(profileEngines, profilesPrefix)

	sort.Slice(tasks, func(i, j int) bool { return tasks[i].pressure > tasks[j].pressure })

	parallel.ForEach(len(tasks), s.maintenanceConcurrency(), func(i int) { tasks[i].run() })
}

// maintenanceConcurrency is the parallel flush/merge/fsync fan-out cap for the background loops,
// from [Options.MaintenanceConcurrency] or a CPU-derived default.
func (s *Storage) maintenanceConcurrency() int {
	if s.opts.MaintenanceConcurrency > 0 {
		return s.opts.MaintenanceConcurrency
	}

	return parallel.DefaultLimit()
}

// ownedTenants reconciles cluster compaction ownership for the given tenant ids and returns the
// set this node owns. It returns nil in single-node mode (every tenant is owned).
func (s *Storage) ownedTenants(ctx context.Context, tids map[signal.TenantID]struct{}) map[signal.TenantID]struct{} {
	if s.cluster == nil {
		return nil
	}

	shards := make([]string, 0, len(tids))
	for tid := range tids {
		shards = append(shards, string(s.normalizeTenant(tid)))
	}

	owned, err := s.cluster.ownership.Reconcile(ctx, s.cluster.membership.Ring(), shards)
	if err != nil {
		return map[signal.TenantID]struct{}{} // on error, own nothing this tick (retry next)
	}

	out := make(map[signal.TenantID]struct{}, len(owned))
	for _, shard := range owned {
		out[signal.TenantID(shard)] = struct{}{}
	}

	return out
}

// retainFrom converts a tenant's retention window into an absolute cutoff timestamp (unix
// nanoseconds); 0 means retain forever.
func (s *Storage) retainFrom(tid signal.TenantID) int64 {
	// tid may be a shard key ({tenant}/_s{idx}) when a signal is sharded; policy is per real tenant.
	return retentionCutoff(s.tenant.Resolve(s.normalizeTenant(tenantOfShard(tid))).Retention, time.Now().UnixNano())
}

// retentionCutoff converts a retention window into an absolute cutoff at the given now (unix
// nanoseconds); 0 means retain forever.
func retentionCutoff(r tenant.Retention, now int64) int64 {
	if r.MaxAge <= 0 {
		return 0
	}

	return now - r.MaxAge.Nanoseconds()
}

// metricMergeOptions resolves a metric tenant's policy into the absolute merge parameters —
// retention cutoff plus downsampling tiers — for one maintenance pass. Resolving against a single
// now keeps the whole merge deterministic (the engine reads no clock). Downsampling applies to
// metrics only (the record signals are append-only event data; rolling them up would destroy them).
func (s *Storage) metricMergeOptions(tid signal.TenantID) engine.MergeOptions {
	now := time.Now().UnixNano()
	// In cluster mode tid is a shard key ({tenant}/_s{idx}); policy is per real tenant.
	p := s.tenant.Resolve(s.normalizeTenant(tenantOfShard(tid)))

	var tiers []engine.DownsampleTier

	for _, t := range p.Downsample.Tiers {
		if t.Interval <= 0 {
			continue
		}

		tiers = append(tiers, engine.DownsampleTier{
			Before:   now - t.After.Nanoseconds(),
			Interval: t.Interval.Nanoseconds(),
			Agg:      t.Agg,
		})
	}

	var recompress *engine.RecompressSpec

	if p.Recompress.After > 0 {
		level := compress.LevelBest
		if p.Recompress.Level > 0 {
			level = compress.Level(p.Recompress.Level)
		}

		recompress = &engine.RecompressSpec{
			Before:    now - p.Recompress.After.Nanoseconds(),
			Algorithm: compress.AlgorithmZSTD,
			Level:     level,
		}
	}

	var precision []engine.PrecisionTier

	for _, t := range p.Precision.Tiers {
		if t.Bits == 0 || t.Bits >= 64 {
			continue
		}

		precision = append(precision, engine.PrecisionTier{
			Before: now - t.After.Nanoseconds(),
			Bits:   t.Bits,
		})
	}

	return engine.MergeOptions{
		RetainFrom: retentionCutoff(p.Retention, now),
		Downsample: tiers,
		Recompress: recompress,
		Precision:  precision,
	}
}

// engineSnapshot returns the current tenant engines (a copy, so callers iterate without
// holding the lock).
func (s *Storage) engineSnapshot() []*engine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make([]*engine.Engine, 0, len(s.tenants))
	for _, eng := range s.tenants {
		out = append(out, eng)
	}

	return out
}

// engineSnapshotByTenant is engineSnapshot keyed by tenant id (for policy lookup).
func (s *Storage) engineSnapshotByTenant() map[signal.TenantID]*engine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	out := make(map[signal.TenantID]*engine.Engine, len(s.tenants))
	maps.Copy(out, s.tenants)

	return out
}

// Accepted carries per-OTLP partial-success counts (DESIGN.md §5). It implements the
// OTLP partial-success semantics: accepted/rejected data points + a reason string.
type Accepted struct {
	// Accepted is the number of accepted data points (metric points, log records,
	// span, profile samples depending on signal).
	Accepted int64
	// Rejected is the number of rejected data points (e.g. over a tenant limit or
	// out of the OOO window). Rejections are reported back to the producer via OTLP
	// partial_success so it can retry.
	Rejected int64
	// RejectedReason is a machine-readable reason for rejections (empty if none).
	RejectedReason string
}

// The read path is the language-agnostic [Storage.Fetcher] seam, not a query-language method:
// this is a columnar storage library, and the embedder owns the query languages
// (PromQL/LogQL/TraceQL) — driving them over [fetch.Fetcher] (see the optional query/promql
// adapter). There is deliberately no Storage.Query / query-language type here.
