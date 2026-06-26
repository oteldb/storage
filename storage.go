package storage

import (
	"context"
	"maps"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/scale"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/tenant"
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
	}
	if s.tenant == nil {
		s.tenant = tenant.Default()
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

	if o.FlushInterval > 0 {
		s.stopCh = make(chan struct{})
		s.wg.Add(1)

		// The maintenance loop's context is created inside the goroutine and scoped to
		// its own lifetime (stopped via stopCh), not to this Open call.
		go s.runMaintenance(time.Duration(o.FlushInterval)) //nolint:gosec,contextcheck // G118: loop-scoped context, see runMaintenance
	}

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
func (s *Storage) WriteMetrics(ctx context.Context, md metric.Metrics) (Accepted, error) {
	if s.closed.Load() {
		return Accepted{}, errors.Wrap(ErrClosed, "write metrics")
	}

	if s.cluster != nil {
		return s.writeMetricsClustered(ctx, md)
	}

	var oooRejected int64

	// The tenant (hence the engine) is derived from Resource+Scope, which are constant within
	// a metric, so points arrive in tenant-contiguous runs. Cache the last resolution to skip
	// the locked engine-map lookup per metric.
	var (
		lastTenant signal.TenantID
		lastEng    *engine.Engine
	)

	emitted := metric.Project(md, func(b *metric.Batch) {
		tid := s.tenantFor(b.Resource(), b.Scope())
		if lastEng == nil || tid != lastTenant {
			lastTenant, lastEng = tid, s.engineFor(tid)
		}

		// Engines are ephemeral here, so AppendBatch never errors; samples beyond the OOO
		// window are not accepted and counted as rejected.
		accepted, _ := lastEng.AppendBatch(b.IDs, b.Ts, b.Values, b.Series)
		oooRejected += int64(b.Len() - accepted)
	})

	return Accepted{
		Accepted: int64(emitted) - oooRejected,
		Rejected: oooRejected,
	}, nil
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

	return s.scaleWrap(s.baseFetcher(tenants), tenants)
}

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
// Note: this restores only *flushed* data. Recovering the unflushed head from a per-tenant
// WAL is a separate piece (the WAL is not yet wired into the facade).
func (s *Storage) recover(ctx context.Context) error {
	if s.backend.IsEphemeral() {
		return nil
	}

	keys, err := s.backend.List(ctx, "")
	if err != nil {
		return errors.Wrap(err, "list backend for recovery")
	}

	metricSuffix := metricsPrefix + "/" + bucketindex.Object
	logSuffix := logsPrefix + "/" + bucketindex.Object
	traceSuffix := tracesPrefix + "/" + bucketindex.Object
	profileSuffix := profilesPrefix + "/" + bucketindex.Object

	for _, k := range keys {
		switch {
		case strings.HasSuffix(k, metricSuffix):
			tid := signal.TenantID(strings.TrimSuffix(k, metricSuffix))
			if err := s.engineFor(tid).LoadParts(ctx); err != nil {
				return errors.Wrapf(err, "recover metrics tenant %q", tid)
			}
		case strings.HasSuffix(k, logSuffix):
			tid := signal.TenantID(strings.TrimSuffix(k, logSuffix))
			if err := s.logEngineFor(tid).LoadParts(ctx); err != nil {
				return errors.Wrapf(err, "recover logs tenant %q", tid)
			}
		case strings.HasSuffix(k, traceSuffix):
			tid := signal.TenantID(strings.TrimSuffix(k, traceSuffix))
			if err := s.traceEngineFor(tid).LoadParts(ctx); err != nil {
				return errors.Wrapf(err, "recover traces tenant %q", tid)
			}
		case strings.HasSuffix(k, profileSuffix):
			tid := signal.TenantID(strings.TrimSuffix(k, profileSuffix))
			if err := s.profileEngineFor(tid).LoadParts(ctx); err != nil {
				return errors.Wrapf(err, "recover profiles tenant %q", tid)
			}
		}
	}

	return nil
}

// engineFor returns the engine for a tenant, creating it on first use.
func (s *Storage) engineFor(tid signal.TenantID) *engine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e := s.tenants[tid]
	if e == nil {
		e = engine.New(engine.Config{
			OOOWindow: s.opts.OOOWindow,
			Backend:   s.backend,
			Prefix:    string(s.normalizeTenant(tid)) + metricsPrefix,
		})
		s.tenants[tid] = e
	}

	return e
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

// maintain flushes then merges (with retention) every tenant engine once. Errors are
// swallowed: a transient backend failure must not crash the background loop, and the next
// tick retries. In cluster mode only the tenants this node owns (its compaction claims) are
// flushed/merged, so exactly one node writes a tenant's parts to the shared object store.
func (s *Storage) maintain(ctx context.Context) {
	metricEngines := s.engineSnapshotByTenant()
	logEngines := s.logEngineSnapshotByTenant()
	traceEngines := s.traceEngineSnapshotByTenant()
	profileEngines := s.profileEngineSnapshotByTenant()

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

	maintainEngine := func(tid signal.TenantID, flush func() error, merge func(int64) error, refresh func() error) {
		if owned != nil {
			if _, ok := owned[tid]; !ok {
				// A replica, not the compaction owner: pull the owner's flushed parts and trim the
				// head to the unflushed window, bounding memory.
				_ = refresh()

				return
			}
		}

		_ = flush()
		_ = merge(s.retainFrom(tid))
	}

	for tid, eng := range metricEngines {
		maintainEngine(tid, func() error { return eng.Flush(ctx) },
			func(r int64) error { return eng.Merge(ctx, r) }, func() error { return eng.RefreshReplica(ctx) })
	}

	for tid, eng := range logEngines {
		maintainEngine(tid, func() error { return eng.Flush(ctx) },
			func(r int64) error { return eng.Merge(ctx, r) }, func() error { return eng.RefreshReplica(ctx) })
	}

	for tid, eng := range traceEngines {
		maintainEngine(tid, func() error { return eng.Flush(ctx) },
			func(r int64) error { return eng.Merge(ctx, r) }, func() error { return eng.RefreshReplica(ctx) })
	}

	for tid, eng := range profileEngines {
		maintainEngine(tid, func() error { return eng.Flush(ctx) },
			func(r int64) error { return eng.Merge(ctx, r) }, func() error { return eng.RefreshReplica(ctx) })
	}
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
	maxAge := s.tenant.Resolve(s.normalizeTenant(tid)).Retention.MaxAge
	if maxAge <= 0 {
		return 0
	}

	return time.Now().UnixNano() - maxAge.Nanoseconds()
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
