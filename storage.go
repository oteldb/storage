package storage

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/signal/profile"
	"github.com/oteldb/storage/signal/trace"
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
// This is a scaffold stub: ingestion and query are wired end-to-end at M3 (metrics
// vertical). The methods currently validate arguments and return
// [ErrNotImplemented] so the surface is stable for embedders to compile against.
type Storage struct {
	opts    Options
	backend backend.Backend
	tenant  tenant.Resolver
	closed  atomic.Bool

	tmu     sync.Mutex
	tenants map[signal.TenantID]*engine.Engine

	stopCh chan struct{}  // closed by Close to stop the maintenance loop
	wg     sync.WaitGroup // tracks the maintenance goroutine
}

// Open constructs a [Storage] from [Options] (DESIGN.md §5). If [Options.Backend] is
// nil it defaults to [backend.Memory]; if the backend is ephemeral and no durability
// is chosen, it defaults to [DurabilityEphemeral] (the in-memory engine).
func Open(_ context.Context, o Options, opts ...Option) (*Storage, error) {
	for _, opt := range opts {
		opt(&o)
	}
	if err := o.validate(); err != nil {
		return nil, err
	}
	o.applyDefaults()
	s := &Storage{
		opts:    o,
		backend: o.Backend,
		tenant:  o.Tenancy,
		tenants: make(map[signal.TenantID]*engine.Engine),
	}
	if s.tenant == nil {
		s.tenant = tenant.Default()
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

	for _, eng := range s.engineSnapshot() {
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

	return nil
}

// WriteMetrics ingests a metrics batch. It projects the internal model, derives each
// point's tenant from its Resource+Scope, and appends to that tenant's engine (indexing
// labels + buffering samples). Returns per-OTLP partial-success counts: rejected counts
// out-of-order drops. (Unsupported point kinds and value-less points never reach here:
// they are filtered and counted by the producer — e.g. the otlp/pdataconv bridge.)
func (s *Storage) WriteMetrics(_ context.Context, md metric.Metrics) (Accepted, error) {
	if s.closed.Load() {
		return Accepted{}, errors.Wrap(ErrClosed, "write metrics")
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

// WriteLogs ingests a logs batch. Later vertical.
func (s *Storage) WriteLogs(_ context.Context, _ log.Logs) (Accepted, error) {
	return s.notImplementedWrite("write logs")
}

// WriteTraces ingests a traces batch. Later vertical.
func (s *Storage) WriteTraces(_ context.Context, _ trace.Traces) (Accepted, error) {
	return s.notImplementedWrite("write traces")
}

// WriteProfiles ingests a profiles batch. Later vertical.
func (s *Storage) WriteProfiles(_ context.Context, _ profile.Profiles) (Accepted, error) {
	return s.notImplementedWrite("write profiles")
}

// Fetcher returns the read seam — a [fetch.Fetcher] over one tenant's data (head ∪ flushed
// parts). It is the storage library's query surface: this is a language-agnostic columnar
// store, so the embedder drives its own query engines (PromQL/LogQL/TraceQL) over the fetch
// contract. The optional query/promql package bridges this to the Prometheus
// storage.Queryable for embedders using the Prometheus engine.
//
// The returned Fetcher is always usable: for an unknown tenant (or after [Close]) it is an
// empty fetcher that yields no series, so callers need not special-case "no data yet".
func (s *Storage) Fetcher(t signal.TenantID) fetch.Fetcher {
	if s.closed.Load() {
		return emptyFetcher{}
	}

	if e, ok := s.lookupEngine(s.normalizeTenant(t)); ok {
		return e
	}

	return emptyFetcher{}
}

// lookupEngine returns the tenant's engine if it exists, without creating one (reads must not
// materialize empty engines for unknown tenants).
func (s *Storage) lookupEngine(tid signal.TenantID) (*engine.Engine, bool) {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e, ok := s.tenants[tid]

	return e, ok
}

// emptyFetcher is the [fetch.Fetcher] returned for a tenant with no data; it yields an empty
// iterator.
type emptyFetcher struct{}

func (emptyFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	return fetch.NewSliceIterator(nil), nil
}

func (s *Storage) notImplementedWrite(op string) (Accepted, error) {
	if s.closed.Load() {
		return Accepted{}, errors.Wrap(ErrClosed, op)
	}

	return Accepted{}, ErrNotImplemented
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

// engineFor returns the engine for a tenant, creating it on first use.
func (s *Storage) engineFor(tid signal.TenantID) *engine.Engine {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	e := s.tenants[tid]
	if e == nil {
		e = engine.New(engine.Config{
			OOOWindow: s.opts.OOOWindow,
			Backend:   s.backend,
			Prefix:    string(s.normalizeTenant(tid)) + "/metrics",
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
// tick retries.
func (s *Storage) maintain(ctx context.Context) {
	for tid, eng := range s.engineSnapshotByTenant() {
		_ = eng.Flush(ctx)
		_ = eng.Merge(ctx, s.retainFrom(tid))
	}
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

// ErrNotImplemented is returned by scaffold-stub methods whose end-to-end wiring
// lands in a later milestone. It is not a fatal error: embedders may compile against
// the surface and gate on [errors.Is](err, [ErrNotImplemented]).
var ErrNotImplemented = errors.New("storage: not implemented in this milestone")
