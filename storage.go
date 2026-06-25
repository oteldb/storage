package storage

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/tenant"
)

// Storage is the embeddable entry point (DESIGN.md §5). It is safe for concurrent use.
// Construct with [Open] or [InMemory]; never with a literal.
//
// All ingestion is push-based and OTLP-shaped: methods accept the OTel-Go pdata types
// and return an [Accepted] carrying per-OTLP partial-success counts. Queries compile
// to the fetch contract (DESIGN.md §7) regardless of [Query.Lang].
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

// WriteMetrics ingests OTLP metrics. It projects pdata to the internal model, derives
// each point's tenant from its Resource+Scope, and appends to that tenant's engine
// (indexing labels + buffering samples). Returns per-OTLP partial-success counts:
// rejected counts unsupported point kinds, value-less points, and out-of-order drops.
func (s *Storage) WriteMetrics(_ context.Context, md pmetric.Metrics) (Accepted, error) {
	if s.closed.Load() {
		return Accepted{}, errors.Wrap(ErrClosed, "write metrics")
	}

	var oooRejected int64

	emitted, projRejected := metric.Project(md, func(id metric.Identity, sample metric.Sample) {
		eng := s.engineFor(s.tenantFor(id.Series.Resource, id.Series.Scope))
		// Engines are ephemeral here, so Append never errors; a false result is an
		// out-of-order rejection.
		if ok, _ := eng.Append(id.ToSeries(), sample.Ts, sample.Value); !ok {
			oooRejected++
		}
	})

	return Accepted{
		Accepted: int64(emitted) - oooRejected,
		Rejected: int64(projRejected) + oooRejected,
	}, nil
}

// WriteLogs ingests OTLP logs. Later vertical.
func (s *Storage) WriteLogs(_ context.Context, _ plog.Logs) (Accepted, error) {
	return s.notImplementedWrite("write logs")
}

// WriteTraces ingests OTLP traces. Later vertical.
func (s *Storage) WriteTraces(_ context.Context, _ ptrace.Traces) (Accepted, error) {
	return s.notImplementedWrite("write traces")
}

// WriteProfiles ingests OTLP profiles. Later vertical.
func (s *Storage) WriteProfiles(_ context.Context, _ pprofile.Profiles) (Accepted, error) {
	return s.notImplementedWrite("write profiles")
}

// Query runs a query against one tenant. The engine selects the language by [Query.Lang].
// PromQL and the rest land at M4; for now the low-level read path is the fetch contract.
func (s *Storage) Query(_ context.Context, t signal.TenantID, _ Query) (Result, error) {
	if s.closed.Load() {
		return Result{}, errors.Wrap(ErrClosed, "query")
	}

	_ = s.tenant.Resolve(s.normalizeTenant(t)) // policy may gate query concurrency/limits

	return Result{}, ErrNotImplemented
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

// Query is a query against the store (DESIGN.md §5, §9).
type Query struct {
	// Lang selects the query language front-end (DESIGN.md §6). Required.
	Lang Lang
	// Tenant is the tenant scope; the top-level [Storage.Query] takes it separately.
	// Text is the query text in [Lang].
	Text string
	// Start, End is the time range in unix nanoseconds, inclusive.
	Start, End int64
	// Step is the evaluation step for range queries (PromQL `range query`), in
	// nanoseconds. Zero ⇒ an instant query.
	Step int64
	// TODO(M4): Timeout, Hints (sharding, sampling, limits), MaxSeries.
}

// Lang is the query language front-end (DESIGN.md §5, §6).
type Lang uint8

const (
	// LangPromQL is the PromQL language (must-have, DESIGN.md §14 M4).
	LangPromQL Lang = iota + 1
	// LangLogQL is the LogQL language (logs, later vertical).
	LangLogQL
	// LangTraceQL is the TraceQL language (traces, later vertical).
	LangTraceQL
	// LangGenericQL is the cross-signal language (deferred until ≥2 signals, §15).
	LangGenericQL
)

// String returns a stable lower-case language name.
func (l Lang) String() string {
	switch l {
	case LangPromQL:
		return "promql"
	case LangLogQL:
		return "logql"
	case LangTraceQL:
		return "traceql"
	case LangGenericQL:
		return "genericql"
	default:
		return "unknown"
	}
}

// Result is a query result (DESIGN.md §9). The concrete shape (matrix/vector/scalar
// for PromQL; log streams for LogQL; trace batches for TraceQL) is filled in at M4.
// It is a placeholder now so the [Storage.Query] signature is stable.
type Result struct {
	// TODO(M4): a streaming result iterator or typed value.
}

// ErrNotImplemented is returned by scaffold-stub methods whose end-to-end wiring
// lands in a later milestone. It is not a fatal error: embedders may compile against
// the surface and gate on [errors.Is](err, [ErrNotImplemented]).
var ErrNotImplemented = errors.New("storage: not implemented in this milestone")
