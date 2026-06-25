package storage

import (
	"context"
	"sync/atomic"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
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
	}
	if s.tenant == nil {
		s.tenant = tenant.Default()
	}
	return s, nil
}

// InMemory returns a fully in-memory, ephemeral [Storage] (DESIGN.md §5): equivalent
// to [Open] with [backend.Memory] and [DurabilityEphemeral]. It is the default in
// tests — a full ingest+query path with no disk or object store.
func InMemory(opts ...Option) (*Storage, error) {
	all := []Option{
		WithBackend(backend.Memory()),
		WithDurability(DurabilityEphemeral),
	}
	all = append(all, opts...)
	return Open(context.Background(), Options{}, all...)
}

// Close releases all resources. It is idempotent. After [Close], every method on s
// returns [ErrClosed].
func (s *Storage) Close(_ context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	// TODO(M3): close engine, flusher, merger, WAL, backend.
	return nil
}

// write checks the closed flag and resolves the tenant policy before dispatching.
func (s *Storage) write(t signal.TenantID) (tenant.Policy, error) {
	if s.closed.Load() {
		return tenant.Policy{}, errors.Wrap(ErrClosed, "write")
	}
	if t == "" {
		t = "default"
	}
	return s.tenant.Resolve(t), nil
}

// WriteMetrics ingests OTLP metrics (DESIGN.md §5, §8). It projects pdata to internal
// columnar builders, interns symbols, computes SeriesIDs, appends to the WAL (per
// durability), and inserts into the in-memory head. Returns per-OTLP partial-success
// counts via [Accepted].
func (s *Storage) WriteMetrics(_ context.Context, t signal.TenantID, _ pmetric.Metrics) (Accepted, error) {
	if _, err := s.write(t); err != nil {
		return Accepted{}, err
	}
	// TODO(M3): project pdata → builders, intern, SeriesID, WAL, head.
	return Accepted{}, ErrNotImplemented
}

// WriteLogs ingests OTLP logs (DESIGN.md §5). Later vertical (M8+).
func (s *Storage) WriteLogs(_ context.Context, t signal.TenantID, _ plog.Logs) (Accepted, error) {
	if _, err := s.write(t); err != nil {
		return Accepted{}, err
	}
	return Accepted{}, ErrNotImplemented
}

// WriteTraces ingests OTLP traces (DESIGN.md §5). Later vertical (M8+).
func (s *Storage) WriteTraces(_ context.Context, t signal.TenantID, _ ptrace.Traces) (Accepted, error) {
	if _, err := s.write(t); err != nil {
		return Accepted{}, err
	}
	return Accepted{}, ErrNotImplemented
}

// WriteProfiles ingests OTLP profiles (DESIGN.md §5). Later vertical (M8+).
func (s *Storage) WriteProfiles(_ context.Context, t signal.TenantID, _ pprofile.Profiles) (Accepted, error) {
	if _, err := s.write(t); err != nil {
		return Accepted{}, err
	}
	return Accepted{}, ErrNotImplemented
}

// Query runs a query (DESIGN.md §5, §9). The engine selects the language by
// [Query.Lang]; all compile to the fetch contract. Returns a [Result].
func (s *Storage) Query(_ context.Context, t signal.TenantID, _ Query) (Result, error) {
	if s.closed.Load() {
		return Result{}, errors.Wrap(ErrClosed, "query")
	}
	if t == "" {
		t = "default"
	}
	_ = s.tenant.Resolve(t) // policy may gate query concurrency/limits (M3)
	// TODO(M4): parse → plan → shard → fetch → exec.
	return Result{}, ErrNotImplemented
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
