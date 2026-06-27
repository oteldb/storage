// Package obs bundles the library's injected observability — a zap logger, an OTel tracer, and the
// metric instruments — built once from the embedder's configuration and handed to each subsystem.
//
// As a library, oteldb/storage never owns a global logger, tracer, or meter: the embedder supplies
// them through [Config] (via storage.Options). Every handle is **no-op by default** — an unset
// logger becomes [zap.Nop], an unset provider becomes the OTel noop provider — so an unconfigured
// store spans, logs, and counts nothing and pays no overhead. The library imports only the OTel
// API (never an SDK or exporter); the embedder owns the SDK, sampling, and pipelines.
package obs

import (
	"context"

	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel/metric"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tnoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// scope is the instrumentation scope name for the library's tracer and meter.
const scope = "github.com/oteldb/storage"

// Config is the embedder-supplied observability configuration. A nil field selects the no-op
// implementation for that pillar.
type Config struct {
	Logger         *zap.Logger
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
}

// Obs is the observability handle passed to each subsystem. Log and Tracer are always non-nil
// (no-op when unconfigured); Admission holds the ingest meta-metrics.
type Obs struct {
	Log       *zap.Logger
	Tracer    trace.Tracer
	Admission *Admission
	Flush     *Flush
	Merge     *Merge
	Fetch     *Fetch
	Backend   *Backend
	WAL       *WAL
	RPC       *RPC
}

// New builds the observability handle, defaulting each unset pillar to its no-op implementation.
// It returns an error only if the meter rejects an instrument name (it does not for valid names).
func New(cfg Config) (*Obs, error) {
	log := cfg.Logger
	if log == nil {
		log = zap.NewNop()
	}

	tp := cfg.TracerProvider
	if tp == nil {
		tp = tnoop.NewTracerProvider()
	}

	mp := cfg.MeterProvider
	if mp == nil {
		mp = mnoop.NewMeterProvider()
	}

	meter := mp.Meter(scope)

	adm, err := newAdmission(meter)
	if err != nil {
		return nil, err
	}

	flush, merge, fetch, err := newEngineInstruments(meter)
	if err != nil {
		return nil, err
	}

	backend, err := newBackend(meter)
	if err != nil {
		return nil, err
	}

	wal, err := newWAL(meter)
	if err != nil {
		return nil, err
	}

	rpc, err := newRPC(meter)
	if err != nil {
		return nil, err
	}

	return &Obs{
		Log:       log,
		Tracer:    tp.Tracer(scope),
		Admission: adm,
		Flush:     flush,
		Merge:     merge,
		Fetch:     fetch,
		Backend:   backend,
		WAL:       wal,
		RPC:       rpc,
	}, nil
}

// NewNop returns a fully no-op handle (the default for tests and unconfigured stores). It never
// errors.
func NewNop() *Obs {
	o, _ := New(Config{})

	return o
}

// Base installs the injected logger as the [zctx] base in ctx, so any layer below can retrieve a
// trace-correlated logger via [zctx.From] without holding the obs handle. Seed it at each operation
// entry, before starting the span; [zctx.From] then attaches span_id/trace_id from the active span.
func (o *Obs) Base(ctx context.Context) context.Context {
	return zctx.Base(ctx, o.Log)
}

// Logger returns the trace-correlated logger for ctx — the injected base with span_id/trace_id
// attached when a span is active. It seeds the base first, so it is correct even when the caller
// forgot to (or could not) seed ctx upstream.
func (o *Obs) Logger(ctx context.Context) *zap.Logger {
	return zctx.From(zctx.Base(ctx, o.Log))
}
