package obs

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Admission holds the ingest/admission meta-metrics (DESIGN §8a — observability is mandatory for
// overload control). They are recorded in **bulk** (one Add per write call per reason, never
// per-point), so they cost nothing on the hot inner loops. With the no-op meter every Add is a
// no-op.
type Admission struct {
	preparedAttempts metric.Int64Counter
	preparedBatches  metric.Int64Counter
	preparedSamples  metric.Int64Counter
	accepted         metric.Int64Counter
	rejected         metric.Int64Counter
	sampled          metric.Int64Counter
	overflow         metric.Int64Counter
}

func newAdmission(m metric.Meter) (*Admission, error) {
	accepted, err := m.Int64Counter("storage.ingest.accepted",
		metric.WithDescription("data points accepted for storage (includes sampled-out points, which are represented)"),
		metric.WithUnit("{point}"))
	if err != nil {
		return nil, err
	}

	rejected, err := m.Int64Counter("storage.ingest.rejected",
		metric.WithDescription("data points shed by admission control, by reason"),
		metric.WithUnit("{point}"))
	if err != nil {
		return nil, err
	}

	sampled, err := m.Int64Counter("storage.ingest.sampled_dropped",
		metric.WithDescription("data points dropped by budgeted sampling (represented by a kept peer's scale factor)"),
		metric.WithUnit("{point}"))
	if err != nil {
		return nil, err
	}

	overflow, err := m.Int64Counter("storage.ingest.overflowed",
		metric.WithDescription("data points for new series past the soft cardinality budget, routed to an overflow series"),
		metric.WithUnit("{point}"))
	if err != nil {
		return nil, err
	}

	b := imb{m: m}
	attempts := b.counter("storage.ingest.prepared.attempts", "parallel prepared attempts by outcome", "{attempt}")
	batches := b.counter("storage.ingest.prepared.batches", "completed prepared calls by path and reason", "{batch}")
	samples := b.counter("storage.ingest.prepared.samples", "accepted prepared samples by path and reason", "{point}")
	if b.err != nil {
		return nil, b.err
	}
	return &Admission{accepted: accepted, rejected: rejected, sampled: sampled, overflow: overflow,
		preparedAttempts: attempts, preparedBatches: batches, preparedSamples: samples}, nil
}

// Accepted records n points accepted for the given signal. A zero n is ignored.
func (a *Admission) Accepted(ctx context.Context, n int64, sig string) {
	if n <= 0 {
		return
	}

	a.accepted.Add(ctx, n, metric.WithAttributes(attribute.String("signal", sig)))
}

// Rejected records n points shed for the given signal and reason (e.g. out_of_order, rate_limit,
// max_series, max_in_flight_bytes, reserved_tenant). A zero n is ignored.
func (a *Admission) Rejected(ctx context.Context, n int64, sig, reason string) {
	if n <= 0 {
		return
	}

	a.rejected.Add(ctx, n, metric.WithAttributes(
		attribute.String("signal", sig),
		attribute.String("reason", reason),
	))
}

// SampledDropped records n points dropped by budgeted sampling for the given signal. A zero n is
// ignored.
func (a *Admission) SampledDropped(ctx context.Context, n int64, sig string) {
	if n <= 0 {
		return
	}

	a.sampled.Add(ctx, n, metric.WithAttributes(attribute.String("signal", sig)))
}

// Overflowed records n points routed to an overflow series past the soft cardinality budget for the
// given signal. A zero n is ignored.
func (a *Admission) Overflowed(ctx context.Context, n int64, sig string) {
	if n <= 0 {
		return
	}

	a.overflow.Add(ctx, n, metric.WithAttributes(attribute.String("signal", sig)))
}

// PreparedAttempt counts parallel preparation attempts, including transient misses.
func (a *Admission) PreparedAttempt(ctx context.Context, outcome string) {
	a.preparedAttempts.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// PreparedResult counts completed prepared calls and their accepted samples.
func (a *Admission) PreparedResult(ctx context.Context, path, reason string, accepted int) {
	attrs := metric.WithAttributes(attribute.String("path", path), attribute.String("reason", reason))
	a.preparedBatches.Add(ctx, 1, attrs)
	if accepted > 0 {
		a.preparedSamples.Add(ctx, int64(accepted), attrs)
	}
}
