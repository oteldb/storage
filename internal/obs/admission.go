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
	accepted metric.Int64Counter
	rejected metric.Int64Counter
	sampled  metric.Int64Counter
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

	return &Admission{accepted: accepted, rejected: rejected, sampled: sampled}, nil
}

// Accepted records n points accepted for the given signal. A zero n is ignored.
func (a *Admission) Accepted(ctx context.Context, n int64, sig string) {
	if n <= 0 {
		return
	}

	a.accepted.Add(ctx, n, metric.WithAttributes(attribute.String("signal", sig)))
}

// Rejected records n points shed for the given signal and reason (e.g. out_of_order, rate_limit,
// max_series, max_in_flight_bytes). A zero n is ignored.
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
