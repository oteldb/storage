package obs

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

// Retention reports confirmed expiry work and physical lag behind requested cutoffs.
type Retention struct {
	droppedParts metric.Int64Counter
	droppedBytes metric.Int64Counter
	rewrites     metric.Int64Counter
	lag          metric.Float64Gauge
}

// Dropped records whole-part retirements after their catalog publication succeeds.
func (r *Retention) Dropped(ctx context.Context, sig string, parts, bytes int64) {
	if parts <= 0 {
		return
	}
	a := sigAttr(sig)
	r.droppedParts.Add(ctx, parts, a)
	if bytes > 0 {
		r.droppedBytes.Add(ctx, bytes, a)
	}
}

// Rewritten records a confirmed merge that rewrites a retention straddler.
func (r *Retention) Rewritten(ctx context.Context, sig string) {
	r.rewrites.Add(ctx, 1, sigAttr(sig))
}

// Lag reports the maximum physical expiry lag among the signal's locally owned engines.
func (r *Retention) Lag(ctx context.Context, sig string, seconds float64) {
	r.lag.Record(ctx, max(seconds, 0), sigAttr(sig))
}

func newRetention(m metric.Meter) (*Retention, error) {
	b := &imb{m: m}
	r := &Retention{
		droppedParts: b.counter("storage.retention.dropped_parts", "whole parts retired by retention", "{part}"),
		droppedBytes: b.counter("storage.retention.dropped_bytes",
			"recorded stored bytes retired by retention, excluding unrecorded sidecars", "By"),
		rewrites: b.counter("storage.retention.rewrites", "published merges that rewrite retention straddlers", "{merge}"),
	}
	if b.err != nil {
		return nil, b.err
	}
	var err error
	r.lag, err = m.Float64Gauge("storage.retention.lag_seconds",
		metric.WithDescription("maximum live-part expiry lag behind requested retention"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	return r, nil
}
