package metric

import (
	"github.com/oteldb/storage/signal"
)

// PointKind is the metric point kind that contributes to identity.
type PointKind uint8

const (
	// KindGauge is a gauge (instantaneous value).
	KindGauge PointKind = iota
	// KindSum is a sum/counter (with temporality + monotonicity).
	KindSum
)

// Temporality is the aggregation temporality (part of a sum's identity).
type Temporality uint8

const (
	// TemporalityUnspecified is an unset temporality.
	TemporalityUnspecified Temporality = iota
	// TemporalityDelta aggregates over the (start, time] window.
	TemporalityDelta
	// TemporalityCumulative aggregates since start.
	TemporalityCumulative
)

// Reserved attribute keys: the metric-specific identity fields are folded into the
// series' point attributes as reserved labels, so the unified [signal.Series] identity +
// the index machinery handle metrics with no metric-specific code, and queries can match
// `__name__` etc. (Prometheus convention). Resource and Scope stay structured.
var (
	LabelName        = []byte("__name__")
	LabelUnit        = []byte("__unit__")
	LabelKind        = []byte("__kind__")
	LabelTemporality = []byte("__temporality__")
	LabelMonotonic   = []byte("__monotonic__")
)

// Identity is a metric series' full identity: the [signal.Series] backbone (Resource +
// Scope + data-point attributes) plus the metric-specific fields name, unit, kind,
// temporality and monotonicity.
type Identity struct {
	Series      signal.Series
	Name        []byte
	Unit        []byte
	Kind        PointKind
	Temporality Temporality
	Monotonic   bool
}

// ToSeries folds the metric-specific fields into the point attributes as reserved labels
// and returns the full [signal.Series] — the value stored, indexed, and returned in
// fetch batches. Two metrics differing only in name/unit/kind/temporality/monotonicity
// produce distinct series (and thus distinct [signal.SeriesID]).
func (id Identity) ToSeries() signal.Series {
	pts := id.Series.Attributes
	merged := make([]signal.KeyValue, 0, len(pts)+5)
	merged = append(merged, pts...)
	merged = append(merged,
		signal.KeyValue{Key: LabelName, Value: signal.StringValue(id.Name)},
		signal.KeyValue{Key: LabelUnit, Value: signal.StringValue(id.Unit)},
		signal.KeyValue{Key: LabelKind, Value: signal.IntValue(int64(id.Kind))},
		signal.KeyValue{Key: LabelTemporality, Value: signal.IntValue(int64(id.Temporality))},
		signal.KeyValue{Key: LabelMonotonic, Value: signal.BoolValue(id.Monotonic)},
	)

	return signal.Series{
		Resource:   id.Series.Resource,
		Scope:      id.Series.Scope,
		Attributes: signal.NewAttributes(merged...),
	}
}

// SeriesID is the content-addressed id of the full identity ([ToSeries] hashed).
func (id Identity) SeriesID() signal.SeriesID { return id.ToSeries().Hash() }

// Sample is a projected number data point: its (start, time] timestamps and value.
type Sample struct {
	StartTs int64
	Ts      int64
	Value   float64
}

// Project iterates an internal [Metrics] batch and calls emit for every number data point
// with its [Identity] and [Sample]. Resource and scope are hoisted once per group. It
// returns how many points were emitted. Every point in a [Metrics] batch is well-formed by
// construction (value-less and unsupported OTLP points are filtered by the producer — e.g.
// the otlp/pdataconv bridge), so projection rejects nothing; out-of-order rejection is the
// engine's concern downstream.
func Project(md Metrics, emit func(Identity, Sample)) (accepted int) {
	for ri := range md.Resources {
		rm := &md.Resources[ri]

		for si := range rm.Scopes {
			sm := &rm.Scopes[si]

			for mi := range sm.Metrics {
				accepted += projectMetric(&sm.Metrics[mi], rm.Resource, sm.Scope, emit)
			}
		}
	}

	return accepted
}

func projectMetric(m *Metric, resource signal.Resource, scope signal.Scope, emit func(Identity, Sample)) int {
	base := Identity{
		Series:      signal.Series{Resource: resource, Scope: scope},
		Name:        m.Name,
		Unit:        m.Unit,
		Kind:        m.Kind,
		Temporality: m.Temporality,
		Monotonic:   m.Monotonic,
	}

	for i := range m.Points {
		p := &m.Points[i]
		id := base
		id.Series = signal.Series{Resource: resource, Scope: scope, Attributes: p.Attributes}
		emit(id, Sample{StartTs: p.StartTs, Ts: p.Ts, Value: p.Value})
	}

	return len(m.Points)
}
