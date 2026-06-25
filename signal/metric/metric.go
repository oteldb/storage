package metric

import (
	"go.opentelemetry.io/collector/pdata/pmetric"

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

// Project iterates an OTLP metric batch and calls emit for every gauge/sum number data
// point with its [Identity] and [Sample]. Resource and scope are converted once per
// group (hoisted). It returns how many points were accepted and how many were rejected
// (unsupported point kinds or value-less points). Histogram/exp-histogram/summary are not
// yet projected (their points are counted as rejected).
func Project(md pmetric.Metrics, emit func(Identity, Sample)) (accepted, rejected int) {
	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		rm := rms.At(i)
		resource := signal.Resource{
			SchemaURL:  []byte(rm.SchemaUrl()),
			Attributes: convertMap(rm.Resource().Attributes()),
		}

		sms := rm.ScopeMetrics()
		for j := range sms.Len() {
			sm := sms.At(j)
			scope := signal.Scope{
				Name:       []byte(sm.Scope().Name()),
				Version:    []byte(sm.Scope().Version()),
				SchemaURL:  []byte(sm.SchemaUrl()),
				Attributes: convertMap(sm.Scope().Attributes()),
			}

			metrics := sm.Metrics()
			for k := range metrics.Len() {
				a, r := projectMetric(metrics.At(k), resource, scope, emit)
				accepted += a
				rejected += r
			}
		}
	}

	return accepted, rejected
}

func projectMetric(m pmetric.Metric, resource signal.Resource, scope signal.Scope, emit func(Identity, Sample)) (int, int) {
	base := Identity{Series: signal.Series{Resource: resource, Scope: scope}, Name: []byte(m.Name()), Unit: []byte(m.Unit())}

	switch m.Type() {
	case pmetric.MetricTypeGauge:
		base.Kind = KindGauge

		return projectNumbers(m.Gauge().DataPoints(), base, resource, scope, emit)
	case pmetric.MetricTypeSum:
		sum := m.Sum()
		base.Kind = KindSum
		base.Temporality = temporalityOf(sum.AggregationTemporality())
		base.Monotonic = sum.IsMonotonic()

		return projectNumbers(sum.DataPoints(), base, resource, scope, emit)
	default:
		return 0, unsupportedPointCount(m)
	}
}

func projectNumbers(
	dps pmetric.NumberDataPointSlice, base Identity, resource signal.Resource, scope signal.Scope,
	emit func(Identity, Sample),
) (accepted, rejected int) {
	for i := range dps.Len() {
		dp := dps.At(i)

		v, ok := numberValue(dp)
		if !ok {
			rejected++

			continue
		}

		id := base
		id.Series = signal.Series{Resource: resource, Scope: scope, Attributes: convertMap(dp.Attributes())}
		emit(id, Sample{StartTs: int64(dp.StartTimestamp()), Ts: int64(dp.Timestamp()), Value: v})

		accepted++
	}

	return accepted, rejected
}

func numberValue(dp pmetric.NumberDataPoint) (float64, bool) {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue(), true
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue()), true
	default:
		return 0, false
	}
}

func temporalityOf(t pmetric.AggregationTemporality) Temporality {
	switch t {
	case pmetric.AggregationTemporalityDelta:
		return TemporalityDelta
	case pmetric.AggregationTemporalityCumulative:
		return TemporalityCumulative
	default:
		return TemporalityUnspecified
	}
}

// unsupportedPointCount returns the number of data points of a not-yet-projected metric
// type, so they are counted as rejected.
func unsupportedPointCount(m pmetric.Metric) int {
	switch m.Type() {
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len()
	default:
		return 0
	}
}
