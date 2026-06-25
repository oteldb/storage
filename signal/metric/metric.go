package metric

import (
	"encoding/binary"

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

// Identity is a metric series' full identity: the [signal.Series] backbone plus the
// metric-specific fields. [Identity.SeriesID] folds them all into one content-addressed id.
type Identity struct {
	Series      signal.Series
	Name        []byte
	Unit        []byte
	Kind        PointKind
	Temporality Temporality
	Monotonic   bool
}

// SeriesID hashes the full metric identity: the backbone pre-image followed by the
// metric-specific fields, so two metrics differing only in name/unit/kind/temporality/
// monotonicity get distinct ids.
func (id Identity) SeriesID() signal.SeriesID {
	buf := id.Series.AppendHashInput(nil)
	buf = appendLenBytes(buf, id.Name)
	buf = appendLenBytes(buf, id.Unit)

	var mono byte
	if id.Monotonic {
		mono = 1
	}

	buf = append(buf, byte(id.Kind), byte(id.Temporality), mono)

	return signal.HashBytes(buf)
}

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

func appendLenBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))

	return append(dst, b...)
}
