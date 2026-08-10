// Package pdataconv is the optional OTel-Go bridge: it converts the collector pdata
// metrics type (pmetric.Metrics) into the storage library's internal, []byte-based
// [metric.Metrics] ingest batch. It exists so OTel users get zero-friction ingestion
// without the pdata dependency or its allocation profile reaching the storage hot path —
// pdata is referenced only here, never by the core packages or the storage facade.
//
// The conversion necessarily allocates: pdata stores keys/values as Go strings, so
// projecting them to the internal []byte model copies each one. Embedders that decode OTLP
// protobuf themselves should build [metric.Metrics] directly (aliasing the decode buffer)
// to stay allocation-free; this bridge is the compatibility path, not the fast path.
package pdataconv

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// Dropped counts what a conversion could not represent. Points and Exemplars are reported
// separately because they mean different things to a caller: Points feeds the OTLP
// partial-success response (a rejected data point is data the producer should retry), whereas a
// dropped exemplar only degrades trace correlation and must not inflate that count.
type Dropped struct {
	// Points is the number of data points that could not be represented.
	Points int
	// Exemplars is the number of exemplars that could not be represented — those on histogram,
	// exponential-histogram and summary points (see [AppendMetrics]), plus value-less ones.
	Exemplars int
}

// AppendMetrics converts an OTLP metrics batch into dst, reusing dst's retained capacity
// (call [metric.Metrics.Reset] or use [metric.GetMetrics] for a recycled batch). Gauge and sum
// number points convert directly, with their exemplars; histogram, exponential-histogram, and
// summary points are stored by **classic decomposition** into
// `_count`/`_sum`/`_bucket{le}`/`{quantile}` float series (see histogram.go), so they need no
// engine support.
//
// **Exemplars on decomposed points are dropped, not stored**, and counted in
// [Dropped.Exemplars]. One histogram point becomes several series, so an exemplar on it has no
// unambiguous home; the Prometheus convention would attach it to the `_bucket` whose `le` covers
// its value, but committing to that before native histograms exist would bake the choice into the
// stored shape. Value-less number points are skipped and counted in [Dropped.Points].
func AppendMetrics(dst *metric.Metrics, md pmetric.Metrics) (dropped Dropped) {
	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		srm := rms.At(i)

		rm := dst.AddResource()
		rm.Resource = signal.Resource{
			SchemaURL:  []byte(srm.SchemaUrl()),
			Attributes: convertMap(srm.Resource().Attributes()),
		}

		sms := srm.ScopeMetrics()
		for j := range sms.Len() {
			ssm := sms.At(j)

			sm := rm.AddScope()
			sm.Scope = signal.Scope{
				Name:       []byte(ssm.Scope().Name()),
				Version:    []byte(ssm.Scope().Version()),
				SchemaURL:  []byte(ssm.SchemaUrl()),
				Attributes: convertMap(ssm.Scope().Attributes()),
			}

			metrics := ssm.Metrics()
			for k := range metrics.Len() {
				d := appendMetric(sm, metrics.At(k))
				dropped.Points += d.Points
				dropped.Exemplars += d.Exemplars
			}
		}
	}

	return dropped
}

func appendMetric(sm *metric.ScopeMetrics, m pmetric.Metric) (dropped Dropped) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		mt := sm.AddMetric()
		mt.Name = []byte(m.Name())
		mt.Unit = []byte(m.Unit())
		mt.Kind = metric.KindGauge

		return appendNumbers(mt, m.Gauge().DataPoints())
	case pmetric.MetricTypeSum:
		sum := m.Sum()
		mt := sm.AddMetric()
		mt.Name = []byte(m.Name())
		mt.Unit = []byte(m.Unit())
		mt.Kind = metric.KindSum
		mt.Temporality = temporalityOf(sum.AggregationTemporality())
		mt.Monotonic = sum.IsMonotonic()

		return appendNumbers(mt, sum.DataPoints())
	case pmetric.MetricTypeHistogram:
		appendHistogram(sm, m)

		return Dropped{Exemplars: decomposedExemplarCount(m)}
	case pmetric.MetricTypeExponentialHistogram:
		appendExpHistogram(sm, m)

		return Dropped{Exemplars: decomposedExemplarCount(m)}
	case pmetric.MetricTypeSummary:
		appendSummary(sm, m)

		return Dropped{} // summary data points carry no exemplars in OTLP
	default:
		return Dropped{Points: unsupportedPointCount(m)}
	}
}

func appendNumbers(mt *metric.Metric, dps pmetric.NumberDataPointSlice) (dropped Dropped) {
	for i := range dps.Len() {
		dp := dps.At(i)

		v, ok := numberValue(dp)
		if !ok {
			dropped.Points++
			// The point is gone, so its exemplars have no series to hang off either.
			dropped.Exemplars += dp.Exemplars().Len()

			continue
		}

		p := mt.AddPoint()
		p.Attributes = convertMap(dp.Attributes())
		p.StartTs = int64(dp.StartTimestamp())
		p.Ts = int64(dp.Timestamp())
		p.Value = v
		dropped.Exemplars += appendExemplars(p, dp.Exemplars())
	}

	return dropped
}

// appendExemplars converts a number point's exemplars, returning how many were dropped (only
// value-less ones, which cannot be represented).
func appendExemplars(p *metric.NumberPoint, exs pmetric.ExemplarSlice) (dropped int) {
	for i := range exs.Len() {
		src := exs.At(i)

		v, ok := exemplarValue(src)
		if !ok {
			dropped++

			continue
		}

		e := p.AddExemplar()
		e.Ts = int64(src.Timestamp())
		e.Value = v
		e.FilteredAttributes = convertMap(src.FilteredAttributes())

		if tid := src.TraceID(); !tid.IsEmpty() {
			e.TraceID = append(e.TraceID[:0], tid[:]...)
		}

		if sid := src.SpanID(); !sid.IsEmpty() {
			e.SpanID = append(e.SpanID[:0], sid[:]...)
		}
	}

	return dropped
}

func exemplarValue(e pmetric.Exemplar) (float64, bool) {
	switch e.ValueType() {
	case pmetric.ExemplarValueTypeDouble:
		return e.DoubleValue(), true
	case pmetric.ExemplarValueTypeInt:
		return float64(e.IntValue()), true
	default:
		return 0, false
	}
}

// decomposedExemplarCount is the number of exemplars carried by a metric stored via classic
// decomposition — all of which are dropped, since no single decomposed series owns them.
func decomposedExemplarCount(m pmetric.Metric) int {
	n := 0

	switch m.Type() { //nolint:exhaustive // only the decomposed types reach here
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := range dps.Len() {
			n += dps.At(i).Exemplars().Len()
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := range dps.Len() {
			n += dps.At(i).Exemplars().Len()
		}
	}

	return n
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

func temporalityOf(t pmetric.AggregationTemporality) metric.Temporality {
	switch t {
	case pmetric.AggregationTemporalityDelta:
		return metric.TemporalityDelta
	case pmetric.AggregationTemporalityCumulative:
		return metric.TemporalityCumulative
	default:
		return metric.TemporalityUnspecified
	}
}

// unsupportedPointCount returns the number of data points of a not-yet-representable metric
// type, so they are counted as dropped.
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

// convertMap projects an OTLP attribute map to internal typed [signal.Attributes]. Byte
// values are copied out of the pdata buffers so the internal model owns them.
func convertMap(m pcommon.Map) signal.Attributes {
	if m.Len() == 0 {
		return nil
	}

	kvs := make([]signal.KeyValue, 0, m.Len())
	for k, v := range m.All() {
		kvs = append(kvs, signal.KeyValue{Key: []byte(k), Value: convertValue(v)})
	}

	return signal.NewAttributes(kvs...)
}

// convertValue projects an OTLP AnyValue to the internal typed [signal.Value], preserving
// type (and recursing into slices/maps).
func convertValue(v pcommon.Value) signal.Value {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return signal.StringValue([]byte(v.Str()))
	case pcommon.ValueTypeBool:
		return signal.BoolValue(v.Bool())
	case pcommon.ValueTypeInt:
		return signal.IntValue(v.Int())
	case pcommon.ValueTypeDouble:
		return signal.DoubleValue(v.Double())
	case pcommon.ValueTypeBytes:
		return signal.BytesValue(v.Bytes().AsRaw())
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		vs := make([]signal.Value, s.Len())
		for i := range s.Len() {
			vs[i] = convertValue(s.At(i))
		}

		return signal.SliceValue(vs...)
	case pcommon.ValueTypeMap:
		return signal.MapValue(convertMap(v.Map())...)
	default: // ValueTypeEmpty
		return signal.EmptyValue()
	}
}
