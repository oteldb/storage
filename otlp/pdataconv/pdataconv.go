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

// AppendMetrics converts an OTLP metrics batch into dst, reusing dst's retained capacity
// (call [metric.Metrics.Reset] or use [metric.GetMetrics] for a recycled batch). Only
// gauge and sum number points are representable today; histogram, exponential-histogram,
// and summary points, plus value-less number points, are skipped and counted in dropped so
// the caller can fold them into an OTLP partial-success response.
func AppendMetrics(dst *metric.Metrics, md pmetric.Metrics) (dropped int) {
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
				dropped += appendMetric(sm, metrics.At(k))
			}
		}
	}

	return dropped
}

func appendMetric(sm *metric.ScopeMetrics, m pmetric.Metric) (dropped int) {
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
	default:
		return unsupportedPointCount(m)
	}
}

func appendNumbers(mt *metric.Metric, dps pmetric.NumberDataPointSlice) (dropped int) {
	for i := range dps.Len() {
		dp := dps.At(i)

		v, ok := numberValue(dp)
		if !ok {
			dropped++

			continue
		}

		p := mt.AddPoint()
		p.Attributes = convertMap(dp.Attributes())
		p.StartTs = int64(dp.StartTimestamp())
		p.Ts = int64(dp.Timestamp())
		p.Value = v
	}

	return dropped
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
