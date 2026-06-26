package pdataconv

import (
	"math"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// Histogram, exponential-histogram, and summary points are stored by **classic decomposition**:
// each point explodes into ordinary float series the columnar engine already handles — `_count`,
// `_sum`, and cumulative `_bucket{le=…}` series for histograms (the Prometheus convention, so an
// embedder's histogram_quantile works directly), and `_count`/`_sum`/`{quantile=…}` for summaries.
// An exponential histogram is converted to explicit `le` buckets from its scale, so it stores the
// same way. The decomposition lives in this bridge (off the hot path) and reuses the metric model's
// Add* builders, so the engine, merge, downsample, sampling, and fetch paths need no histogram code.

// appendHistogram decomposes an explicit-bucket histogram metric into _count/_sum/_bucket series.
func appendHistogram(sm *metric.ScopeMetrics, m pmetric.Metric) {
	h := m.Histogram()
	temp := temporalityOf(h.AggregationTemporality())
	cumulative := h.AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	name, unit := []byte(m.Name()), []byte(m.Unit())

	dps := h.DataPoints()
	for i := range dps.Len() {
		dp := dps.At(i)
		base := convertMap(dp.Attributes())
		st, ts := int64(dp.StartTimestamp()), int64(dp.Timestamp())

		addSeries(sm, suffix(name, "_count"), unit, temp, cumulative, base, st, ts, float64(dp.Count()))

		if dp.HasSum() {
			addSeries(sm, suffix(name, "_sum"), unit, temp, false, base, st, ts, dp.Sum())
		}

		bounds, counts := dp.ExplicitBounds(), dp.BucketCounts()

		var cum uint64

		for b := range counts.Len() {
			cum += counts.At(b)

			le := posInf
			if b < bounds.Len() {
				le = formatBound(bounds.At(b))
			}

			addSeries(sm, suffix(name, "_bucket"), unit, temp, cumulative,
				withLabel(base, leKey, le), st, ts, float64(cum))
		}
	}
}

// appendSummary decomposes a summary metric into _count/_sum counters and per-quantile gauges.
func appendSummary(sm *metric.ScopeMetrics, m pmetric.Metric) {
	name, unit := []byte(m.Name()), []byte(m.Unit())

	dps := m.Summary().DataPoints()
	for i := range dps.Len() {
		dp := dps.At(i)
		base := convertMap(dp.Attributes())
		st, ts := int64(dp.StartTimestamp()), int64(dp.Timestamp())

		// A summary has no temporality; its count/sum are cumulative counters.
		addSeries(sm, suffix(name, "_count"), unit, metric.TemporalityCumulative, true, base, st, ts, float64(dp.Count()))
		addSeries(sm, suffix(name, "_sum"), unit, metric.TemporalityCumulative, false, base, st, ts, dp.Sum())

		qs := dp.QuantileValues()
		for q := range qs.Len() {
			qv := qs.At(q)
			// The quantile estimate is an instantaneous gauge under the base name with a quantile label.
			mt := sm.AddMetric()
			mt.Name = name
			mt.Unit = unit
			mt.Kind = metric.KindGauge

			p := mt.AddPoint()
			p.Attributes = withLabel(base, quantileKey, formatBound(qv.Quantile()))
			p.StartTs, p.Ts, p.Value = st, ts, qv.Value()
		}
	}
}

// appendExpHistogram converts an exponential histogram to explicit cumulative `le` buckets (derived
// from its scale) and decomposes it the same way as an explicit histogram.
func appendExpHistogram(sm *metric.ScopeMetrics, m pmetric.Metric) {
	eh := m.ExponentialHistogram()
	temp := temporalityOf(eh.AggregationTemporality())
	cumulative := eh.AggregationTemporality() == pmetric.AggregationTemporalityCumulative
	name, unit := []byte(m.Name()), []byte(m.Unit())

	dps := eh.DataPoints()
	for i := range dps.Len() {
		dp := dps.At(i)
		base := convertMap(dp.Attributes())
		st, ts := int64(dp.StartTimestamp()), int64(dp.Timestamp())

		addSeries(sm, suffix(name, "_count"), unit, temp, cumulative, base, st, ts, float64(dp.Count()))

		if dp.HasSum() {
			addSeries(sm, suffix(name, "_sum"), unit, temp, false, base, st, ts, dp.Sum())
		}

		for _, b := range expBuckets(dp) {
			le := posInf
			if !math.IsInf(b.le, 1) {
				le = formatBound(b.le)
			}

			addSeries(sm, suffix(name, "_bucket"), unit, temp, cumulative,
				withLabel(base, leKey, le), st, ts, float64(b.cumulative))
		}
	}
}

// expBucket is a derived classic bucket: an upper bound (le) and the cumulative count of
// observations ≤ that bound.
type expBucket struct {
	le         float64
	cumulative uint64
}

// expBuckets converts an exponential histogram point's negative/zero/positive buckets into classic
// `le` buckets in ascending bound order with cumulative counts, ending at +Inf = total count.
func expBuckets(dp pmetric.ExponentialHistogramDataPoint) []expBucket {
	factor := math.Exp2(math.Exp2(-float64(dp.Scale()))) // factor is the exponential base; an index maps to factor raised to that index
	bound := func(index int) float64 { return math.Pow(factor, float64(index)) }

	type pair struct {
		le float64
		n  uint64
	}

	var pairs []pair

	// Negative buckets: index (offset+k) holds values in [-base^(i+1), -base^i); the bucket's upper
	// bound (closest to zero) is -base^i.
	neg := dp.Negative()
	for k := range neg.BucketCounts().Len() {
		if n := neg.BucketCounts().At(k); n > 0 {
			pairs = append(pairs, pair{le: -bound(int(neg.Offset()) + k), n: n})
		}
	}

	// The zero bucket: values at (within the zero threshold of) zero, bound 0.
	if zc := dp.ZeroCount(); zc > 0 {
		pairs = append(pairs, pair{le: 0, n: zc})
	}

	// Positive buckets: index (offset+k) holds values in (base^i, base^(i+1)]; upper bound base^(i+1).
	pos := dp.Positive()
	for k := range pos.BucketCounts().Len() {
		if n := pos.BucketCounts().At(k); n > 0 {
			pairs = append(pairs, pair{le: bound(int(pos.Offset()) + k + 1), n: n})
		}
	}

	// Already in ascending le order (negatives ascending, then 0, then positives ascending), since
	// offsets index monotonically. Cumulate.
	out := make([]expBucket, 0, len(pairs)+1)

	var cum uint64

	for _, p := range pairs {
		cum += p.n
		out = append(out, expBucket{le: p.le, cumulative: cum})
	}

	// The +Inf bucket carries the full count (covers any not-counted residue, e.g. NaN observations).
	out = append(out, expBucket{le: math.Inf(1), cumulative: dp.Count()})

	return out
}

// posInf is the Prometheus `le` value for the catch-all overflow bucket (the total count).
const posInf = "+Inf"

// Reserved label keys for the decomposed series (Prometheus convention).
var (
	leKey       = []byte("le")
	quantileKey = []byte("quantile")
)

// addSeries appends a one-point synthetic Sum series (a decomposed _count/_sum/_bucket).
func addSeries(sm *metric.ScopeMetrics, name, unit []byte, temp metric.Temporality, monotonic bool,
	attrs signal.Attributes, startTs, ts int64, value float64,
) {
	mt := sm.AddMetric()
	mt.Name = name
	mt.Unit = unit
	mt.Kind = metric.KindSum
	mt.Temporality = temp
	mt.Monotonic = monotonic

	p := mt.AddPoint()
	p.Attributes = attrs
	p.StartTs, p.Ts, p.Value = startTs, ts, value
}

// withLabel returns base plus one extra (key, string-value) label, re-sorted by key.
func withLabel(base signal.Attributes, key []byte, value string) signal.Attributes {
	kvs := make([]signal.KeyValue, 0, len(base)+1)
	kvs = append(kvs, base...)
	kvs = append(kvs, signal.KeyValue{Key: key, Value: signal.StringValue([]byte(value))})

	return signal.NewAttributes(kvs...)
}

// suffix returns name+s as a fresh byte slice.
func suffix(name []byte, s string) []byte {
	out := make([]byte, 0, len(name)+len(s))
	out = append(out, name...)

	return append(out, s...)
}

// formatBound formats a bucket bound / quantile as a stable decimal string (the `le`/`quantile`
// label value), matching the float text an embedder parses back.
func formatBound(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
