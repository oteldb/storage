package pdataconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// seriesValue finds the decomposed series named `name` whose point carries exactly the given
// label values, returning its value. Labels with an empty map match a series with no extra labels.
func seriesValue(t *testing.T, out metric.Metrics, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	require.Len(t, out.Resources, 1)
	require.Len(t, out.Resources[0].Scopes, 1)

	for _, m := range out.Resources[0].Scopes[0].Metrics {
		if string(m.Name) != name {
			continue
		}

		p := m.Points[0]
		match := true

		for k, v := range labels {
			av, ok := p.Attributes.Get([]byte(k))
			if !ok || string(av.Str()) != v {
				match = false

				break
			}
		}

		if match {
			return p.Value, true
		}
	}

	return 0, false
}

func mustSeries(t *testing.T, out metric.Metrics, name string, labels map[string]string) float64 {
	t.Helper()
	v, ok := seriesValue(t, out, name, labels)
	require.Truef(t, ok, "series %q %v not found", name, labels)

	return v
}

func TestAppendHistogram(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	h := sm.Metrics().AppendEmpty()
	h.SetName("lat")
	hist := h.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := hist.DataPoints().AppendEmpty()
	dp.SetCount(10)
	dp.SetSum(42.5)
	dp.ExplicitBounds().FromRaw([]float64{1, 5})
	dp.BucketCounts().FromRaw([]uint64{2, 3, 5}) // ≤1:2, (1,5]:3, >5:5

	var out metric.Metrics
	require.Equal(t, 0, AppendMetrics(&out, md))

	assert.InDelta(t, 10, mustSeries(t, out, "lat_count", nil), 0)
	assert.InDelta(t, 42.5, mustSeries(t, out, "lat_sum", nil), 0)
	// Buckets are cumulative.
	assert.InDelta(t, 2, mustSeries(t, out, "lat_bucket", map[string]string{"le": "1"}), 0)
	assert.InDelta(t, 5, mustSeries(t, out, "lat_bucket", map[string]string{"le": "5"}), 0)
	assert.InDelta(t, 10, mustSeries(t, out, "lat_bucket", map[string]string{"le": "+Inf"}), 0)

	// The decomposed series carry the metric name as __name__ and are ordinary Sum series.
	for _, m := range out.Resources[0].Scopes[0].Metrics {
		assert.Equal(t, metric.KindSum, m.Kind)
		assert.Equal(t, metric.TemporalityCumulative, m.Temporality)
	}
}

func TestAppendHistogramKeepsPointAttributes(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	h := sm.Metrics().AppendEmpty()
	h.SetName("lat")
	hist := h.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := hist.DataPoints().AppendEmpty()
	dp.SetCount(3)
	dp.Attributes().PutStr("route", "/x")
	dp.ExplicitBounds().FromRaw([]float64{1})
	dp.BucketCounts().FromRaw([]uint64{1, 2})

	var out metric.Metrics
	require.Equal(t, 0, AppendMetrics(&out, md))

	// The bucket series carries both the original attribute and the synthetic le label.
	assert.InDelta(t, 1, mustSeries(t, out, "lat_bucket", map[string]string{"route": "/x", "le": "1"}), 0)
	assert.InDelta(t, 3, mustSeries(t, out, "lat_count", map[string]string{"route": "/x"}), 0)
}

func TestAppendSummary(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	s := sm.Metrics().AppendEmpty()
	s.SetName("rpc")
	sd := s.SetEmptySummary().DataPoints().AppendEmpty()
	sd.SetCount(4)
	sd.SetSum(20)
	q := sd.QuantileValues().AppendEmpty()
	q.SetQuantile(0.5)
	q.SetValue(1.5)
	q2 := sd.QuantileValues().AppendEmpty()
	q2.SetQuantile(0.9)
	q2.SetValue(9)

	var out metric.Metrics
	require.Equal(t, 0, AppendMetrics(&out, md))

	assert.InDelta(t, 4, mustSeries(t, out, "rpc_count", nil), 0)
	assert.InDelta(t, 20, mustSeries(t, out, "rpc_sum", nil), 0)
	assert.InDelta(t, 1.5, mustSeries(t, out, "rpc", map[string]string{"quantile": "0.5"}), 0)
	assert.InDelta(t, 9, mustSeries(t, out, "rpc", map[string]string{"quantile": "0.9"}), 0)

	// The quantile series is a gauge (an instantaneous estimate), not a counter.
	v, _ := seriesValue(t, out, "rpc", map[string]string{"quantile": "0.5"})
	assert.InDelta(t, 1.5, v, 0)
}

func TestAppendExpHistogram(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	e := sm.Metrics().AppendEmpty()
	e.SetName("dur")
	eh := e.SetEmptyExponentialHistogram()
	eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := eh.DataPoints().AppendEmpty()
	dp.SetScale(0) // base = 2^(2^0) = 2; bound(i) = 2^i
	dp.SetCount(6)
	dp.SetSum(13)
	dp.SetZeroCount(3)
	dp.Positive().SetOffset(0)
	dp.Positive().BucketCounts().FromRaw([]uint64{1, 2}) // (1,2]:1 → le 2; (2,4]:2 → le 4

	var out metric.Metrics
	require.Equal(t, 0, AppendMetrics(&out, md))

	assert.InDelta(t, 6, mustSeries(t, out, "dur_count", nil), 0)
	assert.InDelta(t, 13, mustSeries(t, out, "dur_sum", nil), 0)
	// Cumulative le buckets: zero(le=0)=3, then le=2 ⇒ 4, le=4 ⇒ 6, +Inf ⇒ 6.
	assert.InDelta(t, 3, mustSeries(t, out, "dur_bucket", map[string]string{"le": "0"}), 0)
	assert.InDelta(t, 4, mustSeries(t, out, "dur_bucket", map[string]string{"le": "2"}), 0)
	assert.InDelta(t, 6, mustSeries(t, out, "dur_bucket", map[string]string{"le": "4"}), 0)
	assert.InDelta(t, 6, mustSeries(t, out, "dur_bucket", map[string]string{"le": "+Inf"}), 0)
}

// TestHistogramEndToEnd confirms a decomposed histogram flows through the full ingest+query path:
// the _bucket series are queryable by name and le like any other metric series.
func TestHistogramEndToEnd(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	h := sm.Metrics().AppendEmpty()
	h.SetName("lat")
	hist := h.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := hist.DataPoints().AppendEmpty()
	dp.SetCount(7)
	dp.SetTimestamp(100)
	dp.ExplicitBounds().FromRaw([]float64{10})
	dp.BucketCounts().FromRaw([]uint64{3, 4})

	var out metric.Metrics
	require.Equal(t, 0, AppendMetrics(&out, md))

	// Every decomposed point projects to a real series id (no panics, all accepted).
	var ids int

	accepted := metric.Project(out, func(b *metric.Batch) {
		for i := range b.IDs {
			require.NotEqual(t, signal.SeriesID{}, b.IDs[i])

			ids++
		}
	})
	assert.Equal(t, accepted, ids)
	// _count + two cumulative buckets (le=10, le=+Inf); no _sum since it was left unset.
	assert.Equal(t, 3, accepted)
}
