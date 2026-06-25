package pdataconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

func TestAppendMetricsGaugeAndSum(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("lib")

	g := sm.Metrics().AppendEmpty()
	g.SetName("http.requests")
	g.SetUnit("1")
	gp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	gp.SetDoubleValue(1.0)
	gp.SetTimestamp(pcommon.Timestamp(1000))
	gp.Attributes().PutStr("route", "/x")

	s := sm.Metrics().AppendEmpty()
	s.SetName("bytes.total")
	sum := s.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sp := sum.DataPoints().AppendEmpty()
	sp.SetIntValue(42)
	sp.SetTimestamp(pcommon.Timestamp(5))

	var out metric.Metrics
	dropped := AppendMetrics(&out, md)
	assert.Equal(t, 0, dropped)

	require.Len(t, out.Resources, 1)
	res := out.Resources[0]
	rv, _ := res.Resource.Attributes.Get([]byte("service.name"))
	assert.Equal(t, []byte("api"), rv.Str())
	require.Len(t, res.Scopes, 1)
	assert.Equal(t, []byte("lib"), res.Scopes[0].Scope.Name)
	require.Len(t, res.Scopes[0].Metrics, 2)

	gm := res.Scopes[0].Metrics[0]
	assert.Equal(t, []byte("http.requests"), gm.Name)
	assert.Equal(t, metric.KindGauge, gm.Kind)
	require.Len(t, gm.Points, 1)
	assert.Equal(t, int64(1000), gm.Points[0].Ts)
	assert.InDelta(t, 1.0, gm.Points[0].Value, 0)
	pv, _ := gm.Points[0].Attributes.Get([]byte("route"))
	assert.Equal(t, []byte("/x"), pv.Str())

	smt := res.Scopes[0].Metrics[1]
	assert.Equal(t, metric.KindSum, smt.Kind)
	assert.Equal(t, metric.TemporalityCumulative, smt.Temporality)
	assert.True(t, smt.Monotonic)
	require.Len(t, smt.Points, 1)
	assert.InDelta(t, 42.0, smt.Points[0].Value, 0, "int widened to float")
}

func TestAppendMetricsDropsUnsupportedAndValueless(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	// A histogram (unsupported) with 2 points.
	h := sm.Metrics().AppendEmpty()
	h.SetName("h")
	hp := h.SetEmptyHistogram()
	hp.DataPoints().AppendEmpty()
	hp.DataPoints().AppendEmpty()

	// A gauge with one value-less point and one valid point.
	g := sm.Metrics().AppendEmpty()
	g.SetName("g")
	g.SetEmptyGauge().DataPoints().AppendEmpty() // no value ⇒ dropped
	gp := g.Gauge().DataPoints().AppendEmpty()
	gp.SetDoubleValue(5)

	var out metric.Metrics
	dropped := AppendMetrics(&out, md)
	assert.Equal(t, 3, dropped, "2 histogram points + 1 value-less gauge point")

	// Only the valid gauge point survives projection.
	accepted := metric.Project(out, func(metric.Identity, metric.Sample) {})
	assert.Equal(t, 1, accepted)
}

func TestConvertTypedAttributes(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	g := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("m")
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1)
	a := dp.Attributes()
	a.PutStr("s", "v")
	a.PutInt("i", 7)
	a.PutBool("b", true)
	a.PutDouble("d", 1.5)
	a.PutEmptyBytes("by").FromRaw([]byte{1, 2})
	sl := a.PutEmptySlice("sl")
	sl.AppendEmpty().SetInt(9)

	var out metric.Metrics
	require.Equal(t, 0, AppendMetrics(&out, md))
	at := out.Resources[0].Scopes[0].Metrics[0].Points[0].Attributes

	assertVal := func(key string, want signal.Value) {
		v, ok := at.Get([]byte(key))
		require.Truef(t, ok, "missing %q", key)
		assert.Truef(t, want.Equal(v), "%q", key)
	}
	assertVal("s", signal.StringValue([]byte("v")))
	assertVal("i", signal.IntValue(7))
	assertVal("b", signal.BoolValue(true))
	assertVal("d", signal.DoubleValue(1.5))
	assertVal("by", signal.BytesValue([]byte{1, 2}))
	assertVal("sl", signal.SliceValue(signal.IntValue(9)))
}

func TestAppendMetricsReuseAcrossBatches(t *testing.T) {
	t.Parallel()

	build := func() pmetric.Metrics {
		md := pmetric.NewMetrics()
		dp := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().
			Metrics().AppendEmpty().SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetDoubleValue(1)

		return md
	}

	out := metric.GetMetrics()
	defer metric.PutMetrics(out)

	AppendMetrics(out, build())
	require.Len(t, out.Resources, 1)

	out.Reset()
	AppendMetrics(out, build())
	require.Len(t, out.Resources, 1, "Reset then refill yields a fresh batch")
}

func TestAppendMetricsEmpty(t *testing.T) {
	t.Parallel()

	var out metric.Metrics
	assert.Equal(t, 0, AppendMetrics(&out, pmetric.NewMetrics()))
	assert.Empty(t, out.Resources)
}
