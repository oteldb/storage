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
	assert.Equal(t, Dropped{}, dropped)

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

func TestAppendMetricsDropsOnlyValueless(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	// A histogram with 2 (empty) points: each decomposes to a `_count` series — not dropped.
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
	assert.Equal(t, Dropped{Points: 1}, dropped, "only the value-less gauge point is dropped")

	// The two histogram _count series plus the valid gauge survive projection.
	accepted := metric.Project(out, func(*metric.Batch) {})
	assert.Equal(t, 3, accepted)
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
	require.Equal(t, Dropped{}, AppendMetrics(&out, md))
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
	assert.Equal(t, Dropped{}, AppendMetrics(&out, pmetric.NewMetrics()))
	assert.Empty(t, out.Resources)
}

// addExemplar attaches one double-valued exemplar with trace context to dp.
func addExemplar(dp pmetric.NumberDataPoint, value float64, ts int64) pmetric.Exemplar {
	e := dp.Exemplars().AppendEmpty()
	e.SetDoubleValue(value)
	e.SetTimestamp(pcommon.Timestamp(ts))
	e.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	e.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
	e.FilteredAttributes().PutStr("peer.service", "db")

	return e
}

func TestAppendMetricsConvertsNumberExemplars(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	g := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("g")
	g.SetEmptyGauge()

	dp := g.Gauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1)
	dp.SetTimestamp(1000)
	addExemplar(dp, 12.5, 900)

	// An int-valued exemplar widens to float64, like an int point does.
	ei := dp.Exemplars().AppendEmpty()
	ei.SetIntValue(7)
	ei.SetTimestamp(950)

	var out metric.Metrics

	require.Equal(t, Dropped{}, AppendMetrics(&out, md))

	pts := out.Resources[0].Scopes[0].Metrics[0].Points
	require.Len(t, pts, 1)
	require.Len(t, pts[0].Exemplars, 2)

	first := pts[0].Exemplars[0]
	assert.InDelta(t, 12.5, first.Value, 0)
	assert.Equal(t, int64(900), first.Ts)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, first.TraceID)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, first.SpanID)

	peer, ok := first.FilteredAttributes.Get([]byte("peer.service"))
	require.True(t, ok)
	assert.Equal(t, "db", string(peer.Str()))

	second := pts[0].Exemplars[1]
	assert.InDelta(t, 7.0, second.Value, 0)
	assert.Empty(t, second.TraceID, "an exemplar with no trace context keeps empty ids")
	assert.Empty(t, second.SpanID)
}

// Histogram exemplars are rejected, not attached to an arbitrary decomposed series. This pins the
// deliberate choice — see the package doc and issue #252.
func TestAppendMetricsDropsHistogramExemplars(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	h := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	h.SetName("h")
	h.SetEmptyHistogram()
	h.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := h.Histogram().DataPoints().AppendEmpty()
	dp.SetCount(3)
	dp.SetSum(6)
	dp.ExplicitBounds().FromRaw([]float64{1, 2})
	dp.BucketCounts().FromRaw([]uint64{1, 1, 1})

	for range 2 {
		e := dp.Exemplars().AppendEmpty()
		e.SetDoubleValue(1.5)
	}

	var out metric.Metrics

	assert.Equal(t, Dropped{Exemplars: 2}, AppendMetrics(&out, md),
		"histogram exemplars are counted as dropped, not folded into the point tally")

	// The decomposition still happened, and none of its series carries an exemplar.
	var withExemplars int

	for _, mt := range out.Resources[0].Scopes[0].Metrics {
		for _, p := range mt.Points {
			withExemplars += len(p.Exemplars)
		}
	}

	assert.Zero(t, withExemplars)
	assert.NotEmpty(t, out.Resources[0].Scopes[0].Metrics, "decomposition still produced series")
}

// A value-less point takes its exemplars down with it, and they are counted separately from the
// point so an OTLP partial-success response stays accurate.
func TestAppendMetricsValuelessPointDropsItsExemplars(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	g := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("g")
	g.SetEmptyGauge()

	dp := g.Gauge().DataPoints().AppendEmpty() // no value set
	addExemplar(dp, 1, 1)

	var out metric.Metrics

	assert.Equal(t, Dropped{Points: 1, Exemplars: 1}, AppendMetrics(&out, md))
}

// A value-less exemplar on a good point drops only itself.
func TestAppendMetricsDropsValuelessExemplar(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	g := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("g")
	g.SetEmptyGauge()

	dp := g.Gauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1)
	dp.Exemplars().AppendEmpty() // no value

	var out metric.Metrics

	assert.Equal(t, Dropped{Exemplars: 1}, AppendMetrics(&out, md))
	assert.Empty(t, out.Resources[0].Scopes[0].Metrics[0].Points[0].Exemplars)
}
