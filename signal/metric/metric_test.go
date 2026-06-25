package metric

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/signal"
)

type emitted struct {
	id Identity
	s  Sample
}

func collect(md pmetric.Metrics) ([]emitted, int, int) {
	var out []emitted
	a, r := Project(md, func(id Identity, s Sample) {
		out = append(out, emitted{id.clone(), s})
	})

	return out, a, r
}

func (id Identity) clone() Identity {
	id.Series = id.Series.Clone()
	id.Name = append([]byte(nil), id.Name...)
	id.Unit = append([]byte(nil), id.Unit...)

	return id
}

// newGauge builds a one-metric batch: resource service.name, scope lib, a gauge with the
// given (route, value) data points.
func newGauge(tb testing.TB, name string, points ...any) pmetric.Metrics {
	tb.Helper()
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "api")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("lib")
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	m.SetUnit("1")
	g := m.SetEmptyGauge()

	for i := 0; i+1 < len(points); i += 2 {
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(1000 + i))
		dp.SetDoubleValue(points[i+1].(float64))
		dp.Attributes().PutStr("route", points[i].(string))
	}

	return md
}

func TestProjectGauge(t *testing.T) {
	t.Parallel()

	md := newGauge(t, "http.requests", "/x", 1.0, "/y", 2.0)
	got, accepted, rejected := collect(md)

	require.Len(t, got, 2)
	assert.Equal(t, 2, accepted)
	assert.Equal(t, 0, rejected)

	assert.Equal(t, []byte("http.requests"), got[0].id.Name)
	assert.Equal(t, KindGauge, got[0].id.Kind)
	assert.InDelta(t, 1.0, got[0].s.Value, 0)
	assert.Equal(t, int64(1000), got[0].s.Ts)

	// resource, scope and point attr are all in the identity.
	rv, _ := got[0].id.Series.Resource.Attributes.Get([]byte("service.name"))
	assert.Equal(t, []byte("api"), rv.Str())
	assert.Equal(t, []byte("lib"), got[0].id.Series.Scope.Name)
	pv, _ := got[0].id.Series.Attributes.Get([]byte("route"))
	assert.Equal(t, []byte("/x"), pv.Str())

	// distinct routes ⇒ distinct ids.
	assert.NotEqual(t, got[0].id.SeriesID(), got[1].id.SeriesID())
}

func TestProjectSumCarriesTemporalityAndMonotonic(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("bytes.total")
	s := m.SetEmptySum()
	s.SetIsMonotonic(true)
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := s.DataPoints().AppendEmpty()
	dp.SetIntValue(42)
	dp.SetTimestamp(pcommon.Timestamp(5))

	got, accepted, _ := collect(md)
	require.Len(t, got, 1)
	assert.Equal(t, 1, accepted)
	assert.Equal(t, KindSum, got[0].id.Kind)
	assert.Equal(t, TemporalityCumulative, got[0].id.Temporality)
	assert.True(t, got[0].id.Monotonic)
	assert.InDelta(t, 42.0, got[0].s.Value, 0, "int value widened to float")
}

func TestIdentityDistinguishesMetricFields(t *testing.T) {
	t.Parallel()

	base := Identity{Series: signal.Series{}, Name: []byte("m"), Unit: []byte("s"), Kind: KindGauge}

	variants := map[string]func(*Identity){
		"name":        func(id *Identity) { id.Name = []byte("other") },
		"unit":        func(id *Identity) { id.Unit = []byte("ms") },
		"kind":        func(id *Identity) { id.Kind = KindSum },
		"temporality": func(id *Identity) { id.Temporality = TemporalityDelta },
		"monotonic":   func(id *Identity) { id.Monotonic = true },
	}

	seen := map[signal.SeriesID]string{base.SeriesID(): "base"}
	for name, mut := range variants {
		v := base
		mut(&v)
		h := v.SeriesID()
		if prev, ok := seen[h]; ok {
			t.Fatalf("identity collision: %s == %s", name, prev)
		}

		seen[h] = name
	}
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

	got, _, _ := collect(md)
	require.Len(t, got, 1)
	at := got[0].id.Series.Attributes

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

func TestUnsupportedAndEmptyRejected(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	// A histogram (unsupported) with 2 points.
	h := sm.Metrics().AppendEmpty()
	h.SetName("h")
	hp := h.SetEmptyHistogram()
	hp.DataPoints().AppendEmpty()
	hp.DataPoints().AppendEmpty()

	// A gauge with one value-less point.
	g := sm.Metrics().AppendEmpty()
	g.SetName("g")
	g.SetEmptyGauge().DataPoints().AppendEmpty() // no value set ⇒ Empty

	got, accepted, rejected := collect(md)
	assert.Empty(t, got)
	assert.Equal(t, 0, accepted)
	assert.Equal(t, 3, rejected, "2 histogram points + 1 value-less gauge point")
}

func TestToSeriesFoldsReservedLabels(t *testing.T) {
	t.Parallel()

	id := Identity{
		Series:      signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("route"), Value: signal.StringValue([]byte("/x"))})},
		Name:        []byte("http.requests"),
		Unit:        []byte("1"),
		Kind:        KindSum,
		Temporality: TemporalityDelta,
		Monotonic:   true,
	}
	s := id.ToSeries()

	get := func(key []byte) signal.Value {
		v, ok := s.Attributes.Get(key)
		require.Truef(t, ok, "missing %s", key)

		return v
	}
	assert.Equal(t, []byte("http.requests"), get(LabelName).Str())
	assert.Equal(t, []byte("1"), get(LabelUnit).Str())
	assert.Equal(t, int64(KindSum), get(LabelKind).Int())
	assert.Equal(t, int64(TemporalityDelta), get(LabelTemporality).Int())
	assert.True(t, get(LabelMonotonic).Bool())
	// The original point attribute is preserved.
	assert.Equal(t, []byte("/x"), get([]byte("route")).Str())
	// SeriesID is ToSeries hashed.
	assert.Equal(t, s.Hash(), id.SeriesID())
}

func TestProjectEmptyBatch(t *testing.T) {
	t.Parallel()

	got, accepted, rejected := collect(pmetric.NewMetrics())
	assert.Empty(t, got)
	assert.Equal(t, 0, accepted)
	assert.Equal(t, 0, rejected)
}
