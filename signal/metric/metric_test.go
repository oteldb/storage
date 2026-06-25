package metric

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

type emitted struct {
	id Identity
	s  Sample
}

func collect(md Metrics) ([]emitted, int) {
	var out []emitted
	a := Project(md, func(id Identity, s Sample) {
		out = append(out, emitted{id.clone(), s})
	})

	return out, a
}

func (id Identity) clone() Identity {
	id.Series = id.Series.Clone()
	id.Name = append([]byte(nil), id.Name...)
	id.Unit = append([]byte(nil), id.Unit...)

	return id
}

// newGauge builds a one-metric batch: resource service.name=api, scope lib, a gauge with
// the given (route, value) data points.
func newGauge(name string, points ...any) Metrics {
	var md Metrics
	rm := md.AddResource()
	rm.Resource = signal.Resource{
		Attributes: signal.NewAttributes(signal.KeyValue{
			Key: []byte("service.name"), Value: signal.StringValue([]byte("api")),
		}),
	}
	sm := rm.AddScope()
	sm.Scope = signal.Scope{Name: []byte("lib")}
	mt := sm.AddMetric()
	mt.Name = []byte(name)
	mt.Unit = []byte("1")
	mt.Kind = KindGauge

	for i := 0; i+1 < len(points); i += 2 {
		p := mt.AddPoint()
		p.Ts = int64(1000 + i)
		p.Value = points[i+1].(float64)
		p.Attributes = signal.NewAttributes(signal.KeyValue{
			Key: []byte("route"), Value: signal.StringValue([]byte(points[i].(string))),
		})
	}

	return md
}

func TestProjectGauge(t *testing.T) {
	t.Parallel()

	md := newGauge("http.requests", "/x", 1.0, "/y", 2.0)
	got, accepted := collect(md)

	require.Len(t, got, 2)
	assert.Equal(t, 2, accepted)

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

	var md Metrics
	mt := md.AddResource().AddScope().AddMetric()
	mt.Name = []byte("bytes.total")
	mt.Kind = KindSum
	mt.Temporality = TemporalityCumulative
	mt.Monotonic = true
	p := mt.AddPoint()
	p.Ts = 5
	p.Value = 42

	got, accepted := collect(md)
	require.Len(t, got, 1)
	assert.Equal(t, 1, accepted)
	assert.Equal(t, KindSum, got[0].id.Kind)
	assert.Equal(t, TemporalityCumulative, got[0].id.Temporality)
	assert.True(t, got[0].id.Monotonic)
	assert.InDelta(t, 42.0, got[0].s.Value, 0)
}

func TestProjectCarriesPointAttributes(t *testing.T) {
	t.Parallel()

	var md Metrics
	mt := md.AddResource().AddScope().AddMetric()
	mt.Name = []byte("m")
	mt.Kind = KindGauge
	p := mt.AddPoint()
	p.Value = 1
	p.Attributes = signal.NewAttributes(
		signal.KeyValue{Key: []byte("s"), Value: signal.StringValue([]byte("v"))},
		signal.KeyValue{Key: []byte("i"), Value: signal.IntValue(7)},
	)

	got, _ := collect(md)
	require.Len(t, got, 1)
	at := got[0].id.Series.Attributes
	sv, ok := at.Get([]byte("s"))
	require.True(t, ok)
	assert.Equal(t, []byte("v"), sv.Str())
	iv, ok := at.Get([]byte("i"))
	require.True(t, ok)
	assert.Equal(t, int64(7), iv.Int())
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

	got, accepted := collect(Metrics{})
	assert.Empty(t, got)
	assert.Equal(t, 0, accepted)
}
