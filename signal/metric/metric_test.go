package metric

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

type emitted struct {
	sid signal.SeriesID
	id  Identity
	s   Sample
}

func collect(tb testing.TB, md Metrics) ([]emitted, int) {
	tb.Helper()

	var out []emitted
	a := Project(md, func(sid signal.SeriesID, id *Identity, s Sample) {
		// The hot-path id must equal the materialized identity's hash on every point.
		require.Equal(tb, id.SeriesID(), sid)
		out = append(out, emitted{sid, id.clone(), s})
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
	got, accepted := collect(t, md)

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

	got, accepted := collect(t, md)
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

	got, _ := collect(t, md)
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

	got, accepted := collect(t, Metrics{})
	assert.Empty(t, got)
	assert.Equal(t, 0, accepted)
}

// TestProjectIDHandlesReservedCollision pins the trickiest merge case: a point attribute
// whose key collides with a reserved label (__name__). The hoisted-merge id must still
// equal the materialized identity's hash, with the point attribute ordered before the
// reserved one (stable-sort semantics).
func TestProjectIDHandlesReservedCollision(t *testing.T) {
	t.Parallel()

	var md Metrics
	mt := md.AddResource().AddScope().AddMetric()
	mt.Name = []byte("real.name")
	mt.Kind = KindGauge
	p := mt.AddPoint()
	p.Attributes = signal.NewAttributes(
		signal.KeyValue{Key: LabelName, Value: signal.StringValue([]byte("shadow"))}, // collides with reserved
		signal.KeyValue{Key: []byte("route"), Value: signal.StringValue([]byte("/x"))},
	)

	got, _ := collect(t, md) // collect asserts sid == id.SeriesID() per point
	require.Len(t, got, 1)
	assert.Equal(t, got[0].id.ToSeries().Hash(), got[0].sid)
}

// FuzzProjectIDMatchesToSeries fuzzes the hot-path id against the reference materialization
// (Identity.ToSeries hashed) over arbitrary point attributes, including keys that collide
// with reserved labels and with each other.
func FuzzProjectIDMatchesToSeries(f *testing.F) {
	f.Add([]byte("route"), []byte("/x"), []byte("__name__"), []byte("api"), uint8(1))
	f.Add([]byte("a"), []byte("1"), []byte("a"), []byte("2"), uint8(0))
	f.Add([]byte(""), []byte(""), []byte("z"), []byte(""), uint8(3))

	f.Fuzz(func(t *testing.T, k1, v1, k2, v2 []byte, kind uint8) {
		var md Metrics
		mt := md.AddResource().AddScope().AddMetric()
		mt.Name = []byte("m")
		mt.Unit = []byte("u")
		mt.Kind = PointKind(kind % 2)
		mt.Temporality = Temporality(kind % 3)
		mt.Monotonic = kind&1 == 1
		p := mt.AddPoint()
		p.Attributes = signal.NewAttributes(
			signal.KeyValue{Key: k1, Value: signal.StringValue(v1)},
			signal.KeyValue{Key: k2, Value: signal.BytesValue(v2)},
		)

		Project(md, func(sid signal.SeriesID, id *Identity, _ Sample) {
			require.Equal(t, id.ToSeries().Hash(), sid)
		})
	})
}
