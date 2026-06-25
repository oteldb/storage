package metric

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestBuilderShape(t *testing.T) {
	t.Parallel()

	var md Metrics
	rm := md.AddResource()
	rm.Resource = signal.Resource{SchemaURL: []byte("schema")}
	sm := rm.AddScope()
	sm.Scope = signal.Scope{Name: []byte("lib")}
	mt := sm.AddMetric()
	mt.Name = []byte("m")
	mt.Kind = KindSum
	mt.Temporality = TemporalityDelta
	p := mt.AddPoint()
	p.Ts = 7
	p.Value = 1.5

	require.Len(t, md.Resources, 1)
	require.Len(t, md.Resources[0].Scopes, 1)
	require.Len(t, md.Resources[0].Scopes[0].Metrics, 1)
	require.Len(t, md.Resources[0].Scopes[0].Metrics[0].Points, 1)
	assert.Equal(t, []byte("schema"), md.Resources[0].Resource.SchemaURL)
	assert.Equal(t, []byte("lib"), md.Resources[0].Scopes[0].Scope.Name)
	assert.Equal(t, int64(7), md.Resources[0].Scopes[0].Metrics[0].Points[0].Ts)
}

func TestResetRetainsCapacity(t *testing.T) {
	t.Parallel()

	var md Metrics
	rm := md.AddResource()
	sm := rm.AddScope()
	mt := sm.AddMetric()
	mt.AddPoint()
	mt.AddPoint()

	md.Reset()
	assert.Empty(t, md.Resources)
	assert.GreaterOrEqual(t, cap(md.Resources), 1, "Reset retains the resource backing array")

	// Rebuilding reuses the retained nested slices: a fresh metric starts empty.
	mt2 := md.AddResource().AddScope().AddMetric()
	assert.Empty(t, mt2.Points)
	assert.GreaterOrEqual(t, cap(mt2.Points), 2, "the point backing array is recycled")
}

//nolint:paralleltest // testing.AllocsPerRun must not run during a parallel test.
func TestBuilderReuseZeroAlloc(t *testing.T) {
	name := []byte("m")
	m := &Metrics{}
	build := func() {
		m.Reset()
		for range 2 {
			rm := m.AddResource()
			for range 2 {
				sm := rm.AddScope()
				for range 2 {
					mt := sm.AddMetric()
					mt.Name = name
					mt.Kind = KindGauge
					for pi := range 4 {
						p := mt.AddPoint()
						p.Ts = int64(pi)
						p.Value = float64(pi)
					}
				}
			}
		}
	}

	build() // warm up: grow every backing array once

	allocs := testing.AllocsPerRun(100, build)
	assert.Zero(t, allocs, "rebuild after Reset must not allocate")
}

func TestPoolRoundTrip(t *testing.T) {
	t.Parallel()

	m := GetMetrics()
	m.AddResource().AddScope().AddMetric().AddPoint()
	require.Len(t, m.Resources, 1)
	PutMetrics(m)

	m2 := GetMetrics()
	assert.Empty(t, m2.Resources, "a pooled batch comes back reset")
	PutMetrics(m2)
}
