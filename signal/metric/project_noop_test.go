package metric

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestProjectNoopFixtureLayoutsAgree(t *testing.T) {
	t.Parallel()
	type sample struct {
		id    signal.SeriesID
		ts    int64
		value uint64
	}
	collect := func(t *testing.T, shape projectNoopShape) map[sample]int {
		t.Helper()
		md := newProjectNoopInput(shape)
		out := make(map[sample]int)
		accepted := Project(md, func(batch *Batch) {
			for i, id := range batch.IDs {
				out[sample{id, batch.Ts[i], math.Float64bits(batch.Values[i])}]++
			}
		})
		require.Equal(t, shape.series*max(shape.samples, 1), accepted)
		return out
	}
	want := collect(t, projectNoopShape{series: 64, samples: 4})
	for _, shape := range []projectNoopShape{
		{name: "per-series-metric", series: 64, samples: 4, perSeries: true},
		{name: "cloned-attributes", series: 64, samples: 4, cloned: true},
		{name: "interleaved-points", series: 64, samples: 4, interleave: true},
		{name: "resource-groups", series: 64, samples: 4, resources: 8, perSeries: true},
		{name: "scope-groups", series: 64, samples: 4, scopes: 8, perSeries: true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, want, collect(t, shape))
		})
	}
}
