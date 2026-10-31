package engine

import (
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestBuildFlushColumns(t *testing.T) {
	t.Parallel()
	a, b := signal.SeriesID{Hi: 1, Lo: 2}, signal.SeriesID{Hi: 2, Lo: 1}
	shared := &sampleBuf{ts: []int64{2, 1}, values: []float64{20, 10}}
	for _, tt := range []struct {
		name            string
		samples         map[signal.SeriesID]*sampleBuf
		ids             []signal.SeriesID
		ts              []int64
		values, weights []float64
	}{
		{name: "nil"},
		{name: "empty buffers", samples: map[signal.SeriesID]*sampleBuf{a: {}}},
		{name: "ordered", samples: map[signal.SeriesID]*sampleBuf{a: {ts: []int64{1, 2, 3}, values: []float64{10, 20, 30}}},
			ids: []signal.SeriesID{a, a, a}, ts: []int64{1, 2, 3}, values: []float64{10, 20, 30}},
		{name: "out of order", samples: map[signal.SeriesID]*sampleBuf{a: {ts: []int64{3, 1, 2}, values: []float64{30, 10, 20}, sf: []float64{3, 1, 2}}},
			ids: []signal.SeriesID{a, a, a}, ts: []int64{1, 2, 3}, values: []float64{10, 20, 30}, weights: []float64{1, 2, 3}},
		{name: "duplicate timestamps", samples: map[signal.SeriesID]*sampleBuf{a: {ts: []int64{2, 1, 1}, values: []float64{20, 11, 12}}},
			ids: []signal.SeriesID{a, a, a}, ts: []int64{1, 1, 2}, values: []float64{11, 12, 20}},
		{name: "series order and late weights", samples: map[signal.SeriesID]*sampleBuf{
			b: {ts: []int64{3, 4}, values: []float64{30, 40}, sf: []float64{1, 5}},
			a: {ts: []int64{1, 2}, values: []float64{10, 20}}},
			ids: []signal.SeriesID{a, a, b, b}, ts: []int64{1, 2, 3, 4}, values: []float64{10, 20, 30, 40}, weights: []float64{1, 1, 1, 5}},
		{name: "unweighted suffix", samples: map[signal.SeriesID]*sampleBuf{
			a: {ts: []int64{1}, values: []float64{10}, sf: []float64{2}}, b: {ts: []int64{2}, values: []float64{20}}},
			ids: []signal.SeriesID{a, b}, ts: []int64{1, 2}, values: []float64{10, 20}, weights: []float64{2, 1}},
		{name: "explicit unit weights", samples: map[signal.SeriesID]*sampleBuf{a: {ts: []int64{1, 2}, values: []float64{10, 20}, sf: []float64{1, 1}}},
			ids: []signal.SeriesID{a, a}, ts: []int64{1, 2}, values: []float64{10, 20}},
		{name: "shared source buffers", samples: map[signal.SeriesID]*sampleBuf{a: shared, b: shared},
			ids: []signal.SeriesID{a, a, b, b}, ts: []int64{1, 2, 1, 2}, values: []float64{10, 20, 10, 20}},
		{name: "timestamp extremes", samples: map[signal.SeriesID]*sampleBuf{a: {ts: []int64{math.MaxInt64, math.MinInt64}, values: []float64{20, 10}}},
			ids: []signal.SeriesID{a, a}, ts: []int64{math.MinInt64, math.MaxInt64}, values: []float64{10, 20}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			before := make(map[signal.SeriesID]sampleBuf, len(tt.samples))
			for id, buf := range tt.samples {
				before[id] = sampleBuf{ts: slices.Clone(buf.ts), values: slices.Clone(buf.values), sf: slices.Clone(buf.sf)}
			}
			got := buildFlushColumns(tt.samples)
			if len(tt.ids) == 0 {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tt.ts, got.ts)
			assert.Equal(t, tt.values, got.value)
			assert.Equal(t, tt.weights, got.sf)
			for i, id := range tt.ids {
				assert.Equal(t, idToU128(id), got.series[i])
			}
			for id, buf := range tt.samples {
				assert.Equal(t, before[id], *buf, "builder must not mutate detached buffers")
			}
			got.ts[0]++
			got.value[0]++
			if got.sf != nil {
				got.sf[0]++
			}
			for id, buf := range tt.samples {
				assert.Equal(t, before[id], *buf, "output must own its backing arrays")
			}
		})
	}
}

func TestBuildFlushColumnsPreservesFloatBits(t *testing.T) {
	t.Parallel()
	values := []float64{math.Float64frombits(1 << 63), math.Float64frombits(0x7ff8000000000123), math.Inf(1), math.Inf(-1)}
	got := buildFlushColumns(map[signal.SeriesID]*sampleBuf{{}: {ts: []int64{1, 2, 3, 4}, values: values}})
	for i, v := range values {
		assert.Equal(t, math.Float64bits(v), math.Float64bits(got.value[i]))
	}
}
