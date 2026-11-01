package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestMemoryRunOwnership(t *testing.T) {
	t.Parallel()
	ts := []int64{-1000, -900, 20, 30}
	values := []float64{1, 2, 3, 4}
	released := 0
	r, err := newMemoryRun([]memoryRunSeries{{id: signal.SeriesID{Lo: 1}, start: 2, end: 4}}, ts, values, nil,
		func() { released++ })
	require.NoError(t, err)
	require.Same(t, &ts[0], &r.ts[0])
	require.Same(t, &values[0], &r.values[0])
	require.Equal(t, int64(2), r.rows)
	require.Equal(t, int64(20), r.minTS)
	require.Equal(t, int64(30), r.maxTS)
	rng, found := r.lookup(signal.SeriesID{Lo: 1})
	require.True(t, found)
	require.Equal(t, rowRange{start: 2, end: 4}, rng)
	_, found = r.lookup(signal.SeriesID{Lo: 2})
	require.False(t, found)
	r.acquire()
	r.release()
	require.Zero(t, released)
	require.NotEmpty(t, r.ts)
	r.release()
	require.Equal(t, 1, released)
	require.Nil(t, r.ts)
	require.Nil(t, r.values)
	require.Panics(t, r.acquire)
}

func TestMemoryRunRejectsInvalidDirectory(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		series     []memoryRunSeries
		ts         []int64
		values, sf []float64
	}{
		{name: "empty"},
		{name: "rejected-only entry", series: []memoryRunSeries{{id: signal.SeriesID{Lo: 1}}}},
		{name: "missing values", series: []memoryRunSeries{{id: signal.SeriesID{Lo: 1}, end: 1}}, ts: []int64{1}},
		{name: "unsorted timestamps", series: []memoryRunSeries{{id: signal.SeriesID{Lo: 1}, end: 2}}, ts: []int64{2, 1}, values: []float64{1, 2}},
		{name: "outside columns", series: []memoryRunSeries{{id: signal.SeriesID{Lo: 1}, end: 2}}, ts: []int64{1}, values: []float64{1}},
		{name: "overlap", series: []memoryRunSeries{{id: signal.SeriesID{Lo: 1}, end: 1}, {id: signal.SeriesID{Lo: 2}, end: 1}}, ts: []int64{1}, values: []float64{1}},
		{name: "duplicate id", series: []memoryRunSeries{{id: signal.SeriesID{Lo: 1}, end: 1}, {id: signal.SeriesID{Lo: 1}, start: 1, end: 2}}, ts: []int64{1, 2}, values: []float64{1, 2}},
		{name: "missing weight", series: []memoryRunSeries{{id: signal.SeriesID{Lo: 1}, end: 2}}, ts: []int64{1, 2}, values: []float64{1, 2}, sf: []float64{1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			released := false
			_, err := newMemoryRun(tt.series, tt.ts, tt.values, tt.sf, func() { released = true })
			require.Error(t, err)
			require.False(t, released, "failed construction leaves ownership with the builder")
		})
	}
}

func testMemoryRun(tb testing.TB, value int64, release func()) *memoryRun {
	tb.Helper()
	r, err := newMemoryRun([]memoryRunSeries{{id: signal.SeriesID{Lo: uint64(value)}, end: 1}},
		[]int64{value}, []float64{float64(value)}, nil, release)
	require.NoError(tb, err)
	return r
}
