package engine

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/timebucket"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestEncodedMergeConcatenatesOrderedRuns(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		timestamps [][]int64
		retain     int64
		copyable   bool
	}{
		{name: "hourly", timestamps: [][]int64{{1, 2, 3, 4}, {5, 6, 7, 8}}, copyable: true},
		{name: "interleaved blocks", timestamps: [][]int64{{1, 2, 7, 8}, {3, 4, 5, 6}}, copyable: true},
		{name: "strictly adjacent", timestamps: [][]int64{{1, 2}, {3, 4}}, copyable: true},
		{name: "overlapping boundary", timestamps: [][]int64{{1, 2, 3, 4}, {4, 5, 6, 7}}},
		{name: "duplicate within granule", timestamps: [][]int64{{1, 1, 3, 4}, {5, 6, 7, 8}}},
		{name: "retention between blocks", timestamps: [][]int64{{1, 2, 3, 4}, {5, 6, 7, 8}}, retain: 3, copyable: true},
		{name: "retention cuts block", timestamps: [][]int64{{1, 2, 3, 4}, {5, 6, 7, 8}}, retain: 4},
		{name: "daily", timestamps: [][]int64{{1, 2}, {int64(24*time.Hour) + 1, int64(24*time.Hour) + 2}}, copyable: true},
		{name: "block crosses day", timestamps: [][]int64{{int64(24*time.Hour) - 1, int64(24 * time.Hour)}, {int64(24*time.Hour) + 1, int64(24*time.Hour) + 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := granuleTestEngine(t, 2)
			s := benchSeries(1)
			for generation, stamps := range tc.timestamps {
				for i, ts := range stamps {
					accepted, err := e.Append(s, ts, float64(100*generation+i)/1000)
					require.NoError(t, err)
					require.True(t, accepted)
				}
				require.NoError(t, e.Flush(t.Context()))
			}
			assertGranulePlan(t, e, s.Hash(), tc.retain, tc.copyable)
			want := granuleRows(t, e, tc.retain)
			dedupGranuleRows(want)
			require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true, RetainFrom: tc.retain}))
			assertGranuleRows(t, want, granuleRows(t, e, tc.retain))
			if e.PartCount() == 1 {
				assertGranuleStats(t, e, want)
			}
			require.NoError(t, e.Close(t.Context()))
			reopened := New(e.cfg)
			t.Cleanup(func() { require.NoError(t, reopened.Close(context.WithoutCancel(t.Context()))) })
			require.NoError(t, reopened.LoadParts(t.Context()))
			assertGranuleRows(t, want, granuleRows(t, reopened, tc.retain))
		})
	}
}

func TestEncodedMergeFallbackResumesAfterCopiedSeries(t *testing.T) {
	t.Parallel()
	e := granuleTestEngine(t, 2)
	identities := []signal.Series{benchSeries(0), benchSeries(1), benchSeries(2), benchSeries(3)}
	slices.SortFunc(identities, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	for generation := range 2 {
		for i, s := range identities {
			for point := range 4 {
				ts := int64(1 + generation*4 + point)
				if i == 1 && generation == 1 {
					ts -= 2
				}
				value := float64(generation*100+i*10+point) / 1000
				if i == 2 {
					value = []float64{1e16, -1e16, 1, math.Copysign(0, -1)}[point]
				}
				if i == 3 {
					value = []float64{math.Float64frombits(0x7ff8000000004321), math.Inf(1), math.Inf(-1), math.Copysign(0, -1)}[point]
				}
				accepted, err := e.Append(s, ts, value)
				require.NoError(t, err)
				require.True(t, accepted)
			}
		}
		require.NoError(t, e.Flush(t.Context()))
	}
	want := granuleRows(t, e, 0)
	require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true}))
	assertGranuleRows(t, want, granuleRows(t, e, 0))
	assertGranuleStats(t, e, want)
}

func TestEncodedMergeSmallerDestinationChunks(t *testing.T) {
	t.Parallel()
	e := granuleTestEngine(t, 4)
	s := benchSeries(1)
	for generation := range 2 {
		for point := range 4 {
			accepted, err := e.Append(s, int64(1+generation*4+point), float64(generation*4+point)/1000)
			require.NoError(t, err)
			require.True(t, accepted)
		}
		require.NoError(t, e.Flush(t.Context()))
	}
	e.cfg.MetricBlockRows = 2
	assertGranulePlan(t, e, s.Hash(), 0, false)
	want := granuleRows(t, e, 0)
	require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true}))
	assertGranuleRows(t, want, granuleRows(t, e, 0))
	assertGranuleStats(t, e, want)
}

func TestEncodedMergeValidatesExpiredValues(t *testing.T) {
	t.Parallel()
	e := granuleTestEngine(t, 2)
	s := benchSeries(1)
	for generation := range 2 {
		for point := range 4 {
			accepted, err := e.Append(s, int64(1+generation*4+point), float64(generation*4+point)/1000)
			require.NoError(t, err)
			require.True(t, accepted)
		}
		require.NoError(t, e.Flush(t.Context()))
	}
	streams, _, err := e.openMetricMergeStreams(t.Context(), e.parts)
	require.NoError(t, err)
	defer func() {
		for _, s := range streams {
			s.close()
		}
	}()
	ranges := make([]rowRange, len(e.parts))
	for i, p := range e.parts {
		ranges[i], _, err = p.index.lookup(t.Context(), s.Hash())
		require.NoError(t, err)
	}
	var plan encodedMergePlan
	ok, err := plan.prepare(streams, ranges, 3, 2)
	require.NoError(t, err)
	require.True(t, ok)
	first := plan.runs[plan.next()].source
	require.NoError(t, streams[first].decoders[1].Close())
	err = plan.write(&mergeIdentityItem{}, streams, &timebucket.Router[*partStreamWriter]{}, 3, nil)
	require.Error(t, err, "expired value chunks must still be read and validated")
	require.Zero(t, streams[first].pos)
}

func granuleTestEngine(t *testing.T, rows int) *Engine {
	t.Helper()
	b, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e := New(Config{Backend: b, Prefix: "granules", DiskSeries: true, MetricSeriesChunks: true,
		MetricBlockRows: rows, AggregateStats: true, FlushMemoryBytes: 128 << 20, MergeMemoryBytes: 128 << 20})
	t.Cleanup(func() { require.NoError(t, e.Close(context.WithoutCancel(t.Context()))) })
	return e
}

func assertGranulePlan(t *testing.T, e *Engine, id signal.SeriesID, start int64, want bool) {
	t.Helper()
	streams, _, err := e.openMetricMergeStreams(t.Context(), e.parts)
	require.NoError(t, err)
	defer func() {
		for _, s := range streams {
			s.close()
		}
	}()
	ranges := make([]rowRange, len(e.parts))
	for i, p := range e.parts {
		var found bool
		ranges[i], found, err = p.index.lookup(t.Context(), id)
		require.NoError(t, err)
		require.True(t, found)
	}
	var plan encodedMergePlan
	ok, err := plan.prepare(streams, ranges, start, e.cfg.MetricBlockRows)
	require.NoError(t, err)
	require.Equal(t, want, ok)
}

func granuleRows(t *testing.T, e *Engine, start int64) []*fetch.Batch {
	t.Helper()
	it, err := e.Fetch(t.Context(), fetch.Request{Start: start, End: maxInt64})
	require.NoError(t, err)
	rows, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.NoError(t, it.Close())
	slices.SortFunc(rows, func(a, b *fetch.Batch) int { return a.Series.Hash().Compare(b.Series.Hash()) })
	return rows
}

func assertGranuleRows(t *testing.T, want, got []*fetch.Batch) {
	t.Helper()
	require.Len(t, got, len(want))
	for i, row := range got {
		require.True(t, want[i].Series.Equal(row.Series))
		require.Equal(t, want[i].Timestamps, row.Timestamps)
		require.Len(t, row.Values, len(want[i].Values))
		for j, v := range row.Values {
			require.Equal(t, math.Float64bits(want[i].Values[j]), math.Float64bits(v))
		}
	}
}

func dedupGranuleRows(rows []*fetch.Batch) {
	for _, row := range rows {
		n := 0
		for i, ts := range row.Timestamps {
			if n > 0 && row.Timestamps[n-1] == ts {
				row.Values[n-1] = row.Values[i]
				continue
			}
			row.Timestamps[n], row.Values[n] = ts, row.Values[i]
			n++
		}
		row.Timestamps, row.Values = row.Timestamps[:n], row.Values[:n]
	}
}

func assertGranuleStats(t *testing.T, e *Engine, rows []*fetch.Batch) {
	t.Helper()
	require.Len(t, e.parts, 1)
	for _, row := range rows {
		var want SeriesAgg
		for _, v := range row.Values {
			want.addSample(v, 1)
		}
		got, ok := e.parts[0].seriesStat(t.Context(), row.Series.Hash())
		require.True(t, ok)
		require.Equal(t, want.Rows, got.Rows)
		for _, pair := range [][2]float64{{want.Count, got.Count}, {want.Sum, got.Sum}, {want.Min, got.Min}, {want.Max, got.Max}} {
			if math.IsNaN(pair[0]) {
				require.True(t, math.IsNaN(pair[1]))
			} else {
				require.Equal(t, math.Float64bits(pair[0]), math.Float64bits(pair[1]))
			}
		}
	}
}
