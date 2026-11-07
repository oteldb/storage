package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

func TestSeriesMergeRanges(t *testing.T) {
	t.Parallel()
	rows := []chunk.U128{{Lo: 2}, {Lo: 3}, {Lo: 3}, {Lo: 3}}
	entries, n, total, err := parseSeriesIndex(encodeSeriesIndex(rows), true)
	require.NoError(t, err)
	paged := &pagedIndex{n: n, total: total}
	paged.view.Store(&entries)
	parts := []*part{
		{index: partIndex{ids: []signal.SeriesID{{Lo: 1}, {Lo: 3}}, starts: []int32{0, 2, 5}}},
		{index: partIndex{paged: paged}},
		{index: partIndex{}},
	}
	var keys seriesMerge
	require.NoError(t, keys.init(t.Context(), parts))
	// The iterator owns the captured entries even after the index drops its cached view.
	paged.view.Store(nil)
	for _, tc := range []struct {
		id     uint64
		ranges []rowRange
	}{
		{1, []rowRange{{0, 2}, {}, {}}},
		{2, []rowRange{{}, {0, 1}, {}}},
		{3, []rowRange{{2, 5}, {1, 4}, {}}},
	} {
		require.True(t, keys.Next())
		require.Equal(t, signal.SeriesID{Lo: tc.id}, keys.Key())
		require.Equal(t, tc.ranges, keys.ranges)
	}
	require.False(t, keys.Next())
	require.Equal(t, []rowRange{{}, {}, {}}, keys.ranges)
}

func TestMergeIdentityBatchRetainsRanges(t *testing.T) {
	t.Parallel()
	const count = 600
	parts := make([]*part, 2)
	for source := range parts {
		ids := make([]signal.SeriesID, count)
		starts := make([]int32, count+1)
		for i := range ids {
			ids[i] = signal.SeriesID{Lo: uint64(i*2 + source)}
			starts[i+1] = starts[i] + int32(i%7+1)
		}
		parts[source] = &part{index: partIndex{ids: ids, starts: starts}}
	}
	var keys seriesMerge
	require.NoError(t, keys.init(t.Context(), parts))
	var batch mergeIdentityBatch
	e := New(Config{})
	visited := 0
	for batch.next(e, &keys) {
		i := batch.at - 1
		id := batch.ids[i]
		require.Equal(t, signal.SeriesID{Lo: uint64(visited)}, id)
		for source, p := range parts {
			want, found, err := p.index.lookup(t.Context(), id)
			require.NoError(t, err)
			if !found {
				want = rowRange{}
			}
			require.Equal(t, want, batch.ranges[i*len(parts)+source])
		}
		visited++
	}
	require.Equal(t, count*len(parts), visited)
}
