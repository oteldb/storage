package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func TestPartIndexMetadataBounds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		idx  partIndex
	}{
		{name: "resident", idx: partIndex{ids: []signal.SeriesID{{Lo: 10}, {Lo: 20}}}},
		{name: "paged", idx: partIndex{paged: &pagedIndex{n: 2, bounded: true, first: signal.SeriesID{Lo: 10}, last: signal.SeriesID{Lo: 20}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, value := range []uint64{9, 10, 15, 20, 21} {
				require.Equal(t, value >= 10 && value <= 20, tc.idx.mayContainID(signal.SeriesID{Lo: value}))
			}
		})
	}
	require.False(t, (partIndex{}).mayContainID(signal.SeriesID{}))
	require.True(t, (partIndex{paged: &pagedIndex{n: 1}}).mayContainID(signal.SeriesID{}))
}

func TestDiskHistoricalBatchPreservesNewest(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "batch-watermarks", DiskSeries: true, OOOWindow: 1})
	a, b := diskQuerySeries(signal.StringValue([]byte("a"))), diskQuerySeries(signal.StringValue([]byte("b")))
	for generation := range 3 {
		for _, s := range []signal.Series{a, b} {
			_, err := e.Append(s, int64(10+generation*10), 1)
			require.NoError(t, err)
		}
		require.NoError(t, e.Flush(t.Context()))
	}
	clear(e.head.disk.cache)
	result, err := e.AppendBatch([]signal.SeriesID{b.Hash(), a.Hash(), b.Hash()}, []int64{29, 28, 31}, []float64{1, 1, 1}, nil,
		func(i int) signal.Series {
			if i == 1 {
				return a
			}
			return b
		}, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 2, result.Accepted)
	require.Equal(t, 1, result.RejectedOOO)
	require.EqualValues(t, 2, e.Stats().Series)
	for _, pt := range e.parts {
		meta, found, err := pt.diskSeriesMetadata(t.Context(), signal.SeriesID{})
		require.NoError(t, err)
		require.False(t, found)
		require.Zero(t, meta)
	}
}
