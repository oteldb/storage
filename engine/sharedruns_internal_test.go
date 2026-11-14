package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
)

func TestOutputRunStorageIsShared(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory()})
	w, err := newPartStreamWriter(t.Context(), e, compressProfile{}, 0, false, true, nil)
	require.NoError(t, err)
	defer w.abort()
	require.NoError(t, w.reserveSeries(1024))
	owned, err := w.w.U128Runs(colSeriesIdx)
	require.NoError(t, err)
	require.Equal(t, cap(owned), cap(w.runs))
	require.EqualValues(t, 1024*(8+40), w.residentBytes()-w.w.ResidentBytes(), "the ID run array is charged once")
	require.NoError(t, w.appendSeries(chunk.U128{Lo: 1}, []int64{1, 2}, []float64{3, 4}, nil, 5))
	require.NoError(t, w.appendSeries(chunk.U128{Lo: 1}, []int64{3}, []float64{5}, nil, 7))
	owned, err = w.w.U128Runs(colSeriesIdx)
	require.NoError(t, err)
	require.Same(t, &owned[0], &w.runs[0])
	require.Equal(t, 3, w.runs[0].Count)
	require.Equal(t, []int64{7}, w.covered)
	require.InDelta(t, 3, w.stats[0].Count, 0)
}
