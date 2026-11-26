package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

func TestMergeWindowTiesAndWatermarks(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "windows", DiskSeries: true, MetricSeriesChunks: true})
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	const rows = mergeSmallSeries + mergeSeriesWindow
	series := benchSeries(1)
	for source := range 2 {
		for i := range rows {
			_, err := e.Append(series, int64(i+1), float64(i+rows*source))
			require.NoError(t, err)
		}
		require.NoError(t, e.Flush(t.Context()))
	}
	require.Equal(t, 2, e.PartCount())
	require.NoError(t, e.MergeWith(t.Context(), MergeOptions{RetainFrom: mergeSeriesWindow, Force: true}))
	out, err := e.Fetch(t.Context(), fetch.Request{Start: 1, End: rows})
	require.NoError(t, err)
	data, err := fetch.Drain(t.Context(), out)
	require.NoError(t, err)
	require.Len(t, data, 1)
	require.Len(t, data[0].Values, rows-mergeSeriesWindow+1)
	for i, ts := range data[0].Timestamps {
		require.Equal(t, int64(i+mergeSeriesWindow), ts)
		require.Equal(t, float64(int(ts)-1+rows), data[0].Values[i])
	}
}
