package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/backendtest"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
)

func TestCountGridSeriesChunkTimestampOnly(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	counter := backendtest.NewSizedByteCounter(files)
	e := New(Config{Backend: counter, Prefix: "timestamp-count", MetricBlockRows: 4, MetricSeriesChunks: true,
		DecodeCacheBytes: 1 << 20, DecodeBudget: NewDecodeBudget(1 << 20)})
	for i := range 32 {
		_, err := e.Append(apiSeries(), int64(i), float64(i*i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.parts, 1)
	part := e.parts[0]
	require.NoError(t, files.Delete(t.Context(), part.prefix+"/c/2"))
	counter.Reset()
	grid := fetch.WindowGrid{Start: 4, Step: 4, Width: 3, NumSteps: 5}
	got, err := e.CountGrid(t.Context(), fetch.Request{Start: 0, End: 40}, grid, nil)
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"": {1, 1, 1, 1, 1}}, got)
	require.Contains(t, counter.Report(), "/c/1", "edge windows inspect timestamps")
	require.NotContains(t, counter.Report(), "/c/2", "no value-column reads")
	require.Zero(t, e.budget.inFlight())
	e.blockCache.evictPrefix(part.prefix)
	require.NoError(t, files.Delete(t.Context(), part.prefix+"/c/1"))
	_, err = e.CountGrid(t.Context(), fetch.Request{Start: 0, End: 40}, grid, nil)
	require.Error(t, err, "unresolved windows still require their timestamp data")
	require.Zero(t, e.budget.inFlight())
	require.Zero(t, part.refs.Load())
}
