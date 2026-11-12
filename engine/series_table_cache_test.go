package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

func TestSeparateSeriesCacheSharesBudget(t *testing.T) {
	t.Parallel()
	const budget = 64 << 10
	e := New(Config{Backend: backend.Memory(), Prefix: "cache-budget", DiskSeries: true, MetricSeriesChunks: true, SeparateSeries: true, SeriesCacheBytes: budget})
	for i := range 128 {
		ok, err := e.Append(benchSeries(i), 100, 1)
		require.NoError(t, err)
		require.True(t, ok)
	}
	require.NoError(t, e.Flush(t.Context()))
	retained := e.seriesIDs.SizeBytes()
	require.Greater(t, retained, int64(128*16))
	require.LessOrEqual(t, retained, int64(budget/8))
	require.LessOrEqual(t, e.head.disk.cache.sizeBytes(), int64(budget-budget/8))
	require.GreaterOrEqual(t, e.IdentityBytes(), retained+e.head.disk.cache.sizeBytes())
	e.mu.Lock()
	err := e.rebuildDiskStateLocked(t.Context())
	e.mu.Unlock()
	require.NoError(t, err)
	require.LessOrEqual(t, e.head.disk.cache.sizeBytes(), int64(budget-budget/8))
	require.NoError(t, e.Close(t.Context()))
	require.Less(t, e.seriesIDs.SizeBytes(), retained)
}
