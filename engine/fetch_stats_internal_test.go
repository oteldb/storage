package engine

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
)

type statsCountingFile struct {
	*file.File

	stats atomic.Int64
}

func (b *statsCountingFile) Read(ctx context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, "/stats") {
		b.stats.Add(1)
	}
	return b.File.Read(ctx, key)
}

func TestFetchSkipsSharedAggregateStats(t *testing.T) {
	t.Parallel()
	f, err := file.New(t.TempDir())
	require.NoError(t, err)
	be := &statsCountingFile{File: f}
	e := New(Config{Backend: be, Prefix: "fetch-stats", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true, AggregateStats: true})
	t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
	identity := benchSeries(0)
	for i := range 3 {
		_, err := e.Append(identity, int64(100+i), float64(i+1))
		require.NoError(t, err)
		_, err = e.Append(benchSeries(1), int64(100+i), 100)
		require.NoError(t, err)
		require.NoError(t, e.Flush(t.Context()))
	}
	be.stats.Store(0)
	r := fetch.Request{Start: 0, End: 1000}
	it, err := e.Fetch(t.Context(), r)
	require.NoError(t, err)
	batches, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.NoError(t, it.Close())
	require.Len(t, batches, 2)
	for _, batch := range batches {
		require.Equal(t, []int64{100, 101, 102}, batch.Timestamps)
		if batch.ID == identity.Hash() {
			require.Equal(t, []float64{1, 2, 3}, batch.Values)
			require.True(t, batch.Series.Equal(identity))
		} else {
			require.Equal(t, []float64{100, 100, 100}, batch.Values)
		}
	}
	require.Zero(t, be.stats.Load(), "sample fetch must not read aggregate sidecars")
	ids, plan, err := e.planAggregate(t.Context(), r)
	require.NoError(t, err)
	defer plan.releaseParts()
	require.Len(t, ids, 2)
	require.Positive(t, be.stats.Load(), "aggregate planning must retain optional statistics")
	for _, p := range plan.liveParts {
		stats, ok := plan.seriesStat(t.Context(), p, identity.Hash())
		require.True(t, ok)
		require.InDelta(t, 1, stats.Count, 0)
	}
}
