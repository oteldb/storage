package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

func TestMemoryCoalescingPreservesAppendsAndOldReaders(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	enableMemoryPublicationTest(t, e)
	s := apiSeries()
	for value := range 300 {
		_, err := e.Append(s, 10, float64(value))
		require.NoError(t, err)
	}
	require.Equal(t, 300, e.readView.active.runs)
	old, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 20})
	require.NoError(t, err)
	e.coalescePrepared = func() {
		_, err := e.Append(s, 10, 300)
		require.NoError(t, err, "independent publication proceeds after coalescing preparation")
	}
	changed, err := e.coalesceMemoryOnce(t.Context())
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, 47, e.readView.active.runs)
	require.Equal(t, int64(301), e.readView.active.rows)
	require.Equal(t, int64(301), e.Stats().HeadSamples)
	prior, err := old.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []float64{299}, prior.Values)
	require.NoError(t, old.Close())
	current, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 20})
	require.NoError(t, err)
	batch, err := current.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []float64{300}, batch.Values)
	require.NoError(t, current.Close())
	agg, err := e.AggregateRange(t.Context(), fetch.Request{Start: 0, End: 20})
	require.NoError(t, err)
	require.Equal(t, int64(1), agg[s.Hash()].Rows)
	require.InDelta(t, 300, agg[s.Hash()].Sum, 0)
}

func TestMemoryCoalescingCancellation(t *testing.T) {
	t.Parallel()
	run := testMemoryRun(t, 1, nil)
	path := newMemoryDirectoryPath(nil)
	root := path.insert(nil, 1, run)
	path.close()
	run.release()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := mergeMemoryCohort(ctx, root)
	require.ErrorIs(t, err, context.Canceled)
	root.release()
}
