package engine

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestWarmPreparationCapacityAndCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		pool := newBatchPreparations()
		owned := make([]*preparedBatch, maxPreparedBatches)
		for i := range owned {
			owned[i] = pool.acquireUntil(nil)
			require.NotNil(t, owned[i])
		}
		cancel := make(chan struct{})
		result := make(chan *preparedBatch, 1)
		go func() { result <- pool.acquireUntil(cancel) }()
		synctest.Wait()
		select {
		case <-result:
			t.Fatal("preparation exceeded the configured concurrency")
		default:
		}
		close(cancel)
		require.Nil(t, <-result)
		for _, batch := range owned {
			pool.release(batch)
		}
		for range maxPreparedBatches * 2 {
			batch := pool.acquireUntil(nil)
			require.NotNil(t, batch)
			pool.release(batch)
		}
		require.Empty(t, pool.active)
	})
}

func TestMemoryCapacityRejectsUnmergeableRunsAndRecovers(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	enableMemoryPublicationTest(t, e)
	e.memoryLife = newMemoryLifecycle()
	t.Cleanup(e.stopMemoryWork)
	for key := 1; key <= maxActiveMemoryRuns; key++ {
		run := testMemoryRun(t, int64(key), nil)
		// Exercise the admission metadata without allocating oversized numeric columns.
		run.bytes = maxMemoryCoalesceBytes + 1
		path := newMemoryDirectoryPath(nil)
		next := path.insert(e.readView.active, uint64(key), run)
		path.close()
		e.readView.active.release()
		e.readView.active = next
		run.release()
	}
	e.runSequence = maxActiveMemoryRuns
	require.ErrorIs(t, e.awaitMemoryCapacity(), errMemoryCapacity)
	require.NoError(t, e.Reset(t.Context()))
	require.NoError(t, e.awaitMemoryCapacity(), "a drained directory admits writes again")
}

func TestMemoryFileFlushCrashReplayAndPinnedReader(t *testing.T) {
	t.Parallel()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	e := New(Config{Backend: files, WAL: w, Prefix: "immutable"})
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	s := apiSeries()
	ids := []signal.SeriesID{s.Hash(), s.Hash(), s.Hash()}
	result, err := e.AppendBatch(ids, []int64{10, 10, 20}, []float64{1, 2, 3}, []float64{1, 2, 3},
		func(int) signal.Series { return s }, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 3, result.Accepted)
	reader, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	require.Zero(t, e.HeadSampleCount())
	require.Positive(t, e.ResidentHeadBytes(), "the old reader still owns immutable columns")
	batch, err := reader.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20}, batch.Timestamps)
	require.Equal(t, []float64{2, 3}, batch.Values)
	require.Equal(t, []float64{2, 3}, batch.ScaleFactors)
	require.NoError(t, reader.Close())
	require.Zero(t, e.ResidentHeadBytes())
	_, err = e.Append(s, 30, 4)
	require.NoError(t, err)
	require.NoError(t, e.CloseWAL())
	resumed, err := wal.Create(dir, 0)
	require.NoError(t, err)
	restored := New(Config{Backend: files, WAL: resumed, Prefix: "immutable"})
	t.Cleanup(func() { require.NoError(t, restored.CloseWAL()) })
	require.NoError(t, restored.LoadParts(t.Context()))
	require.NoError(t, restored.Replay(t.Context(), dir))
	require.Equal(t, 1, restored.HeadSampleCount())
	iterator, err := restored.Fetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	replayed, err := iterator.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20, 30}, replayed.Timestamps)
	require.Equal(t, []float64{2, 3, 4}, replayed.Values)
	require.NoError(t, iterator.Close())
}

func TestMemoryLabelProbeRetainsSourcesAcrossFlushAndReset(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	e.cfg.Backend = fileBackendForMemoryTest(t)
	enableMemoryPublicationTest(t, e)
	s := apiSeries()
	_, err := e.Append(s, 10, 1)
	require.NoError(t, err)
	probe, err := e.newLabelProbe(t.Context(), fetch.Request{Start: 0, End: 20})
	require.NoError(t, err)
	defer probe.release()
	require.NoError(t, e.Flush(t.Context()))
	require.NoError(t, e.Reset(t.Context()))
	probe.lock(e)
	live, err := probe.seriesLive(t.Context(), e, s.Hash())
	_, known := probe.registry.Get(s.Hash())
	e.mu.RUnlock()
	require.NoError(t, err)
	require.True(t, live)
	require.True(t, known)
}

func fileBackendForMemoryTest(t *testing.T) *file.File {
	t.Helper()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	return files
}
