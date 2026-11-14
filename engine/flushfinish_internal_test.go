package engine

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/query/fetch"
)

func TestNativeFlushFinishingFitsMergeShare(t *testing.T) {
	t.Parallel()
	for _, cached := range []bool{false, true} {
		name := "file"
		if cached {
			name = "cached-file"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				const flushBytes, seriesCount = 16 << 20, 8192
				pool := memlimit.NewPool(4 * flushBytes)
				releaseMerge, err := pool.Acquire(t.Context(), 3*flushBytes)
				require.NoError(t, err)
				defer releaseMerge()
				file, err := backendfile.New(t.TempDir())
				require.NoError(t, err)
				var be backend.Backend = file
				if cached {
					be = backend.Cached(be, 8<<20)
				}
				var largest int64
				e := New(Config{Backend: be, Prefix: "flush-finish", DiskSeries: true, MetricSeriesChunks: true,
					AggregateStats: true, ResidentHeadBytes: 1 << 30, FlushMemoryBytes: flushBytes,
					FlushAdmission: func(ctx context.Context, bytes int64) (func(), error) {
						largest = max(largest, bytes)
						return pool.Acquire(ctx, bytes)
					}})
				defer func() { require.NoError(t, e.CloseWAL()) }()
				for i := range seriesCount {
					_, err := e.Append(benchSeries(i), 100, float64(i))
					require.NoError(t, err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- e.Flush(ctx) }()
				synctest.Wait()
				select {
				case err := <-done:
					require.NoError(t, err)
				default:
					waiting := pool.Waiting()
					cancel()
					synctest.Wait()
					require.ErrorIs(t, <-done, context.Canceled)
					require.Zero(t, pool.Waiting(), "cancellation must remove the blocked admission")
					require.Equal(t, seriesCount, e.HeadSampleCount(), "failed flush must restore every sample")
					t.Fatalf("flush blocked behind merge: waiters=%d, largest reservation=%d, flush share=%d", waiting, largest, flushBytes)
				}
				require.LessOrEqual(t, largest, int64(flushBytes), "finishing must fit the protected flush share")
				require.Greater(t, e.PartCount(), 1, "fixture must seal multiple bounded output parts")
				t.Logf("completed %d parts while merge held its share; largest reservation=%d, share=%d", e.PartCount(), largest, flushBytes)
				require.Zero(t, e.HeadSampleCount())
				it, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 101})
				require.NoError(t, err)
				got, err := fetch.Drain(t.Context(), it)
				require.NoError(t, err)
				require.Len(t, got, seriesCount)
				release, ok := pool.TryAcquire(flushBytes)
				require.True(t, ok, "completed flush must return its reservation while the merge still holds its share")
				release()
			})
		})
	}
}

func TestNativeFlushPricesFinishingWorkspaceBeforeSealing(t *testing.T) {
	t.Parallel()
	const flushBytes = 16 << 20
	partCounts := make([]int, 0, 2)
	for _, workspace := range []int64{128 << 10, 6 << 20} {
		file, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		be := &identityWriterBackend{Backend: file, ObjectCreator: file, DeferredSyncer: file,
			known: true, streaming: true, workspace: workspace}
		e := New(Config{Backend: be, Prefix: "priced-finish", DiskSeries: true, MetricSeriesChunks: true,
			FlushMemoryBytes: flushBytes, ResidentHeadBytes: 1 << 30,
			FlushAdmission: func(_ context.Context, bytes int64) (func(), error) {
				require.LessOrEqual(t, bytes, int64(flushBytes))
				return func() {}, nil
			}})
		t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
		for i := range 8192 {
			_, err := e.Append(benchSeries(i), 100, float64(i))
			require.NoError(t, err)
		}
		require.NoError(t, e.Flush(t.Context()))
		partCounts = append(partCounts, e.PartCount())
	}
	require.Greater(t, partCounts[1], partCounts[0], "larger finishing workspace must reduce the rows admitted to each part")
}

func TestNativeFlushAdmissionCancellationRestoresHead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		pool := memlimit.NewPool(64 << 20)
		held, err := pool.Acquire(t.Context(), pool.Total())
		require.NoError(t, err)
		defer held()
		file, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		e := New(Config{Backend: file, Prefix: "cancel-finish", DiskSeries: true, MetricSeriesChunks: true,
			FlushMemoryBytes: 16 << 20, FlushAdmission: pool.Acquire})
		defer func() { require.NoError(t, e.CloseWAL()) }()
		for i := range 32 {
			_, err := e.Append(benchSeries(i), 100, float64(i))
			require.NoError(t, err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- e.Flush(ctx) }()
		synctest.Wait()
		require.Equal(t, 1, pool.Waiting())
		cancel()
		synctest.Wait()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Zero(t, pool.Waiting())
		require.Equal(t, 32, e.HeadSampleCount())
		require.Zero(t, e.PartCount())
		held()
		release, ok := pool.TryAcquire(pool.Total())
		require.True(t, ok, "cancellation must not leak an admission grant")
		release()
	})
}

func TestOversizedNativeFlushWaitsForMerge(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const flushBytes = 16 << 20
		pool := memlimit.NewPool(4 * flushBytes)
		releaseMerge, err := pool.Acquire(t.Context(), 3*flushBytes)
		require.NoError(t, err)
		defer releaseMerge()
		file, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		e := New(Config{Backend: file, Prefix: "oversized-finish", DiskSeries: true, MetricSeriesChunks: true,
			FlushMemoryBytes: flushBytes, FlushAdmission: pool.Acquire})
		defer func() { require.NoError(t, e.CloseWAL()) }()
		series := benchSeries(0)
		series.Resource.SchemaURL = []byte(strings.Repeat("x", 2<<20))
		_, err = e.Append(series, 100, 1)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- e.Flush(ctx) }()
		synctest.Wait()
		require.Equal(t, 1, pool.Waiting(), "an oversized identity must retain its larger admission")
		releaseMerge()
		synctest.Wait()
		require.NoError(t, <-done)
		require.Zero(t, e.HeadSampleCount())
		require.Equal(t, 1, e.PartCount())
		release, ok := pool.TryAcquire(pool.Total())
		require.True(t, ok)
		release()
	})
}
