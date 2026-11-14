package engine

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/query/fetch"
)

func TestNativeFlushFinishingFitsMergeShare(t *testing.T) {
	reproduce.Unfixed(t, 661, "finishing a near-target flush waits for the merge's reserved memory")
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
