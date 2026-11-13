package engine

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
)

func TestSmallNativeFlushDoesNotReservePartTarget(t *testing.T) {
	t.Parallel()
	file, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	var reservations []int64
	e := New(Config{Backend: file, Prefix: "shape", FlushMemoryBytes: 448 << 20,
		FlushAdmission: func(_ context.Context, n int64) (func(), error) {
			reservations = append(reservations, n)
			return func() {}, nil
		}})
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	for i := range 8192 {
		_, err = e.Append(benchSeries(i), 100, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Equal(t, 1, e.PartCount(), "reservation sizing must not lower the output target")
	require.NotEmpty(t, reservations)
	for _, n := range reservations {
		require.Less(t, n, int64(128<<20))
	}
}

func TestNativeFlushInitialReservationsAllowPeers(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		pool := memlimit.NewPool(64 << 20)
		first := make(chan int64, 4)
		gate := make(chan struct{})
		errors := make(chan error, 4)
		var workers sync.WaitGroup
		for i := range 4 {
			file, err := backendfile.New(t.TempDir())
			require.NoError(t, err)
			initial := true
			e := New(Config{Backend: file, Prefix: "peer", FlushMemoryBytes: 448 << 20,
				FlushAdmission: func(ctx context.Context, n int64) (func(), error) {
					release, err := pool.Acquire(ctx, n)
					if err == nil && initial {
						initial = false
						first <- n
						<-gate
					}
					return release, err
				}})
			t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
			_, err = e.Append(benchSeries(i), 100, 1)
			require.NoError(t, err)
			workers.Go(func() { errors <- e.Flush(t.Context()) })
		}
		synctest.Wait()
		admitted := len(first)
		close(gate)
		workers.Wait()
		require.Equal(t, 4, admitted, "small frozen heads must not each reserve the whole pool")
		for range 4 {
			require.NoError(t, <-errors)
		}
	})
}
