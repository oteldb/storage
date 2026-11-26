package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
)

func TestMetadataReservationFallsBackToExactIncrement(t *testing.T) {
	t.Parallel()
	const mib = 1 << 20
	pool := memlimit.NewPool(100 * mib)
	merge, ok := pool.TryAcquire(42 * mib)
	require.True(t, ok)
	defer merge()
	held, ok := pool.TryAcquire(40 * mib)
	require.True(t, ok)
	var attempts []int64
	e := New(Config{TryFlushAdmission: func(bytes int64) (func(), bool) {
		attempts = append(attempts, bytes)
		return pool.TryAcquire(bytes)
	}})
	b := headPartBuilder{ctx: t.Context(), e: e, release: held, reserved: 40 * mib}
	defer func() { b.release() }()
	require.NoError(t, b.reservePhase(56*mib, "metadata"))
	require.Equal(t, []int64{20 * mib, 16 * mib}, attempts)
	require.Equal(t, int64(98*mib), pool.State().UsedBytes)
	require.Equal(t, int64(56*mib), b.reserved)
}

func TestNativeBackgroundMergeAdmitsSmallerBudget(t *testing.T) {
	t.Parallel()
	file, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	var requests []int64
	e := New(Config{
		Backend: file, Prefix: "smaller-merge", DiskSeries: true, MergeMemoryBytes: 128 << 20,
		MergeAdmission: func(_ context.Context, bytes int64, wait bool) (func(), bool, error) {
			require.False(t, wait)
			requests = append(requests, bytes)
			return func() {}, bytes <= 64<<20, nil
		},
	})
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	for i := range 4 {
		_, err := e.Append(benchSeries(1), int64(i+1), 1)
		require.NoError(t, err)
		require.NoError(t, e.Flush(t.Context()))
	}
	merged, err := e.MergeStep(t.Context(), MergeOptions{Background: true})
	require.NoError(t, err)
	require.True(t, merged)
	require.Equal(t, []int64{128 << 20, 64 << 20}, requests)
	require.Equal(t, int64(64<<20), e.LastMergePlan().AllowanceBytes)
}
