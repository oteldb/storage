package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

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
