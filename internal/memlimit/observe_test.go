package memlimit_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/memlimit"
)

func TestUncontendedPoolSkipsWaitObserver(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		pool *memlimit.Pool
	}{
		{name: "disabled"},
		{name: "unbounded", pool: memlimit.NewPool(0)},
		{name: "available", pool: memlimit.NewPool(100)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			observed := false
			release, err := tc.pool.AcquireObserved(t.Context(), 50,
				func(_ context.Context, _ memlimit.Wait, wait func() (bool, error)) (bool, error) {
					observed = true
					return wait()
				})
			require.NoError(t, err)
			require.False(t, observed)
			if tc.pool != nil {
				require.EqualValues(t, 50, tc.pool.State().UsedBytes)
			}
			release()
			release()
			require.Zero(t, tc.pool.State().UsedBytes)
		})
	}
}
