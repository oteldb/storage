package storage

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/obs/obstest"
)

func TestStorageMemoryWaitInstrumentation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, pool, operation string
	}{
		{name: "flush-build", pool: "build", operation: "flush"},
		{name: "merge-build", pool: "build", operation: "merge"},
		{name: "merge-pool", pool: "merge", operation: "merge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				provider, metrics := obstest.Provider(t)
				s, err := Open(t.Context(), Options{Backend: backend.Memory(), Durability: DurabilityEphemeral,
					MergeMemoryBytes: 64 << 20, FlushInterval: -1, FlushThresholdBytes: -1, MeterProvider: provider})
				require.NoError(t, err)
				defer func() { require.NoError(t, s.Close(context.Background())) }()
				pool := s.buildPool
				if tc.pool == "merge" {
					pool = s.mergePool
				}
				held, err := pool.Acquire(t.Context(), pool.Total())
				require.NoError(t, err)
				defer held()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					var err error
					var release func()
					if tc.operation == "flush" {
						release, err = s.flushAdmission()(ctx, 8<<20)
					} else {
						release, _, err = s.admitMerge(ctx, 8<<20, true)
					}
					if err == nil {
						release()
					}
					done <- err
				}()
				synctest.Wait()
				require.EqualValues(t, 1, metrics.Counter("storage.memory.admission.waiters", "pool", tc.pool, "operation", tc.operation))
				time.Sleep(time.Second)
				synctest.Wait()
				require.EqualValues(t, 1, metrics.Counter("storage.memory.admission.slow_waiters", "pool", tc.pool, "operation", tc.operation))
				cancel()
				synctest.Wait()
				require.ErrorIs(t, <-done, context.Canceled)
				require.Zero(t, metrics.Counter("storage.memory.admission.waiters"))
				require.Zero(t, metrics.Counter("storage.memory.admission.waiting_bytes"))
				held()
				for _, pool := range []*memlimit.Pool{s.buildPool, s.mergePool} {
					release, ok := pool.TryAcquire(pool.Total())
					require.True(t, ok, "canceled merge admission must release its first pool grant")
					release()
				}
			})
		})
	}
}
