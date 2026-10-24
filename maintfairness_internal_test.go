package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/signal"
)

func TestDeferredMaintenanceGetsBudget(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		tasks int
		limit int
		mixed bool
	}{
		{"below worker count", 3, 16, false},
		{"at worker count", 3, 4, false},
		{"above worker count", 7, 2, false},
		{"one worker", 3, 1, false},
		{"mixed requests", 7, 16, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				pool := memlimit.NewPool(100)
				admitted := make([]bool, tt.tasks+1)
				tasks := make([]maintTask, len(admitted))

				for i := range tasks {
					tasks[i] = maintTask{deferred: i < tt.tasks, pressure: int64(i), run: func() {
						ask := int64(100)
						if tt.mixed {
							ask = []int64{25, 75, 100}[i%3]
						}
						release, ok := pool.TryAcquire(ask)
						if !ok {
							return
						}
						defer release()

						admitted[i] = true
						time.Sleep(time.Second)
					}}
				}

				runMaintTasks(tasks, tt.limit)

				for i, ok := range admitted {
					assert.True(t, ok, "engine %d never received its budget", i)
				}
			})
		})
	}
}

func TestDeferredMaintenanceDoesNotWaitForOperator(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pool := memlimit.NewPool(100)
		held, ok := pool.TryAcquire(100)
		require.True(t, ok)
		defer held()

		var attempted, fresh bool
		runMaintTasks([]maintTask{
			{deferred: true, run: func() {
				attempted = true
				release, ok := pool.TryAcquire(100)
				assert.False(t, ok, "the operator still holds the budget")
				if ok {
					release()
				}
			}},
			{run: func() { fresh = true }},
		}, 16)

		assert.True(t, attempted)
		assert.True(t, fresh, "a busy budget must not prevent the remaining maintenance work")
	})
}

func TestMaintainRetriesDeferredEngines(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory(WithMergeMemory(64<<20), WithMaintenanceConcurrency(16),
		WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
			v, _ := r.Attributes.Get([]byte("service.name"))
			return signal.TenantID(v.Str())
		}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	held, ok := s.mergePool.TryAcquire(s.mergePool.Total())
	require.True(t, ok)
	defer held()

	engines := make([]*engine.Engine, 0, 3)
	for i := range 3 {
		name := fmt.Sprintf("tenant-%d", i)
		eng, err := s.engineFor(signal.TenantID(name))
		require.NoError(t, err)
		for j := range 2 {
			_, err := s.WriteMetrics(ctx, gaugeBatch(name, "m", []int64{int64(j + 1)}, []float64{1}))
			require.NoError(t, err)
			require.NoError(t, eng.Flush(ctx))
		}
		require.NoError(t, eng.MergeWith(ctx, engine.MergeOptions{Background: true}))
		require.True(t, eng.MergeDeferred())
		require.Equal(t, 2, eng.PartCount())
		engines = append(engines, eng)
	}

	held()
	s.maintain(ctx)

	for i, eng := range engines {
		assert.False(t, eng.MergeDeferred(), "engine %d remained deferred", i)
		assert.Equal(t, 1, eng.PartCount(), "engine %d did not compact", i)
	}
}

func TestFreshMaintenanceRunsConcurrently(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var running, peak atomic.Int64
		tasks := make([]maintTask, 5)

		for i := range tasks {
			tasks[i].run = func() {
				n := running.Add(1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
				time.Sleep(time.Second)
				running.Add(-1)
			}
		}

		runMaintTasks(tasks, 3)
		assert.Equal(t, int64(3), peak.Load())
		assert.Zero(t, running.Load())
	})
}
