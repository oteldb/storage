package recordengine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/obs/obstest"
)

// poolAdmission is the facade's admission over pool: a caller that may wait queues, one that may
// not takes what is free now.
func poolAdmission(pool *memlimit.Pool) func(context.Context, int64, bool) (func(), bool, error) {
	return func(ctx context.Context, n int64, wait bool) (func(), bool, error) {
		if !wait {
			release, ok := pool.TryAcquire(n)

			return release, ok, nil
		}

		release, err := pool.Acquire(ctx, n)

		return release, err == nil, err
	}
}

// TestMergeNeedingMoreThanTheBudgetCompletes: a merge that needs more than the whole process merge
// budget neither blocks forever nor runs silently over it. It takes the whole budget and runs alone:
// a background one once the budget is all free, deferring while anyone holds part of it, and a
// waiting one once the holders drain; each counts what it holds past the budget.
func TestMergeNeedingMoreThanTheBudgetCompletes(t *testing.T) {
	t.Parallel()

	const budget = 1 << 20

	for _, background := range []bool{true, false} {
		t.Run(map[bool]string{true: "background", false: "waiting"}[background], func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			pool := memlimit.NewPool(budget)
			o, m := obstest.New(t)

			e := wideDictEngine(t, backend.Memory(), Config{MergeMemoryBytes: budget, MergeAdmission: poolAdmission(pool), Obs: o}, 3, 4, 8<<10, 2)
			need, ok := e.mergeNeed(ctx, e.parts, e.mergeCapBytes())
			require.True(t, ok)
			require.Greater(t, need, int64(budget), "the merge must need more than the whole budget")

			before := e.PartPrefixes()

			held, ok := pool.TryAcquire(budget / 4)
			require.True(t, ok)

			if background {
				require.NoError(t, e.MergeWith(ctx, MergeOptions{Force: true, Background: true}))
				assert.True(t, e.MergeDeferred(), "a background merge defers while part of the budget is held")
				assert.Equal(t, before, e.PartPrefixes(), "nothing was compacted")

				held()

				require.NoError(t, e.MergeWith(ctx, MergeOptions{Force: true, Background: true}))
			} else {
				done := make(chan error, 1)

				go func() { done <- e.MergeWith(ctx, MergeOptions{Force: true}) }()

				require.Eventually(t, func() bool { return pool.Waiting() == 1 }, 5*time.Second, time.Millisecond,
					"a waiting merge queues for the whole budget")

				held()
				require.NoError(t, <-done)
			}

			assert.False(t, e.MergeDeferred())

			for _, p := range before {
				assert.NotContains(t, e.PartPrefixes(), p, "the merge rewrote every source")
			}

			assert.Equal(t, need-budget, m.Counter("storage.merge.over_budget_bytes", "signal", "record"),
				"what it held past the budget is counted")

			release, ok := pool.TryAcquire(budget)
			require.True(t, ok, "the merge handed the whole budget back")
			release()
		})
	}
}
