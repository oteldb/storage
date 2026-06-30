package engine_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// TestDecodeBudgetCorrectUnderConcurrency checks the decode-memory budget is transparent to results:
// a tiny budget (which forces every query through the over-budget-alone path) returns the same data
// as an unbudgeted engine, and many concurrent fetches through that budget all succeed without
// deadlock.
func TestDecodeBudgetCorrectUnderConcurrency(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const series, samples = 30, 8

	mk := func(budget int64) *engine.Engine {
		e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/budget", DecodeMemoryBytes: budget})
		for s := range series {
			ser := mkSeries("__name__", "m", "host", strconv.Itoa(s))
			for k := range samples {
				mustAppend(t, e, ser, int64(100+s*1000+k*10), float64(s*10+k))
			}
		}

		require.NoError(t, e.Flush(ctx))

		return e
	}

	req := fetch.Request{Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{eqMatcher("__name__", "m")}}

	// A 1 KiB budget is far below any part's decode footprint, so every query is admitted alone — it
	// must still return exactly the unbudgeted result.
	want := drainSorted(t, mk(0), req)
	require.Len(t, want, series)

	budgeted := mk(1 << 10)
	assert.Equal(t, want, drainSorted(t, budgeted, req))

	// Concurrent fetches through the tiny budget must all complete (no deadlock) and match.
	var (
		wg     sync.WaitGroup
		badErr atomic.Bool
		badLen atomic.Bool
	)

	for range 8 {
		wg.Go(func() {
			it, err := budgeted.Fetch(ctx, req)
			if err != nil {
				badErr.Store(true)

				return
			}

			batches, err := fetch.Drain(ctx, it)
			if err != nil {
				badErr.Store(true)

				return
			}

			if len(batches) != series {
				badLen.Store(true)
			}
		})
	}

	wg.Wait()

	assert.False(t, badErr.Load(), "a concurrent budgeted fetch errored")
	assert.False(t, badLen.Load(), "a concurrent budgeted fetch returned the wrong series count")
}
