package engine_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// TestConcurrentAppendFetchFlushMerge is the core safety test for the off-lock flush/merge/fetch
// refactor: many appenders and fetchers run against a single background flush/merge mutator (the real
// maintenance invariant) on a shared backend. The race detector validates the COW parts slice and the
// refcounted deferred reclamation; the post-conditions validate no samples are lost and no fetch ever
// sees a reclaimed part. Run with -race.
func TestConcurrentAppendFetchFlushMerge(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/m"})

	const (
		seriesN   = 4
		perSeries = 200
		fetchers  = 4
	)

	var (
		wg       sync.WaitGroup
		appended atomic.Int64
		stop     atomic.Bool
		fetchErr atomic.Pointer[error]
	)

	for s := range seriesN {
		ser := mkSeries("job", fmt.Sprintf("svc-%d", s))

		wg.Go(func() {
			for i := range perSeries {
				ok, err := e.Append(ser, int64(i+1), float64(i))
				if err != nil || !ok {
					t.Errorf("append: ok=%v err=%v", ok, err)

					return
				}

				appended.Add(1)
			}
		})
	}

	for range fetchers {
		wg.Go(func() {
			for !stop.Load() {
				it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 60})
				if err != nil {
					err := err
					fetchErr.CompareAndSwap(nil, &err)

					return
				}

				if _, err := fetch.Drain(ctx, it); err != nil {
					err := err
					fetchErr.CompareAndSwap(nil, &err)

					return
				}
			}
		})
	}

	// Single maintainer: flush then merge, repeatedly (flush/merge is single-mutator by the invariant).
	wg.Go(func() {
		for !stop.Load() {
			if err := e.Flush(ctx); err != nil {
				t.Errorf("flush: %v", err)

				return
			}

			if err := e.Merge(ctx, 0); err != nil {
				t.Errorf("merge: %v", err)

				return
			}
		}
	})

	for appended.Load() < seriesN*perSeries {
	}

	stop.Store(true)
	wg.Wait()

	if ep := fetchErr.Load(); ep != nil {
		t.Fatalf("fetch saw an error during churn (reclamation race?): %v", *ep)
	}

	require.NoError(t, e.Flush(ctx))

	total := 0
	for s := range seriesN {
		got := fetchAll(t, e, fetch.Request{
			Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{eqMatcher("job", fmt.Sprintf("svc-%d", s))},
		})
		for _, b := range got {
			total += len(b.Timestamps)
		}
	}

	assert.Equal(t, seriesN*perSeries, total, "no samples lost across concurrent flush/merge/fetch")
}

// TestFetchDuringMergeNoDataLoss checks a fetch overlapping a merge returns a consistent view (every
// sample present), never a torn old/new part set or a mid-flush visibility gap.
func TestFetchDuringMergeNoDataLoss(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/m2"})
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 100, 1)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 200, 2)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 300, 3)

	var wg sync.WaitGroup

	wg.Go(func() {
		for range 50 {
			require.NoError(t, e.Merge(ctx, 0))
			require.NoError(t, e.Flush(ctx))
		}
	})

	wg.Go(func() {
		for range 200 {
			got := fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})

			n := 0
			for _, b := range got {
				n += len(b.Timestamps)
			}

			assert.Equal(t, 3, n, "all three samples visible regardless of merge/flush interleaving")
		}
	})

	wg.Wait()
}
