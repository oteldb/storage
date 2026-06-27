package recordengine_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
)

// TestConcurrentAppendFetchFlushMerge is the core safety test for the off-lock flush/merge/fetch
// refactor: many appenders and fetchers run against a single background flush/merge mutator (the real
// maintenance invariant) on a shared backend. The race detector validates the COW parts slice and the
// refcounted deferred reclamation; the post-conditions validate no data is lost and no fetch ever sees
// a reclaimed part (ErrNotExist). Run with -race.
func TestConcurrentAppendFetchFlushMerge(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	const (
		services    = 4
		perService  = 200
		fetchers    = 4
		maintainers = 1 // exactly one: flush/merge is single-mutator by the maintenance invariant
	)

	var (
		wg       sync.WaitGroup
		appended atomic.Int64
		stop     atomic.Bool
		fetchErr atomic.Pointer[error]
	)

	// Appenders: each service appends perService records at strictly increasing timestamps.
	for s := range services {
		svc := fmt.Sprintf("svc-%d", s)

		wg.Go(func() {
			for i := range perService {
				ts := int64(i + 1)
				_, err := e.AppendBatch(mkBatch(svc, rrec{ts: ts, body: fmt.Sprintf("b%d", i)}), recordengine.AppendLimits{})
				if err != nil {
					t.Errorf("append: %v", err)

					return
				}

				appended.Add(1)
			}
		})
	}

	// Fetchers: hammer reads across the whole window while parts churn underneath. Any error (notably
	// an ErrNotExist from a reclaimed part) fails the test.
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

	// Single maintainer: flush then merge, repeatedly, while appends/fetches run.
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

	// Let the appenders finish, then stop the fetchers/maintainer.
	doneAppending := make(chan struct{})
	go func() {
		// Spin until every appender has produced its quota.
		for appended.Load() < services*perService {
		}

		close(doneAppending)
	}()

	<-doneAppending
	stop.Store(true)
	wg.Wait()

	if ep := fetchErr.Load(); ep != nil {
		t.Fatalf("fetch saw an error during churn (reclamation race?): %v", *ep)
	}

	// Final flush so every record is in a part, then assert the full count survived the churn.
	require.NoError(t, e.Flush(ctx))

	total := 0
	for s := range services {
		got := fetchAll(t, e, req(fmt.Sprintf("svc-%d", s)))
		for _, b := range got {
			total += len(b.Timestamps)
		}
	}

	assert.Equal(t, services*perService, total, "no records lost across concurrent flush/merge/fetch")
}

// TestFetchDuringMergeNoDataLoss checks the snapshot semantics directly: a fetch that overlaps a merge
// returns a consistent view (every record present), never a torn old/new part set.
func TestFetchDuringMergeNoDataLoss(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	// Two parts plus a head record for one stream.
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 300, body: "head"}))

	var wg sync.WaitGroup

	wg.Go(func() {
		for range 50 {
			require.NoError(t, e.Merge(ctx, 0))
			require.NoError(t, e.Flush(ctx))
		}
	})

	wg.Go(func() {
		for range 200 {
			got := fetchAll(t, e, req("api"))

			n := 0
			for _, b := range got {
				n += len(b.Timestamps)
			}

			assert.Equal(t, 3, n, "all three records visible regardless of merge/flush interleaving")
		}
	})

	wg.Wait()
}
