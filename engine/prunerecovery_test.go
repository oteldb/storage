package engine_test

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/wal"
)

// TestPruneIdentitiesSurvivesRestart covers the WAL hazard a prune could introduce: the log's
// sample records resolve through the identity object, so a pruned identity must never be one the
// log still needs. It cannot be — the WAL is checkpointed at every flush, so its live records name
// only series with buffered samples, which are live by definition — and this asserts it end to end.
func TestPruneIdentitiesSurvivesRestart(t *testing.T) {
	t.Parallel()

	const (
		total = pruneMinIdentitiesTest
		keep  = 4
	)

	ctx := context.Background()
	dir := t.TempDir()
	be := backend.Memory()

	sw, err := wal.Create(dir, 0)
	require.NoError(t, err)

	src := engine.New(engine.Config{WAL: sw, Backend: be, Prefix: "t/prune-wal"})

	for i := range total {
		mustAppend(t, src, churnSeries(i), 1000, float64(i))
	}

	require.NoError(t, src.Flush(ctx)) // checkpoints the WAL: those records are now in a part

	// Unflushed samples for the survivors: only the WAL holds them, so replay must still resolve
	// their identities after the prune.
	for i := range keep {
		mustAppend(t, src, churnSeries(i), 2000, 7)
	}

	require.NoError(t, src.MergeWith(ctx, engine.MergeOptions{RetainFrom: 1500}))

	removed, err := src.PruneIdentities(ctx)
	require.NoError(t, err)
	require.Equal(t, total-keep, removed)
	require.NoError(t, sw.Close())

	restored := engine.New(engine.Config{Backend: be, Prefix: "t/prune-wal"})
	require.NoError(t, restored.LoadParts(ctx))
	require.NoError(t, restored.Replay(dir))

	assert.Equal(t, keep, restored.SeriesCount(), "recovery rebuilds the pruned set, not the old one")

	got := fetchAll(t, restored, fetch.Request{
		Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{eqMatcher("inst", "0")},
	})
	require.Len(t, got, 1, "a survivor's unflushed samples replay")
	assert.Equal(t, []int64{2000}, got[0].Timestamps)
}

// TestPruneIdentitiesConcurrentIngest runs the prune against live appends and fetches. The rebuild
// runs off the engine lock, so both ways a series can gain data mid-rebuild must survive the swap:
// a brand-new series (registered past the snapshot's end) and a *dead* one that starts ingesting
// again (no re-registration — the old index still holds its identity).
func TestPruneIdentitiesConcurrentIngest(t *testing.T) {
	t.Parallel()

	const (
		total  = pruneMinIdentitiesTest
		keep   = 16
		extra  = 200
		firstX = total // ids of the series appended concurrently
	)

	ctx := context.Background()
	e := pruneEngine(t, total, keep)

	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{RetainFrom: 500}))

	// The concurrent goroutines collect their errors instead of asserting: only the test goroutine
	// may fail the test.
	var (
		wg   sync.WaitGroup
		errs = make(chan error, 2)
	)

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := range extra {
			for _, inst := range []int{firstX + i, i} { // a new identity, and one the prune calls dead
				if _, err := e.Append(churnSeries(inst), 2000, 1); err != nil {
					errs <- err

					return
				}
			}
		}
	}()

	go func() {
		defer wg.Done()

		for range 20 {
			it, err := e.Fetch(ctx, fetch.Request{
				Start: 0, End: 1 << 60,
				Matchers: []fetch.Matcher{eqMatcher("job", "api")},
			})
			if err != nil {
				errs <- err

				return
			}

			if _, err := fetch.Drain(ctx, it); err != nil {
				errs <- err

				return
			}
		}
	}()

	_, err := e.PruneIdentities(ctx)
	require.NoError(t, err)

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	// Every concurrently-appended series is still resolvable — new or revived — and its samples are
	// still reachable: whenever it landed relative to the swap, its buffer made it live.
	for i := range extra {
		for _, inst := range []int{firstX + i, i} {
			got := fetchAll(t, e, fetch.Request{
				Start: 0, End: 1 << 60,
				Matchers: []fetch.Matcher{eqMatcher("inst", strconv.Itoa(inst))},
			})
			require.Len(t, got, 1, "series written during the prune is still indexed")
			require.Contains(t, got[0].Timestamps, int64(2000), "its samples are still reachable")
		}
	}
}

// TestPruneIdentitiesReclaimsHeap is the point of the exercise: the identity memory an operator
// cannot otherwise get back short of a restart actually returns to the heap.
//
//nolint:paralleltest // reads process-wide heap statistics; a parallel test would pollute them
func TestPruneIdentitiesReclaimsHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~100 MB and measures the heap")
	}

	const (
		total = 200_000
		keep  = total / 100
	)

	ctx := context.Background()
	e := pruneEngine(t, total, keep)

	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{RetainFrom: 500}))

	before := heapAlloc()
	reportedBefore := e.Stats().IdentityBytes

	removed, err := e.PruneIdentities(ctx)
	require.NoError(t, err)
	require.Equal(t, total-keep, removed)

	after := heapAlloc()
	reportedAfter := e.Stats().IdentityBytes
	runtime.KeepAlive(e)

	t.Logf("identity bytes %d → %d (reported), heap %d → %d, %d identities removed",
		reportedBefore, reportedAfter, before, after, removed)

	assert.Less(t, reportedAfter, reportedBefore/10, "99 % of the identities were dead")
	assert.Less(t, after, before, "the reclaimed identity memory returns to the heap")
}
