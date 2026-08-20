package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/reproduce"
	"github.com/oteldb/storage/query/fetch"
)

// These cover what a *rebase* leaves behind. A commit that loses the conditional write reloads the
// winner's index and retries on top of it, which keeps the winner's entries — and that is where
// both of these stop. See #397 and #398.

// rivals returns two engines over one shared backend, each with its own node identity and each
// holding the state the other's commits will move under it.
func rivals(t *testing.T, be backend.Backend) (a, b *engine.Engine, aID, bID string) {
	t.Helper()

	a, aID = newSharedWriter(t, be)
	b, bID = newSharedWriter(t, be)

	return a, b, aID, bID
}

// committedIndex reads the index the shared prefix currently holds.
func committedIndex(t *testing.T, be backend.Backend) *bucketindex.Index {
	t.Helper()

	ix, err := bucketindex.Load(context.Background(), be, sharedPrefix+"/"+bucketindex.Object)
	require.NoError(t, err)

	return ix
}

// TestRebaseDoesNotLowerTheFlushWatermark is the reproducer for #397. FlushedEpoch is a per-node
// WAL generation, but the index holding it is shared, so a rebasing writer stamps its own counter
// over one that means something else. Here the loser's counter is the lower of the two, and the
// watermark a later recovery reads goes backwards — which is a replay of records the parts already
// hold.
//
// Non-regression is asserted rather than any particular value: it is a necessary condition of
// every fix in #397 (per-writer slots keep each writer's own; a globally comparable epoch is
// monotone), while the value itself depends on which is chosen. The watermark is read per writer
// because that is the design that won — a scalar in a shared object has no owner.
func TestRebaseDoesNotLowerTheFlushWatermark(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	a, b, aID, bID := rivals(t, be)

	s := mkSeries("job", "api")
	for i := range 3 {
		mustAppend(t, a, s, int64(100+i), float64(i))
		require.NoError(t, a.Flush(ctx))
	}

	committed := committedIndex(t, be).WriterEpoch(aID)
	require.EqualValues(t, 3, committed, "three flushes advanced the watermark")

	// b has flushed once, so its own watermark is 1. Its commit loses and rebases onto a's.
	mustAppend(t, b, mkSeries("job", "web"), 200, 2.0)
	require.NoError(t, b.Flush(ctx))

	after := committedIndex(t, be)
	require.GreaterOrEqual(t, after.WriterEpoch(aID), committed,
		"a rebased commit must not lower the flush watermark a previous commit recorded")
	require.EqualValues(t, 1, after.WriterEpoch(bID), "and records its own under its own name")
}

// TestRebaseServesTheAdoptedParts is the reproducer for #398. A rebase carries the rival's entries
// into the index but never opens them, so this engine publishes a part set it will not serve until
// its next LoadParts. Queried directly — an embedder, a single-replica read, or a completeness
// check that trusts the local index — the rival's rows are missing while the index says they are
// there.
func TestRebaseServesTheAdoptedParts(t *testing.T) {
	reproduce.Unfixed(t, 398, "adopted entries are committed but not opened")
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	a, b, _, _ := rivals(t, be)

	mustAppend(t, a, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, a.Flush(ctx))

	// b's commit loses to a's and rebases, adopting a's entry.
	mustAppend(t, b, mkSeries("job", "web"), 200, 2.0)
	require.NoError(t, b.Flush(ctx))

	require.Len(t, committedIndex(t, be).Entries, 2, "the rebased commit names both writers' parts")

	got := fetchAll(t, b, fetch.Request{
		Start:    0,
		End:      10000,
		Matchers: []fetch.Matcher{eqMatcher("job", "api")},
	})

	require.Len(t, got, 1, "an engine must serve every part the index it committed names")
	require.Equal(t, []int64{100}, got[0].Timestamps)
}
