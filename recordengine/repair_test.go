package recordengine_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/recordengine"
)

// fakeFetcher stands in for the cluster layer's part-sync pull: it records what repair asked for
// and answers with whatever the test staged, so the engine half of the seam is exercised without a
// peer, a transport or a syncer.
type fakeFetcher struct {
	mu     sync.Mutex
	asked  []string
	calls  int
	answer func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error)
}

func (f *fakeFetcher) FetchWants(_ context.Context, wants []bucketindex.Want) []recordengine.FetchResult {
	f.mu.Lock()
	f.calls++

	for _, w := range wants {
		f.asked = append(f.asked, w.Prefix)
	}

	answer := f.answer
	f.mu.Unlock()

	out := make([]recordengine.FetchResult, len(wants))

	for i, w := range wants {
		if answer == nil {
			out[i].Outcome = bucketindex.WantAbsent

			continue
		}

		ent, outcome, err := answer(w)
		out[i] = recordengine.FetchResult{Entry: ent, Outcome: outcome, Err: err}
	}

	return out
}

// asks reports what repair has asked for, in call order.
func (f *fakeFetcher) asks() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.asked)
}

// fetchCalls reports how many batches repair issued — one per cycle, whatever the want count.
func (f *fakeFetcher) fetchCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

func newRepairEngine(t *testing.T, be backend.Backend, r recordengine.PartFetcher) *recordengine.Engine {
	t.Helper()

	return recordengine.New(recordengine.Config{
		Schema: testSchema, Backend: be, Prefix: "t/recs", Repair: r,
	})
}

// copyObjects moves every object under prefix from src to dst, which is what a repair fetch does
// to the objects of a part it pulls back.
func copyObjects(t *testing.T, src, dst backend.Backend, prefix string) int {
	t.Helper()
	ctx := context.Background()

	keys, err := src.List(ctx, prefix)
	require.NoError(t, err)

	for _, k := range keys {
		data, err := backend.ReadView(ctx, src, k)
		require.NoError(t, err)
		require.NoError(t, dst.Write(ctx, k, data))
	}

	return len(keys)
}

// dropObjects deletes every object of a part, simulating the disk loss a want records.
func dropObjects(t *testing.T, be backend.Backend, prefix string) {
	t.Helper()
	ctx := context.Background()

	keys, err := be.List(ctx, prefix)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	for _, k := range keys {
		require.NoError(t, be.Delete(ctx, k))
	}
}

// committedIndex reads back the engine's persisted bucket index.
func committedIndex(t *testing.T, be backend.Backend) *bucketindex.Index {
	t.Helper()

	ix, _, err := bucketindex.LoadVersioned(context.Background(), be, "t/recs/"+bucketindex.Object)
	require.NoError(t, err)

	return ix
}

// TestRepairFetchesWantedPartFromPeer is the plain repair: the exact part comes back from a peer,
// is readable again, is named by the committed index, and its want is gone — one commit.
func TestRepairFetchesWantedPartFromPeer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()

	f := &fakeFetcher{}
	e := newRepairEngine(t, be, f)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, e.Flush(ctx))

	parts := e.PartPrefixes()
	require.Len(t, parts, 2)
	lost := parts[0]

	// The peer keeps a copy; this node loses its own and owes the repair.
	copyObjects(t, be, peer, lost)
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	require.Equal(t, []string{lost}, e.WantPrefixes())

	f.answer = func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peer, be, w.Prefix)

		return bucketindex.Entry{
			Prefix: w.Prefix, MinTime: 100, MaxTime: 100, Blocks: w.Blocks,
		}, bucketindex.WantSatisfied, nil
	}

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, f.asks())
	assert.Empty(t, e.WantPrefixes(), "committing the part is what discharges the want")
	assert.Equal(t, int64(1), e.RepairStats().Fetched)
	assert.ElementsMatch(t, []string{"p1", "p2"}, streamBodies(t, e))

	ix := committedIndex(t, be)
	assert.Empty(t, ix.Wanted)
	assert.NotEmpty(t, ix.Entries)
}

// TestRepairDischargedByContainingSuccessor is the property that makes repair terminate: the
// wanted part exists nowhere any more, only inside a merged successor, and accepting that
// successor both discharges the want and retires the local parts it contains — so the rows are
// there exactly once.
func TestRepairDischargedByContainingSuccessor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be, peerBE := backend.Memory(), backend.Memory()

	f := &fakeFetcher{}
	e := newRepairEngine(t, be, f)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, e.Flush(ctx))

	parts := e.PartPrefixes()
	require.Len(t, parts, 2)
	lost, kept := parts[0], parts[1]

	e.SetPartBlocks(lost, bucketindex.Interval{Min: 1, Max: 1}, 0)
	e.SetPartBlocks(kept, bucketindex.Interval{Min: 2, Max: 2}, 0)

	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	// The peer merged both blocks away into one level-1 part; the wanted prefix is gone there too.
	peer := newRepairEngine(t, peerBE, nil)
	ingest(t, peer, mkBatch("api", rrec{ts: 100, body: "p1"}))
	ingest(t, peer, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, peer.Flush(ctx))

	peerParts := peer.PartPrefixes()
	require.Len(t, peerParts, 1)
	successor := peerParts[0]

	f.answer = func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peerBE, be, successor)

		return bucketindex.Entry{
			Prefix: successor, MinTime: 100, MaxTime: 200,
			Blocks: bucketindex.Interval{Min: 1, Max: 2}, Level: 1,
		}, bucketindex.WantSatisfied, nil
	}

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, f.asks(), "repair asks for the want it holds")
	assert.Empty(t, e.WantPrefixes(), "a containing successor discharges the want")
	assert.Equal(t, int64(1), e.RepairStats().Fetched)

	local, err := be.List(ctx, lost)
	require.NoError(t, err)
	assert.Empty(t, local, "the vanished prefix is never fetched — only the successor containing it")

	assert.Equal(t, []string{successor}, e.PartPrefixes(),
		"the superseded local part is retired, not kept alongside the part containing it")
	assert.ElementsMatch(t, []string{"p1", "p2"}, streamBodies(t, e),
		"accepting a containing part must not double-count the rows inside it")

	assert.Empty(t, committedIndex(t, be).Wanted)
}

// TestRepairDischargedByLocalPart verifies the check that runs before the network: a want this
// node's own index already covers costs no fetch at all.
func TestRepairDischargedByLocalPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	f := &fakeFetcher{}
	e := newRepairEngine(t, be, f)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "p1"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, e.Flush(ctx))

	parts := e.PartPrefixes()
	require.Len(t, parts, 2)

	// The surviving part already covers the wanted blocks at a higher level.
	e.SetPartBlocks(parts[1], bucketindex.Interval{Min: 1, Max: 4}, 1)

	dropObjects(t, be, parts[0])
	e.LosePart(parts[0], bucketindex.Interval{Min: 1, Max: 1})

	require.NoError(t, e.Merge(ctx, 0))

	assert.Empty(t, f.asks(), "no network call for a want the local index covers")
	assert.Empty(t, e.WantPrefixes())
	assert.Equal(t, int64(1), e.RepairStats().Local)
	assert.Equal(t, []string{"p2"}, streamBodies(t, e))
}

// TestRepairNoPeerLeavesWant verifies definitive absence: the obligation survives, is counted, and
// the merge that carried the repair still compacts.
func TestRepairNoPeerLeavesWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	f := &fakeFetcher{answer: func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{}, bucketindex.WantAbsent, nil
	}}
	e := newRepairEngine(t, be, f)

	lost := loseOneOfThree(t, e, be)

	require.NoError(t, e.Merge(ctx, 0), "a repair that cannot proceed must not block compaction")

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Equal(t, int64(1), e.RepairStats().Unsatisfiable)
	assert.Less(t, len(e.PartPrefixes()), 2, "the merge still compacted the parts that are here")

	wanted := committedIndex(t, be).Wanted
	require.Len(t, wanted, 1)
	assert.Equal(t, lost, wanted[0].Prefix, "the obligation is durable, not only in memory")
}

// TestRepairTransientFailureKeepsWant verifies the permanent-vs-transient discipline: an
// unreachable peer leaves the want intact and the index consistent, and the next cycle retries.
func TestRepairTransientFailureKeepsWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	f := &fakeFetcher{answer: func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{}, bucketindex.WantIncomplete, errors.New("peer unreachable")
	}}
	e := newRepairEngine(t, be, f)

	lost := loseOneOfThree(t, e, be)

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Equal(t, int64(1), e.RepairStats().Failed)
	assert.Zero(t, e.RepairStats().Unsatisfiable, "a peer we could not ask is not evidence of absence")

	// The index must still be loadable and name only parts that are here.
	r := newRepairEngine(t, be, nil)
	require.NoError(t, r.LoadParts(ctx))

	require.NoError(t, e.Merge(ctx, 0))
	assert.Equal(t, []string{lost}, e.WantPrefixes(), "the obligation is retried, not forgotten")
	assert.Equal(t, int64(2), e.RepairStats().Failed)
}

// TestRepairWithoutCallbackIsNoOp verifies single-node mode: with no cluster to pull from, a merge
// runs exactly as it did before and the want is left alone.
func TestRepairWithoutCallbackIsNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newRepairEngine(t, be, nil)

	lost := loseOneOfThree(t, e, be)

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Equal(t, recordengine.RepairStats{}, e.RepairStats())
}

// loseOneOfThree flushes three parts, destroys one of them and records its want, returning the
// lost prefix. Two parts survive so the merge the repair rides on still has work to do.
func loseOneOfThree(t *testing.T, e *recordengine.Engine, be backend.Backend) string {
	t.Helper()
	ctx := context.Background()

	for i, body := range []string{"p1", "p2", "p3"} {
		ingest(t, e, mkBatch("api", rrec{ts: int64(100 * (i + 1)), body: body}))
		require.NoError(t, e.Flush(ctx))
	}

	parts := e.PartPrefixes()
	require.Len(t, parts, 3)

	lost := parts[0]
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	require.True(t, slices.Contains(e.WantPrefixes(), lost))

	return lost
}

// TestRepairUnreadablePartKeepsWant verifies the half-copied case: the objects arrived but the part
// will not open, so nothing is published and the obligation is still owed.
func TestRepairUnreadablePartKeepsWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	f := &fakeFetcher{}
	e := newRepairEngine(t, be, f)

	lost := loseOneOfThree(t, e, be)

	f.answer = func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		require.NoError(t, be.Write(ctx, w.Prefix+"/manifest", []byte("truncated")))

		return bucketindex.Entry{Prefix: w.Prefix, Blocks: w.Blocks}, bucketindex.WantSatisfied, nil
	}

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Zero(t, e.RepairStats().Fetched, "a part that will not open was not repaired")
	assert.Equal(t, int64(1), e.RepairStats().Failed)
}

// TestRepairCommitFailureKeepsWant verifies that only an index write that *landed* discharges a
// want: one that fails rolls the part set back and leaves the obligation owed.
func TestRepairCommitFailureKeepsWant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &rejectIndexWrites{Backend: backend.Memory()}
	peer := backend.Memory()

	f := &fakeFetcher{}
	e := newRepairEngine(t, be, f)

	for i, body := range []string{"p1", "p2", "p3"} {
		ingest(t, e, mkBatch("api", rrec{ts: int64(100 * (i + 1)), body: body}))
		require.NoError(t, e.Flush(ctx))
	}

	parts := e.PartPrefixes()
	require.Len(t, parts, 3)

	lost := parts[0]
	copyObjects(t, be, peer, lost)
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	f.answer = func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peer, be, w.Prefix)

		return bucketindex.Entry{Prefix: w.Prefix, MinTime: 100, MaxTime: 100, Blocks: w.Blocks}, bucketindex.WantSatisfied, nil
	}

	be.armed.Store(true)
	require.Error(t, e.Merge(ctx, 0))
	be.armed.Store(false)

	assert.Equal(t, []string{lost}, e.WantPrefixes(),
		"an index write that never landed cannot discharge an obligation")
	assert.NotContains(t, e.PartPrefixes(), lost, "the unpublished part is not in the live set")
}

// TestRepairAsksTheFetcherOncePerCycle pins the batch seam: a cycle's wants go over in one call,
// which is what lets the cluster side read each peer's index once instead of once per want.
func TestRepairAsksTheFetcherOncePerCycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	f := &fakeFetcher{answer: func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{}, bucketindex.WantAbsent, nil
	}}
	e := newRepairEngine(t, be, f)

	for i, body := range []string{"p1", "p2", "p3"} {
		ingest(t, e, mkBatch("api", rrec{ts: int64(100 * (i + 1)), body: body}))
		require.NoError(t, e.Flush(ctx))
	}

	parts := e.PartPrefixes()
	require.Len(t, parts, 3)

	for i, p := range parts {
		dropObjects(t, be, p)
		e.LosePart(p, bucketindex.Interval{Min: uint64(i + 1), Max: uint64(i + 1)})
	}

	require.Len(t, e.WantPrefixes(), len(parts))
	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, 1, f.fetchCalls(), "one batch per cycle, whatever the want count")
	assert.Len(t, f.asks(), len(parts), "every want is still asked for")
}
