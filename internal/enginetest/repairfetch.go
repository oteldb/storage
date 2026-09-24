package enginetest

import (
	"context"
	"slices"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

// flushThree writes api(100, 1), api(200, 2) and api(300, 3) as one part each, returning the part
// prefixes.
func flushThree(t *testing.T, e Engine) []string {
	t.Helper()
	ctx := context.Background()

	for _, r := range []Row{api(100, 1), api(200, 2), api(300, 3)} {
		e.Append(t, r)
		require.NoError(t, e.Flush(ctx))
	}

	parts := e.PartPrefixes()
	require.Len(t, parts, 3)

	return parts
}

// loseOneOfThree flushes three parts, destroys one of them and records its want, returning the
// lost prefix. Two parts survive so the merge the repair rides on still has work to do.
func loseOneOfThree(t *testing.T, e Engine, be backend.Backend) string {
	t.Helper()

	lost := flushThree(t, e)[0]

	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	require.True(t, slices.Contains(e.WantPrefixes(), lost))

	return lost
}

// mergedPeerPart writes api(100, 1) and api(200, 2) as one part on a peer engine over peerBE: the
// level-1 successor a peer that merged both blocks away would hold.
func (k Kind) mergedPeerPart(t *testing.T, peerBE backend.Backend) string {
	t.Helper()

	peer := k.open(t, peerBE)
	peer.Append(t, api(100, 1), api(200, 2))
	require.NoError(t, peer.Flush(context.Background()))

	parts := peer.PartPrefixes()
	require.Len(t, parts, 1)

	return parts[0]
}

// phantom is a part prefix nothing ever writes.
func (k Kind) phantom() string { return k.Prefix + "/00000000000000000000000000" }

// repairFetchesWantedPartFromPeer is the plain repair: the exact part comes back from a peer, is
// readable again, and its want is discharged by the commit that publishes it.
func repairFetchesWantedPartFromPeer(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()

	f := NewFetcher(nil)
	e := k.openRepair(t, be, f)

	lost := flushTwo(t, e)[0]

	CopyObjects(t, be, peer, lost)
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	f.SetAnswer(satisfyFrom(t, peer, be))

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, f.Asks())
	assert.Empty(t, e.WantPrefixes(), "committing the part is what discharges the want")
	assert.Equal(t, int64(1), e.RepairStats().Fetched)
	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream))

	ix := k.loadIndex(t, be)
	assert.Empty(t, ix.Wanted)
	assert.NotEmpty(t, ix.Entries)
}

// repairDischargedByContainingSuccessor is the property that makes repair terminate: the wanted part
// exists nowhere any more, only inside a merged successor, and accepting that successor both
// discharges the want and retires the local parts it contains, so the rows are there exactly once.
func repairDischargedByContainingSuccessor(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be, peerBE := backend.Memory(), backend.Memory()

	f := NewFetcher(nil)
	e := k.openRepair(t, be, f)

	parts := flushTwo(t, e)
	lost, kept := parts[0], parts[1]

	e.SetPartBlocks(lost, bucketindex.Interval{Min: 1, Max: 1}, 0)
	e.SetPartBlocks(kept, bucketindex.Interval{Min: 2, Max: 2}, 0)

	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	// The peer merged both blocks away into one level-1 part; the wanted prefix is gone there too.
	successor := k.mergedPeerPart(t, peerBE)

	f.SetAnswer(func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		CopyObjects(t, peerBE, be, successor)

		return bucketindex.Entry{
			Prefix: successor, MinTime: 100, MaxTime: 200,
			Blocks: bucketindex.Interval{Min: 1, Max: 2}, Level: 1,
		}, bucketindex.WantSatisfied, nil
	})

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, f.Asks(), "repair asks for the want it holds")
	assert.Empty(t, e.WantPrefixes(), "a containing successor discharges the want")
	assert.Equal(t, int64(1), e.RepairStats().Fetched)

	local, err := be.List(ctx, lost)
	require.NoError(t, err)
	assert.Empty(t, local, "the vanished prefix is never fetched — only the successor containing it")

	assert.Equal(t, []string{successor}, e.PartPrefixes(),
		"the superseded local part is retired, not kept alongside the part containing it")
	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream),
		"accepting a containing part must not double-count the rows inside it")
	assert.Empty(t, k.loadIndex(t, be).Wanted)
}

// repairDischargedByLocalPart: a want this node's own index already covers costs no fetch at all.
func repairDischargedByLocalPart(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()

	f := NewFetcher(nil)
	e := k.openRepair(t, be, f)

	parts := flushTwo(t, e)

	// The surviving part already covers the wanted blocks at a higher level.
	e.SetPartBlocks(parts[1], bucketindex.Interval{Min: 1, Max: 4}, 1)

	dropObjects(t, be, parts[0])
	e.LosePart(parts[0], bucketindex.Interval{Min: 1, Max: 1})

	require.NoError(t, e.Merge(ctx, 0))

	assert.Empty(t, f.Asks(), "no network call for a want the local index covers")
	assert.Empty(t, e.WantPrefixes())
	assert.Equal(t, int64(1), e.RepairStats().Local)
	assert.Equal(t, []Row{api(200, 2)}, rows(t, e, apiStream))
}

// repairNoPeerLeavesWant: definitive absence leaves the obligation, counts it, and the merge that
// carried the repair still compacts.
func repairNoPeerLeavesWant(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.openRepair(t, be, AnswerAlways(bucketindex.WantAbsent, nil))

	lost := loseOneOfThree(t, e, be)

	require.NoError(t, e.Merge(ctx, 0), "a repair that cannot proceed must not block compaction")

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Equal(t, int64(1), e.RepairStats().Unsatisfiable)
	assert.Less(t, len(e.PartPrefixes()), 2, "the merge still compacted the parts that are here")

	wanted := k.loadIndex(t, be).Wanted
	require.Len(t, wanted, 1)
	assert.Equal(t, lost, wanted[0].Prefix, "the obligation is durable, not only in memory")
}

// repairTransientFailureKeepsWant: an unreachable peer leaves the want intact and the index
// consistent, and the next cycle retries.
func repairTransientFailureKeepsWant(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.openRepair(t, be, AnswerAlways(bucketindex.WantIncomplete, errors.New("peer unreachable")))

	lost := loseOneOfThree(t, e, be)

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Equal(t, int64(1), e.RepairStats().Failed)
	assert.Zero(t, e.RepairStats().Unsatisfiable, "a peer we could not ask is not evidence of absence")

	require.NoError(t, k.open(t, be).LoadParts(ctx), "the index must still name only parts that are here")

	require.NoError(t, e.Merge(ctx, 0))
	assert.Equal(t, []string{lost}, e.WantPrefixes(), "the obligation is retried, not forgotten")
	assert.Equal(t, int64(2), e.RepairStats().Failed)
}

// repairWithoutCallbackIsNoOp: in single-node mode, with no cluster to pull from, a merge runs as it
// always does and the want is left alone.
func repairWithoutCallbackIsNoOp(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.open(t, be)

	lost := loseOneOfThree(t, e, be)

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Equal(t, RepairStats{}, e.RepairStats())
}

// repairUnreadablePartKeepsWant covers the half-copied case: the objects arrived but the part will
// not open, so nothing is published and the obligation is still owed.
func repairUnreadablePartKeepsWant(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()

	f := NewFetcher(nil)
	e := k.openRepair(t, be, f)

	lost := loseOneOfThree(t, e, be)

	f.SetAnswer(truncatedCopy(t, be))

	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Zero(t, e.RepairStats().Fetched, "a part that will not open was not repaired")
	assert.Equal(t, int64(1), e.RepairStats().Failed)
}

// truncatedCopy answers a want by "copying" a part whose manifest is truncated.
func truncatedCopy(t *testing.T, be backend.Backend) Answer {
	t.Helper()

	return func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		require.NoError(t, be.Write(context.Background(), w.Prefix+"/manifest", []byte("truncated")))

		return bucketindex.Entry{Prefix: w.Prefix, Blocks: w.Blocks}, bucketindex.WantSatisfied, nil
	}
}

// repairCommitFailureKeepsWant: only an index write that landed discharges a want. One that fails
// rolls the part set back and leaves the obligation owed.
func repairCommitFailureKeepsWant(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	peer := backend.Memory()

	f := NewFetcher(nil)
	e := k.openRepair(t, be, f)

	lost := flushThree(t, e)[0]
	CopyObjects(t, be, peer, lost)
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	f.SetAnswer(satisfyFrom(t, peer, be))

	rejectWrites(be, "/"+bucketindex.Object, errWriteRejected)
	require.Error(t, e.Merge(ctx, 0))
	be.Reset()

	assert.Equal(t, []string{lost}, e.WantPrefixes(), "an index write that never landed cannot discharge an obligation")
	assert.NotContains(t, e.PartPrefixes(), lost, "the unpublished part is not in the live set")
}

// repairAsksTheFetcherOncePerCycle pins the batch seam: a cycle's wants go over in one call, which
// is what lets the cluster side read each peer's index once instead of once per want.
func repairAsksTheFetcherOncePerCycle(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()

	f := AnswerAlways(bucketindex.WantAbsent, nil)
	e := k.openRepair(t, be, f)

	parts := flushThree(t, e)
	for i, p := range parts {
		dropObjects(t, be, p)
		e.LosePart(p, bucketindex.Interval{Min: uint64(i + 1), Max: uint64(i + 1)})
	}

	require.Len(t, e.WantPrefixes(), len(parts))
	require.NoError(t, e.Merge(ctx, 0))

	assert.Equal(t, 1, f.Calls(), "one batch per cycle, whatever the want count")
	assert.Len(t, f.Asks(), len(parts), "every want is still asked for")
}

// repairCoveredWantIsNotAFailure covers an accounting hazard (#577): two wants answered by one merged
// successor arrive as two entries when the peer names each separately, and the second names a copy
// nothing brought in. Opening it fails, correctly, but the want it stands for is already discharged
// by the successor committed alongside it, so nothing failed and there is nothing to retry.
func repairCoveredWantIsNotAFailure(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be, peerBE := backend.Memory(), backend.Memory()

	f := NewFetcher(nil)
	e := k.openRepair(t, be, f)

	parts := flushThree(t, e)
	first, second, kept := parts[0], parts[1], parts[2]

	e.SetPartBlocks(first, bucketindex.Interval{Min: 1, Max: 1}, 0)
	e.SetPartBlocks(second, bucketindex.Interval{Min: 2, Max: 2}, 0)
	e.SetPartBlocks(kept, bucketindex.Interval{Min: 3, Max: 3}, 0)

	for _, lost := range []string{first, second} {
		dropObjects(t, be, lost)
	}

	e.LosePart(first, bucketindex.Interval{Min: 1, Max: 1})
	e.LosePart(second, bucketindex.Interval{Min: 2, Max: 2})

	successor := k.mergedPeerPart(t, peerBE)
	phantom := k.phantom()

	// The peer answers the lower want with the successor it actually copies, and the higher one with
	// an entry whose objects it never writes — the shape a peer produces when it names each want's
	// own prefix and one copy is retired underneath the pass.
	f.SetAnswer(func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		if w.Prefix == first {
			CopyObjects(t, peerBE, be, successor)

			return bucketindex.Entry{
				Prefix: successor, MinTime: 100, MaxTime: 200,
				Blocks: bucketindex.Interval{Min: 1, Max: 2}, Level: 1,
			}, bucketindex.WantSatisfied, nil
		}

		return bucketindex.Entry{
			Prefix: phantom, MinTime: 200, MaxTime: 200,
			Blocks: bucketindex.Interval{Min: 2, Max: 2},
		}, bucketindex.WantSatisfied, nil
	})

	require.NoError(t, e.Merge(ctx, 0))

	assert.Zero(t, e.RepairStats().Failed, "a want the same commit already covers is not a transient failure")
	assert.Empty(t, e.WantPrefixes(), "the successor discharges both wants")
	assert.NotContains(t, e.PartPrefixes(), phantom, "the unreadable copy is never committed")
	assert.Empty(t, k.loadIndex(t, be).Wanted, "the committed index carries no outstanding want")
	assert.Equal(t, []Row{api(100, 1), api(200, 2), api(300, 3)}, rows(t, e, apiStream),
		"no rows are lost or double-counted")
}
