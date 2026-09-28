package enginetest

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
)

// splitBrainMergeResolvesToTheLaterTenure is the other order of splitBrainMergeSameInputs: the
// displaced tenure A still holds its claim when it commits its merge, and B's merge of the same two
// parts lands second. B's rebase adopts A's output, finds its own output to be the same identity
// written by a later tenure, and retires A's rather than keeping both.
func splitBrainMergeResolvesToTheLaterTenure(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	inner := backend.Memory()
	bBe := faultbackend.Wrap(inner)
	want := []Row{api(100, 1), api(200, 2)}

	a := k.openTenure(t, inner, displacedTerm, 0)
	k.flushEach(t, a, inner, want...)

	b := k.openTenure(t, bBe, ownerTerm, 0)
	commitB := stallMerge(t, b, gateIndexCommit(bBe))

	require.NoError(t, a.ForceMerge(ctx))
	require.NoError(t, commitB())

	k.requireStoredOnce(t, inner, want)

	ix := k.loadIndex(t, inner)
	require.Len(t, ix.Entries, 1)
	assert.EqualValues(t, ownerTerm, ix.Entries[0].Term, "the later tenure's output is the one kept")
	assert.Len(t, ix.Removed, 3, "both inputs and the earlier output are tombstoned, not dropped silently")
}

// tenuresAllocateDisjointBlocks is #725's block collision: two tenures whose indexes diverged — a
// displaced owner and its successor, each on its own copy — both flush the next part. Allocation is
// scoped to the tenure's term, so the two parts never share an identity, and a later merge of both
// supersedes each of them.
func tenuresAllocateDisjointBlocks(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	aBe, bBe := backend.Memory(), backend.Memory()

	a := k.openTenure(t, aBe, displacedTerm, 0)
	k.flushEach(t, a, aBe, api(100, 1))

	copyObjects(ctx, t, aBe, bBe, k.Prefix)
	b := k.openTenure(t, bBe, ownerTerm, 0)

	a.Append(t, api(200, 2))
	require.NoError(t, a.Flush(ctx))
	b.Append(t, api(300, 3))
	require.NoError(t, b.Flush(ctx))

	fresh := func(be backend.Backend) bucketindex.Entry {
		t.Helper()

		ix := k.loadIndex(t, be)
		require.Len(t, ix.Entries, 2)

		return ix.Entries[1]
	}

	pa, pb := fresh(aBe), fresh(bBe)
	t.Logf("displaced %v, successor %v", pa.Blocks, pb.Blocks)
	assert.Equal(t, bucketindex.TermBlocks(displacedTerm, 2), pa.Blocks)
	assert.Equal(t, bucketindex.TermBlocks(ownerTerm, 1), pb.Blocks, "a new tenure numbers under its own term")
	assert.False(t, bucketindex.LineageOf(nil).Overlaps(pa, pb), "the two parts share no identity")

	require.NoError(t, b.ForceMerge(ctx))
	merged := k.loadIndex(t, bBe).Entries
	require.Len(t, merged, 1)
	assert.True(t, merged[0].Blocks.Contains(pb.Blocks))
	assert.False(t, merged[0].Blocks.Contains(pa.Blocks), "the successor never merged the displaced part")
}

// writingTermSurvivesTheNextTenure pins that a part's writing term is part of its identity, not of
// the engine that holds it: a later tenure loading the index and committing its own part carries
// every earlier entry's term through unchanged.
func writingTermSurvivesTheNextTenure(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()

	a := k.openTenure(t, be, displacedTerm, 0)
	k.flushEach(t, a, be, api(100, 1))

	b := k.openTenure(t, be, ownerTerm, 0)
	b.Append(t, api(200, 2))
	require.NoError(t, b.Flush(ctx))

	ix := k.loadIndex(t, be)
	require.Len(t, ix.Entries, 2)

	terms := map[uint64]bucketindex.Interval{}
	for i := range ix.Entries {
		terms[ix.Entries[i].Term] = ix.Entries[i].Blocks
	}

	assert.Equal(t, map[uint64]bucketindex.Interval{
		displacedTerm: bucketindex.TermBlocks(displacedTerm, 1),
		ownerTerm:     bucketindex.TermBlocks(ownerTerm, 1),
	}, terms)
	assert.Equal(t, bucketindex.Block{Term: ownerTerm, N: 1}, ix.AllocatedBlocks)
}

// lapsedClaimCommitsNothing is the commit fence over a writer's own term: a flush while the claim is
// gone is refused before it writes anything and keeps its rows in the head, and a merge whose tenure
// ended and restarted while it ran is refused too, since it chose its inputs under a tenure that is
// over.
func lapsedClaimCommitsNothing(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	inner := backend.Memory()
	be := faultbackend.Wrap(inner)

	var term atomic.Uint64

	term.Store(1)

	e := k.Open(t, Config{Backend: be, Term: term.Load})
	require.NoError(t, e.LoadParts(ctx))
	k.flushEach(t, e, inner, api(100, 1), api(200, 2))

	term.Store(0)
	e.Append(t, api(300, 3))
	require.ErrorIs(t, e.Flush(ctx), bucketindex.ErrSuperseded, "no claim, no commit")
	assert.Len(t, k.loadIndex(t, inner).Entries, 2)
	assert.Len(t, k.partDirs(ctx, t, inner), 2, "and no part written for it")
	assert.Equal(t, 1, e.HeadRows(), "the rows stay in the head")

	term.Store(3)
	e.Append(t, api(400, 4))
	require.NoError(t, e.Flush(ctx), "the head flushes once the claim is back")
	assert.EqualValues(t, 3, k.loadIndex(t, inner).Generation.Term)
	assert.Len(t, k.loadIndex(t, inner).Entries, 3)

	// Held while it writes its output, so the tenure ends and restarts before it reaches the commit.
	gate := faultbackend.NewGate()
	be.Add(gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool {
		return !strings.HasSuffix(op.Key, "/"+bucketindex.Object)
	}))

	commit := stallMerge(t, e, gate)
	term.Store(4)
	require.ErrorIs(t, commit(), bucketindex.ErrSuperseded, "a merge that outlived its tenure commits nothing")

	assert.Len(t, k.loadIndex(t, inner).Entries, 3)
	assert.Equal(t, []Row{api(100, 1), api(200, 2), api(300, 3), api(400, 4)}, sortedRows(t, e))
}
