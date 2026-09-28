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
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/internal/obs/obstest"
)

// staleCommitCannotLandAfterTheSuccessorEstablishes is the check-to-CAS window: A's merge passes its
// tenure check and is held inside the index CAS while the claim moves to B. B's first write is the
// commit that establishes its tenure, which changes the index's version, so A's held CAS can only
// fail, rebase into B's term, and be refused — whatever A's local view of its claim still says.
func staleCommitCannotLandAfterTheSuccessorEstablishes(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	inner := backend.Memory()
	aBe := faultbackend.Wrap(inner)
	want := []Row{api(100, 1), api(200, 2), api(300, 3)}

	a := k.openTenure(t, aBe, displacedTerm, 0)
	k.flushEach(t, a, inner, want[:2]...)

	commitA := stallMerge(t, a, gateIndexCommit(aBe))

	b := k.openTenure(t, inner, ownerTerm, 0)
	b.Append(t, want[2])
	require.NoError(t, b.Flush(ctx))
	require.EqualValues(t, ownerTerm, k.loadIndex(t, inner).Generation.Term)

	require.ErrorIs(t, commitA(), bucketindex.ErrSuperseded, "the held commit lands against a moved index")

	ix := k.loadIndex(t, inner)
	assert.Len(t, ix.Entries, 3, "A's output never became live")
	k.requireStoredOnce(t, inner, want)
}

// predecessorCommitLandsBeforeTheSuccessorEstablishes is the other side of the same window: A's held
// merge lands after B took the claim but before B wrote anything. B establishes its tenure over a
// fresh load of the index, not a rebase of the view it held as a replica, so A's merge is B's starting
// point — the inputs A consumed are not carried back in beside A's output.
func predecessorCommitLandsBeforeTheSuccessorEstablishes(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	inner := backend.Memory()
	aBe := faultbackend.Wrap(inner)
	want := []Row{api(100, 1), api(200, 2), api(300, 3)}

	a := k.openTenure(t, aBe, displacedTerm, 0)
	k.flushEach(t, a, inner, want[:2]...)

	commitA := stallMerge(t, a, gateIndexCommit(aBe))

	b := k.openTenure(t, inner, ownerTerm, 0)
	require.Equal(t, 2, b.PartCount(), "B's view is the two inputs")

	require.NoError(t, commitA(), "A's merge lands before any write of B's tenure")

	b.Append(t, want[2])
	require.NoError(t, b.Flush(ctx))

	ix := k.loadIndex(t, inner)
	assert.EqualValues(t, ownerTerm, ix.Generation.Term)
	assert.Len(t, ix.Entries, 2, "A's output and B's flush, and not A's inputs again")
	k.requireStoredOnce(t, inner, want)
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

// unestablishedTenureKeepsRowsInTheHead: until a new tenure's first commit lands, nothing of it
// commits. Each flush and merge makes one attempt — one CAS, no retry loop — and one that cannot land,
// whether it lost the race or the backend failed, is counted, fails without writing a part and keeps
// the rows readable in the head. The first attempt that lands establishes the tenure and the same
// flush publishes every held row.
func unestablishedTenureKeepsRowsInTheHead(t *testing.T, k Kind) {
	t.Helper()

	const cycles = 3

	ctx := context.Background()
	inner := backend.Memory()
	be := faultbackend.Wrap(inner)
	o, m := obstest.New(t)

	e := k.Open(t, Config{Backend: be, Term: termOf(ownerTerm), Obs: o})
	require.NoError(t, e.LoadParts(ctx))

	held := []Row{api(100, 1), api(200, 2)}
	e.Append(t, held...)

	commits := func() int { return be.Count(k.indexCommit) }

	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Lose: true})

	for i := range cycles {
		before := commits()
		require.ErrorIsf(t, e.Flush(ctx), bucketindex.ErrConflict, "cycle %d", i)
		require.ErrorIsf(t, e.ForceMerge(ctx), bucketindex.ErrConflict, "cycle %d", i)
		assert.Equal(t, before+2, commits(), "one attempt per flush and one per merge")
	}

	assert.EqualValues(t, 2*cycles,
		m.Counter("storage.index.establish_failures", "reason", obs.EstablishConflict))

	be.Reset()
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Err: assert.AnError})

	for range cycles {
		require.ErrorIs(t, e.Flush(ctx), assert.AnError)
	}

	assert.EqualValues(t, cycles,
		m.Counter("storage.index.establish_failures", "reason", obs.EstablishUnavailable))
	assert.True(t, e.Stats().TenureUnestablished)
	assert.Empty(t, k.partDirs(ctx, t, inner), "no part written for a tenure that could not establish")
	assert.Equal(t, len(held), e.HeadRows())
	assert.Equal(t, held, sortedRows(t, e), "the held rows stay readable from the head")

	be.Reset()
	e.Append(t, api(300, 3))
	require.NoError(t, e.Flush(ctx))

	ix := k.loadIndex(t, inner)
	require.Len(t, ix.Entries, 1)
	assert.Equal(t, bucketindex.Generation{Term: ownerTerm, Counter: 2}, ix.Generation,
		"the establishing commit, then the flush")
	assert.False(t, e.Stats().TenureUnestablished)
	assert.Zero(t, e.HeadRows())
	assert.Equal(t, []Row{api(100, 1), api(200, 2), api(300, 3)}, sortedRows(t, e))
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
