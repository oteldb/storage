package enginetest

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// holeCommittedAfterRepeatedAbsence is the happy path: a want no owner can satisfy, concluded over
// the complete owner set on holeConfirmations consecutive passes, becomes a hole — committed with
// the want discharged and the data-loss counter raised, in one index write.
func holeCommittedAfterRepeatedAbsence(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()
	e := k.openRepair(t, be, AnswerAlways(bucketindex.WantAbsent, nil))

	lost := loseFirstOfTwo(t, e, be)

	// Short of the confirmation bar the obligation stands: one absent answer is a snapshot.
	mergeTimes(t, e, 2)
	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Empty(t, e.Holes())
	assert.Zero(t, e.LostParts())

	mergeTimes(t, e, 1)

	assert.Empty(t, e.WantPrefixes(), "the hole discharges the want, so reads resume")
	assert.EqualValues(t, 1, e.LostParts())
	assert.Equal(t, int64(1), e.RepairStats().Lost)

	holes := e.Holes()
	require.Len(t, holes, 1)
	assert.Equal(t, lost, holes[0].Prefix)
	assert.True(t, holes[0].Hole)

	// The whole point of the flag: the committed index says "acknowledged loss", not "empty part".
	ix := k.loadIndex(t, be)
	assert.EqualValues(t, 1, ix.LostParts)
	assert.Empty(t, ix.Wanted)

	committed := ix.Holes()
	require.Len(t, committed, 1)
	assert.Equal(t, lost, committed[0].Prefix)

	for i := range ix.Entries {
		if ix.Entries[i].Prefix != lost {
			assert.False(t, ix.Entries[i].Hole, "a real part is never marked as a hole")
		}
	}

	st := e.Stats()
	assert.Equal(t, 1, st.Holes)
	assert.Zero(t, st.WantedParts)
	assert.EqualValues(t, 1, st.LostParts)
	assert.NotContains(t, e.PartPrefixes(), lost, "a hole is not a part")
}

// incompletePeerSetNeverHoles is the guard that matters most: absence observed over a strict subset
// of the shard's owners is not absence. A rolling restart makes the peer list a subset for as long
// as it runs, and a hole committed over live data is unrecoverable in a way a want never is.
func incompletePeerSetNeverHoles(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()
	e := k.openRepair(t, be, AnswerAlways(bucketindex.WantIncomplete, nil))

	lost := loseFirstOfTwo(t, e, be)

	mergeTimes(t, e, 3*3)

	assert.Equal(t, []string{lost}, e.WantPrefixes(), "the obligation outlives every incomplete pass")
	assert.Empty(t, e.Holes())
	assert.Zero(t, e.LostParts())
	assert.Zero(t, e.RepairStats().Lost)
	assert.Zero(t, e.RepairStats().Unsatisfiable)
	assert.Greater(t, e.RepairStats().Incomplete, int64(2))

	ix := k.loadIndex(t, be)
	assert.Empty(t, ix.Holes())
	assert.Len(t, ix.Wanted, 1)
	assert.Zero(t, ix.LostParts)
}

// transientFailureNeverHoles: an unreachable peer is an error, and an error says nothing about
// whether the data exists.
func transientFailureNeverHoles(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()
	e := k.openRepair(t, be, AnswerAlways(bucketindex.WantIncomplete, errors.New("peer unreachable")))

	lost := loseFirstOfTwo(t, e, be)

	mergeTimes(t, e, 3*3)

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Empty(t, e.Holes())
	assert.Zero(t, e.LostParts())
	assert.Greater(t, e.RepairStats().Failed, int64(2))
}

// absenceEvidenceResetsOnAnyOtherOutcome: the confirmations must be consecutive, so an incomplete
// pass in the middle of a run of absent ones starts the count over.
func absenceEvidenceResetsOnAnyOtherOutcome(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()

	outcome := bucketindex.WantAbsent
	e := k.openRepair(t, be, NewFetcher(func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{}, outcome, nil
	}))

	lost := loseFirstOfTwo(t, e, be)

	mergeTimes(t, e, 2)
	outcome = bucketindex.WantIncomplete
	mergeTimes(t, e, 1)
	outcome = bucketindex.WantAbsent
	mergeTimes(t, e, 2)

	assert.Equal(t, []string{lost}, e.WantPrefixes(), "the interrupted run does not add up")
	assert.Empty(t, e.Holes())

	mergeTimes(t, e, 1)
	assert.Len(t, e.Holes(), 1, "three uninterrupted passes do")
}

// holeRevokedByExactPrefix is the revocability the design requires: the commit is not cross-replica
// atomic, so an owner may acknowledge a loss while a peer still holds the part. When it turns up,
// the hole is replaced rather than blocking it.
func holeRevokedByExactPrefix(t *testing.T, k Kind) {
	t.Helper()

	be, peer := backend.Memory(), backend.Memory()

	f := AnswerAlways(bucketindex.WantAbsent, nil)
	e := k.openRepair(t, be, f)

	lost := flushTwo(t, e)[0]

	CopyObjects(t, be, peer, lost)
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	mergeTimes(t, e, 3)
	require.Len(t, e.Holes(), 1)

	// The peer that was restarting comes back holding the part after all.
	f.SetAnswer(satisfyFrom(t, peer, be))

	mergeTimes(t, e, 1)

	assert.Empty(t, e.Holes(), "the real part replaces the hole")
	assert.Equal(t, int64(1), e.RepairStats().Revoked)
	assert.EqualValues(t, 1, e.LostParts(), "the loss counter is monotone: a revoked hole is still a fact")
	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream),
		"the rows the hole stood for are readable again")

	ix := k.loadIndex(t, be)
	assert.Empty(t, ix.Holes())
	assert.EqualValues(t, 1, ix.LostParts)
}

// holeRevokedByLostSuccessor: the successor containing the hole's blocks at a higher level was lost
// here too, and comes back from a peer that kept it.
func holeRevokedByLostSuccessor(t *testing.T, k Kind) {
	t.Helper()

	be, peer := backend.Memory(), backend.Memory()

	f := AnswerAlways(bucketindex.WantAbsent, nil)
	e := k.openRepair(t, be, f)

	parts := flushTwo(t, e)
	lost, successor := parts[0], parts[1]

	e.SetPartBlocks(successor, bucketindex.Interval{Min: 1, Max: 4}, 2)
	CopyObjects(t, be, peer, successor)
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	// The successor is local, so it would discharge the want before a hole could form: take it out
	// of the way and let the loss be acknowledged first.
	e.LosePart(successor, bucketindex.Interval{Min: 1, Max: 4})
	dropObjects(t, be, successor)

	mergeTimes(t, e, 3)
	require.Len(t, e.Holes(), 2)

	f.SetAnswer(func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		CopyObjects(t, peer, be, successor)

		return bucketindex.Entry{
			Prefix: successor, MinTime: 200, MaxTime: 200,
			Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 2,
		}, bucketindex.WantSatisfied, nil
	})

	mergeTimes(t, e, 1)

	assert.Empty(t, e.Holes(), "a part containing the hole's blocks at a higher level replaces it, exact prefix or not")
	assert.Equal(t, []string{successor}, e.PartPrefixes(), "one successor discharges both holes, and is committed once")
	assert.EqualValues(t, 2, e.LostParts())
}

// holeRevokedByContainingSuccessor: by the time an owner finds the data, it exists only inside a
// part a peer merged from both lost blocks.
func holeRevokedByContainingSuccessor(t *testing.T, k Kind) {
	t.Helper()

	be, peerBE := backend.Memory(), backend.Memory()

	f := AnswerAlways(bucketindex.WantAbsent, nil)
	e := k.openRepair(t, be, f)

	parts := flushTwo(t, e)
	for i, p := range parts {
		e.SetPartBlocks(p, bucketindex.Interval{Min: uint64(i + 1), Max: uint64(i + 1)}, 0)
	}

	for _, p := range parts {
		dropObjects(t, be, p)
	}

	e.LosePart(parts[0], bucketindex.Interval{Min: 1, Max: 1})
	e.LosePart(parts[1], bucketindex.Interval{Min: 2, Max: 2})

	// The peer merged both blocks away into one level-1 part.
	successor := k.mergedPeerPart(t, peerBE)

	mergeTimes(t, e, 3)
	require.Len(t, e.Holes(), 2)

	f.SetAnswer(func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		CopyObjects(t, peerBE, be, successor)

		return bucketindex.Entry{
			Prefix: successor, MinTime: 100, MaxTime: 200,
			Blocks: bucketindex.Interval{Min: 1, Max: 2}, Level: 1,
		}, bucketindex.WantSatisfied, nil
	})

	mergeTimes(t, e, 1)

	assert.Empty(t, e.Holes(), "a part containing the holes' blocks at a higher level replaces them, exact prefix or not")
	assert.Equal(t, []string{successor}, e.PartPrefixes(), "one successor discharges both holes, and is committed once")
	assert.Equal(t, []Row{api(100, 1), api(200, 2)}, rows(t, e, apiStream))
	assert.EqualValues(t, 2, e.LostParts())
}

// holeSurvivesReload: a hole is durable state, not a per-process one. A fresh engine over the prefix
// reads it back, and does not try to open the objects it does not have.
func holeSurvivesReload(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	e := k.openRepair(t, be, AnswerAlways(bucketindex.WantAbsent, nil))

	lost := loseFirstOfTwo(t, e, be)
	mergeTimes(t, e, 3)
	require.Len(t, e.Holes(), 1)

	r := k.open(t, be)
	require.NoError(t, r.LoadParts(ctx))

	holes := r.Holes()
	require.Len(t, holes, 1)
	assert.Equal(t, lost, holes[0].Prefix)
	assert.EqualValues(t, 1, r.LostParts())
	assert.Empty(t, r.WantPrefixes(), "a reload must not read the hole back as a fresh obligation")
	assert.NotContains(t, r.PartPrefixes(), lost)

	// A commit by the reloaded engine carries the hole and the count through unchanged.
	require.NoError(t, r.Merge(ctx, 0))

	ix := k.loadIndex(t, be)
	assert.Len(t, ix.Holes(), 1)
	assert.EqualValues(t, 1, ix.LostParts)
}

// holeNotOfferedToAPeer: an acknowledged loss cannot spread. Asked for a part it holds only a hole
// for, an index answers with nothing.
func holeNotOfferedToAPeer(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()
	e := k.openRepair(t, be, AnswerAlways(bucketindex.WantAbsent, nil))

	lost := loseFirstOfTwo(t, e, be)
	mergeTimes(t, e, 3)

	_, ok := k.loadIndex(t, be).Satisfying(bucketindex.Want{Prefix: lost, Blocks: bucketindex.Interval{Min: 1, Max: 1}})
	assert.False(t, ok)
}

// repairStatsSurfaceLoss: the operator surface distinguishes what is still owed, what stands
// acknowledged, and what was ever lost.
func repairStatsSurfaceLoss(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()
	e := k.openRepair(t, be, AnswerAlways(bucketindex.WantIncomplete, nil))

	loseFirstOfTwo(t, e, be)
	mergeTimes(t, e, 1)

	st := e.Stats()
	assert.Equal(t, 1, st.WantedParts)
	assert.Zero(t, st.Holes)
	assert.Zero(t, st.LostParts)
}
