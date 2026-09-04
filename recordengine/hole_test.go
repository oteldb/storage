package recordengine_test

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/recordengine"
)

// answerAlways builds a fetcher that returns the same outcome to every ask.
func answerAlways(outcome bucketindex.WantOutcome, err error) *fakeFetcher {
	return &fakeFetcher{answer: func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{}, outcome, err
	}}
}

// flushParts writes n one-record parts, returning their prefixes.
func flushParts(t *testing.T, e *recordengine.Engine, n int) []string {
	t.Helper()
	ctx := context.Background()

	for i := range n {
		ingest(t, e, mkBatch("api", rrec{ts: int64(100 * (i + 1)), body: "p" + string(rune('1'+i))}))
		require.NoError(t, e.Flush(ctx))
	}

	parts := e.PartPrefixes()
	require.Len(t, parts, n)

	return parts
}

// loseFirstOfTwo flushes two parts, destroys the first, records the want its absence owes and
// commits that loss, returning the lost prefix.
func loseFirstOfTwo(t *testing.T, e *recordengine.Engine, be backend.Backend) string {
	t.Helper()

	parts := flushParts(t, e, 2)
	lost := parts[0]

	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	ingest(t, e, mkBatch("api", rrec{ts: 300, body: "p3"}))
	require.NoError(t, e.Flush(context.Background()))

	return lost
}

// mergeTimes runs n maintenance merges, which is how many repair passes the engine gets.
func mergeTimes(t *testing.T, e *recordengine.Engine, n int) {
	t.Helper()

	for range n {
		require.NoError(t, e.Merge(context.Background(), 0))
	}
}

// TestHoleCommittedAfterRepeatedAbsence is Decision 6's happy path: a want no owner can satisfy,
// concluded over the complete owner set on holeConfirmations consecutive passes, becomes a hole —
// committed with the want discharged and the data-loss counter raised, in one index write.
func TestHoleCommittedAfterRepeatedAbsence(t *testing.T) {
	t.Parallel()

	be := backend.Memory()

	f := answerAlways(bucketindex.WantAbsent, nil)
	e := newRepairEngine(t, be, f)

	lost := loseFirstOfTwo(t, e, be)

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

	ix := committedIndex(t, be)
	assert.EqualValues(t, 1, ix.LostParts)
	assert.Empty(t, ix.Wanted)

	committed := ix.Holes()
	require.Len(t, committed, 1)
	assert.Equal(t, lost, committed[0].Prefix)

	for _, ent := range ix.Entries {
		if ent.Prefix != lost {
			assert.False(t, ent.Hole, "a real part is never marked as a hole")
		}
	}

	st := e.Stats()
	assert.Equal(t, 1, st.Holes)
	assert.Zero(t, st.WantedParts)
	assert.EqualValues(t, 1, st.LostParts)
	assert.NotContains(t, e.PartPrefixes(), lost, "a hole is not a part")
}

// TestIncompletePeerSetNeverHoles is the guard that matters most: absence observed over a strict
// subset of the shard's owners is not absence. A rolling restart makes the peer list a subset for
// as long as it runs, and a hole committed over live data is unrecoverable in a way an outstanding
// want never is.
func TestIncompletePeerSetNeverHoles(t *testing.T) {
	t.Parallel()

	be := backend.Memory()

	f := answerAlways(bucketindex.WantIncomplete, nil)
	e := newRepairEngine(t, be, f)

	lost := loseFirstOfTwo(t, e, be)

	mergeTimes(t, e, 3*3)

	assert.Equal(t, []string{lost}, e.WantPrefixes(), "the obligation outlives every incomplete pass")
	assert.Empty(t, e.Holes())
	assert.Zero(t, e.LostParts())
	assert.Zero(t, e.RepairStats().Lost)
	assert.Zero(t, e.RepairStats().Unsatisfiable)
	assert.Greater(t, e.RepairStats().Incomplete, int64(2))

	ix := committedIndex(t, be)
	assert.Empty(t, ix.Holes())
	assert.Len(t, ix.Wanted, 1)
	assert.Zero(t, ix.LostParts)
}

// TestTransientFailureNeverHoles asserts the other half of the contract where it is consumed: an
// unreachable peer is an error, and an error says nothing about whether the data exists.
func TestTransientFailureNeverHoles(t *testing.T) {
	t.Parallel()

	be := backend.Memory()

	f := answerAlways(bucketindex.WantIncomplete, errors.New("peer unreachable"))
	e := newRepairEngine(t, be, f)

	lost := loseFirstOfTwo(t, e, be)

	mergeTimes(t, e, 3*3)

	assert.Equal(t, []string{lost}, e.WantPrefixes())
	assert.Empty(t, e.Holes())
	assert.Zero(t, e.LostParts())
	assert.Greater(t, e.RepairStats().Failed, int64(2))
}

// TestAbsenceEvidenceResetsOnAnyOtherOutcome verifies the confirmations must be consecutive: an
// incomplete pass in the middle of a run of absent ones starts the count over.
func TestAbsenceEvidenceResetsOnAnyOtherOutcome(t *testing.T) {
	t.Parallel()

	be := backend.Memory()

	outcome := bucketindex.WantAbsent
	f := &fakeFetcher{answer: func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{}, outcome, nil
	}}
	e := newRepairEngine(t, be, f)

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

// TestHoleRevokedByExactPrefix is the revocability the design requires: the commit is not
// cross-replica atomic, so an owner may acknowledge a loss while a peer still holds the part. When
// it turns up, the hole is replaced rather than blocking it.
func TestHoleRevokedByExactPrefix(t *testing.T) {
	t.Parallel()

	be, peerBE := backend.Memory(), backend.Memory()

	f := answerAlways(bucketindex.WantAbsent, nil)
	e := newRepairEngine(t, be, f)

	parts := flushParts(t, e, 2)
	lost := parts[0]

	copyObjects(t, be, peerBE, lost)
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	mergeTimes(t, e, 3)
	require.Len(t, e.Holes(), 1)

	f.answer = func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peerBE, be, w.Prefix)

		return bucketindex.Entry{
			Prefix: w.Prefix, MinTime: 100, MaxTime: 100, Blocks: w.Blocks,
		}, bucketindex.WantSatisfied, nil
	}

	mergeTimes(t, e, 1)

	assert.Empty(t, e.Holes(), "the real part replaces the hole")
	assert.Equal(t, int64(1), e.RepairStats().Revoked)
	assert.EqualValues(t, 1, e.LostParts(), "the loss counter is monotone: a revoked hole is still a fact")
	assert.ElementsMatch(t, []string{"p1", "p2"}, streamBodies(t, e),
		"the rows the hole stood for are readable again")

	ix := committedIndex(t, be)
	assert.Empty(t, ix.Holes())
	assert.EqualValues(t, 1, ix.LostParts)
}

// TestHoleRevokedByContainingSuccessor covers the other way the data comes back: by the time an
// owner finds it, it exists only inside a merged part covering the hole's blocks.
func TestHoleRevokedByContainingSuccessor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be, peerBE := backend.Memory(), backend.Memory()

	f := answerAlways(bucketindex.WantAbsent, nil)
	e := newRepairEngine(t, be, f)

	parts := flushParts(t, e, 2)
	for i, p := range parts {
		e.SetPartBlocks(p, bucketindex.Interval{Min: uint64(i + 1), Max: uint64(i + 1)}, 0)
	}

	for _, p := range parts {
		dropObjects(t, be, p)
	}

	e.LosePart(parts[0], bucketindex.Interval{Min: 1, Max: 1})
	e.LosePart(parts[1], bucketindex.Interval{Min: 2, Max: 2})

	// The peer merged both blocks away into one level-1 part.
	peer := newRepairEngine(t, peerBE, nil)
	ingest(t, peer, mkBatch("api", rrec{ts: 100, body: "p1"}))
	ingest(t, peer, mkBatch("api", rrec{ts: 200, body: "p2"}))
	require.NoError(t, peer.Flush(ctx))

	peerParts := peer.PartPrefixes()
	require.Len(t, peerParts, 1)
	successor := peerParts[0]

	mergeTimes(t, e, 3)
	require.Len(t, e.Holes(), 2)

	f.answer = func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peerBE, be, successor)

		return bucketindex.Entry{
			Prefix: successor, MinTime: 100, MaxTime: 200,
			Blocks: bucketindex.Interval{Min: 1, Max: 2}, Level: 1,
		}, bucketindex.WantSatisfied, nil
	}

	mergeTimes(t, e, 1)

	assert.Empty(t, e.Holes(),
		"a part containing the holes' blocks at a higher level replaces them, exact prefix or not")
	assert.Equal(t, []string{successor}, e.PartPrefixes(),
		"one successor discharges both holes, and is committed once")
	assert.ElementsMatch(t, []string{"p1", "p2"}, streamBodies(t, e))
	assert.EqualValues(t, 2, e.LostParts())
}

// TestHoleSurvivesReload verifies a hole is durable state, not a per-process one: a fresh engine
// over the prefix reads it back — and does not try to open the objects it does not have.
func TestHoleSurvivesReload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()

	f := answerAlways(bucketindex.WantAbsent, nil)
	e := newRepairEngine(t, be, f)

	lost := loseFirstOfTwo(t, e, be)
	mergeTimes(t, e, 3)
	require.Len(t, e.Holes(), 1)

	r := newRepairEngine(t, be, nil)
	require.NoError(t, r.LoadParts(ctx))

	holes := r.Holes()
	require.Len(t, holes, 1)
	assert.Equal(t, lost, holes[0].Prefix)
	assert.EqualValues(t, 1, r.LostParts())
	assert.Empty(t, r.WantPrefixes(), "a reload must not read the hole back as a fresh obligation")
	assert.NotContains(t, r.PartPrefixes(), lost)

	require.NoError(t, r.Merge(ctx, 0))

	ix := committedIndex(t, be)
	assert.Len(t, ix.Holes(), 1)
	assert.EqualValues(t, 1, ix.LostParts)
}

// TestHoleNotOfferedToAPeer verifies an acknowledged loss cannot spread: asked for a part it holds
// only a hole for, an index answers with nothing.
func TestHoleNotOfferedToAPeer(t *testing.T) {
	t.Parallel()

	be := backend.Memory()

	f := answerAlways(bucketindex.WantAbsent, nil)
	e := newRepairEngine(t, be, f)

	lost := loseFirstOfTwo(t, e, be)
	mergeTimes(t, e, 3)

	ix := committedIndex(t, be)
	w := bucketindex.Want{Prefix: lost, Blocks: bucketindex.Interval{Min: 1, Max: 1}}

	_, ok := ix.Satisfying(w)
	assert.False(t, ok)
}
