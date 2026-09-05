package engine_test

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
)

// answerAlways builds a fetcher that returns the same outcome to every ask.
func answerAlways(outcome bucketindex.WantOutcome, err error) *fakeFetcher {
	return &fakeFetcher{answer: func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		return bucketindex.Entry{}, outcome, err
	}}
}

// loseFirstOfTwo flushes two parts, destroys the first and records the want its absence owes,
// returning the lost prefix.
func loseFirstOfTwo(t *testing.T, e *engine.Engine, be backend.Backend) string {
	t.Helper()

	parts := flushSamples(t, e, 2)
	lost := parts[0]

	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	// A flush commits the loss, so the want is durable before any repair pass runs.
	mustAppend(t, e, mkSeries("job", "api"), 300, 3)
	require.NoError(t, e.Flush(context.Background()))

	return lost
}

// mergeTimes runs n maintenance merges, which is how many repair passes the engine gets.
func mergeTimes(t *testing.T, e *engine.Engine, n int) {
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
	ix := metricsIndex(t, be)
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

	ix := metricsIndex(t, be)
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

	be, peer := backend.Memory(), backend.Memory()

	f := answerAlways(bucketindex.WantAbsent, nil)
	e := newRepairEngine(t, be, f)

	parts := flushSamples(t, e, 2)
	lost := parts[0]

	copyObjects(t, be, peer, lost)
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	mergeTimes(t, e, 3)
	require.Len(t, e.Holes(), 1)

	// The peer that was restarting comes back holding the part after all.
	f.answer = func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peer, be, w.Prefix)

		return bucketindex.Entry{
			Prefix: w.Prefix, MinTime: 100, MaxTime: 100, Blocks: w.Blocks,
		}, bucketindex.WantSatisfied, nil
	}

	mergeTimes(t, e, 1)

	assert.Empty(t, e.Holes(), "the real part replaces the hole")
	assert.Equal(t, int64(1), e.RepairStats().Revoked)
	assert.EqualValues(t, 1, e.LostParts(), "the loss counter is monotone: a revoked hole is still a fact")

	ix := metricsIndex(t, be)
	assert.Empty(t, ix.Holes())
	assert.EqualValues(t, 1, ix.LostParts)

	ts, vals := seriesSamples(t, e)
	assert.Equal(t, []int64{100, 200}, ts, "the rows the hole stood for are readable again")
	assert.Equal(t, []float64{1, 2}, vals)
}

// TestHoleRevokedByContainingSuccessor covers the other way the data comes back: by the time an
// owner finds it, it exists only inside a merged part covering the hole's blocks.
func TestHoleRevokedByContainingSuccessor(t *testing.T) {
	t.Parallel()

	be, peer := backend.Memory(), backend.Memory()

	f := answerAlways(bucketindex.WantAbsent, nil)
	e := newRepairEngine(t, be, f)

	parts := flushSamples(t, e, 2)
	lost, successor := parts[0], parts[1]

	e.SetPartBlocks(successor, bucketindex.Interval{Min: 1, Max: 4}, 2)
	copyObjects(t, be, peer, successor)
	dropObjects(t, be, lost)
	e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

	// The successor is local, so it would discharge the want before a hole could form: take it out
	// of the way and let the loss be acknowledged first.
	e.LosePart(successor, bucketindex.Interval{Min: 1, Max: 4})
	dropObjects(t, be, successor)

	mergeTimes(t, e, 3)
	require.Len(t, e.Holes(), 2)

	f.answer = func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		copyObjects(t, peer, be, successor)

		return bucketindex.Entry{
			Prefix: successor, MinTime: 200, MaxTime: 200,
			Blocks: bucketindex.Interval{Min: 1, Max: 4}, Level: 2,
		}, bucketindex.WantSatisfied, nil
	}

	mergeTimes(t, e, 1)

	assert.Empty(t, e.Holes(),
		"a part containing the hole's blocks at a higher level replaces it, exact prefix or not")
	assert.Equal(t, []string{successor}, e.PartPrefixes(),
		"one successor discharges both holes, and is committed once")
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

	// A commit by the reloaded engine carries the hole and the count through unchanged.
	require.NoError(t, r.Merge(ctx, 0))

	ix := metricsIndex(t, be)
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

	ix := metricsIndex(t, be)
	w := bucketindex.Want{Prefix: lost, Blocks: bucketindex.Interval{Min: 1, Max: 1}}

	_, ok := ix.Satisfying(w)
	assert.False(t, ok)
}

// TestRepairStatsSurfaceLoss verifies the operator surface distinguishes the states a hole exists
// to make visible: what is still owed, what stands acknowledged, and what was ever lost.
func TestRepairStatsSurfaceLoss(t *testing.T) {
	t.Parallel()

	be := backend.Memory()

	f := answerAlways(bucketindex.WantIncomplete, nil)
	e := newRepairEngine(t, be, f)

	loseFirstOfTwo(t, e, be)
	mergeTimes(t, e, 1)

	st := e.Stats()
	assert.Equal(t, 1, st.WantedParts)
	assert.Zero(t, st.Holes)
	assert.Zero(t, st.LostParts)
}
