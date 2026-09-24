package enginetest

import (
	"context"
	"fmt"
	"path"
	"testing"
	"testing/synctest"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// wantOverlapIsBoundedByTheLostPart: a want is what the read policy keys on, and it is bounded by
// the lost part's own time range. A query that does not reach into it is answerable here in full,
// so disclaiming the whole shard would cost availability the loss does not justify.
func wantOverlapIsBoundedByTheLostPart(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()
	e := k.openRepair(t, be, AnswerAlways(bucketindex.WantIncomplete, nil))

	assert.False(t, e.HasWants())
	assert.False(t, e.WantOverlaps(0, 1_000), "a healthy engine disclaims nothing")

	loseFirstOfTwo(t, e, be) // the part holding the row at 100

	require.True(t, e.HasWants())
	assert.True(t, e.WantOverlaps(0, 150), "a read reaching the lost part is short")
	assert.True(t, e.WantOverlaps(100, 100), "and so is one landing exactly on it")
	assert.False(t, e.WantOverlaps(200, 400), "a read past it is served here")
}

// committedHoleLetsReadsThrough bounds the policy: a want no owner can satisfy becomes a hole, the
// hole discharges the want, and the engine stops disclaiming. Without this, acknowledging a loss
// would leave the shard permanently unreadable for that range.
func committedHoleLetsReadsThrough(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()
	e := k.openRepair(t, be, AnswerAlways(bucketindex.WantAbsent, nil))

	loseFirstOfTwo(t, e, be)
	require.True(t, e.WantOverlaps(0, 150))

	mergeTimes(t, e, 3) // holeConfirmations passes of definitive absence

	require.Len(t, e.Holes(), 1)
	assert.False(t, e.HasWants(), "the hole discharged the obligation")
	assert.False(t, e.WantOverlaps(0, 150), "so the read policy lets the window through again")
}

// transientErrorBreaksAbsenceRun: absences separated by a failed attempt are not consecutive, so
// they do not add up to a hole. The error carries an absent outcome, which it must override.
func transientErrorBreaksAbsenceRun(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()

	var fail bool

	e := k.openRepair(t, be, NewFetcher(func(bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		if fail {
			return bucketindex.Entry{}, bucketindex.WantAbsent, errors.New("peer unreachable")
		}

		return bucketindex.Entry{}, bucketindex.WantAbsent, nil
	}))

	lost := loseFirstOfTwo(t, e, be)

	mergeTimes(t, e, 1)

	fail = true
	mergeTimes(t, e, 1)
	fail = false

	mergeTimes(t, e, 2)

	assert.Equal(t, []string{lost}, e.WantPrefixes(), "absent, error, absent, absent is not three in a row")
	assert.Empty(t, e.Holes())
	assert.Zero(t, e.LostParts())

	mergeTimes(t, e, 1)

	holes := e.Holes()
	require.Len(t, holes, 1, "three uninterrupted absences after the error do")
	assert.Equal(t, lost, holes[0].Prefix)
}

// unattemptedWantKeepsAbsenceEvidence: only a want the cycle asked for can have its run broken. One
// pushed past the per-cycle fetch cap by wants that fail keeps the absences it had earned.
func unattemptedWantKeepsAbsenceEvidence(t *testing.T, k Kind) {
	t.Helper()

	be := backend.Memory()

	blockers := make(map[string]struct{})
	f := NewFetcher(func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		if _, ok := blockers[w.Prefix]; ok {
			return bucketindex.Entry{}, bucketindex.WantIncomplete, errors.New("peer unreachable")
		}

		return bucketindex.Entry{}, bucketindex.WantAbsent, nil
	})
	e := k.openRepair(t, be, f)

	lost := loseFirstOfTwo(t, e, be)

	mergeTimes(t, e, 2)
	require.Empty(t, e.Holes())

	// Wants sorting ahead of it, on blocks nothing covers, fill the cycle's fetch budget and fail.
	for i := range 4 {
		b := fmt.Sprintf("%s/%026d", path.Dir(lost), i)
		require.Less(t, b, lost)

		blockers[b] = struct{}{}
		e.LosePart(b, bucketindex.Interval{Min: uint64(100 + i), Max: uint64(100 + i)})
	}

	before := len(f.Asks())
	mergeTimes(t, e, 1)

	capped := f.Asks()[before:]
	require.Len(t, capped, 4)
	require.NotContains(t, capped, lost, "the want is past the per-cycle cap")

	// A local part now covers the blockers, so they discharge without a fetch and the want is asked.
	e.SetPartBlocks(e.PartPrefixes()[0], bucketindex.Interval{Min: 100, Max: 103}, 1)
	mergeTimes(t, e, 1)

	holes := e.Holes()
	require.Len(t, holes, 1, "two absences before the capped cycle and one after make three")
	assert.Equal(t, lost, holes[0].Prefix)
	assert.Empty(t, e.WantPrefixes())
}

// repairConcurrentMergesFetchOnce pins the single-flight: two maintenance passes overlapping on one
// engine (an operator's MaintainNow racing the background loop) must not each copy the same part
// from a peer, nor each count the copy. The gated fetcher holds the first pass inside the peer I/O
// until the second has run as far as it can, which is where the duplicate would be issued.
func repairConcurrentMergesFetchOnce(t *testing.T, k Kind) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		be, peer := backend.Memory(), backend.Memory()

		f := NewFetcher(nil)
		e := k.openRepair(t, be, f)

		lost := flushTwo(t, e)[0]

		CopyObjects(t, be, peer, lost)
		dropObjects(t, be, lost)
		e.LosePart(lost, bucketindex.Interval{Min: 1, Max: 1})

		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		satisfy := satisfyFrom(t, peer, be)

		f.SetAnswer(func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
			entered <- struct{}{}
			<-release

			return satisfy(w)
		})

		errs := make(chan error, 2)
		go func() { errs <- e.Merge(ctx, 0) }()

		<-entered // the first pass is inside the peer fetch

		go func() { errs <- e.Merge(ctx, 0) }()

		synctest.Wait() // the second pass has run as far as it can

		close(release)

		require.NoError(t, <-errs)
		require.NoError(t, <-errs)

		assert.Equal(t, []string{lost}, f.Asks(), "the wanted part is copied from a peer once")
		assert.Equal(t, 1, f.Calls())
		assert.Equal(t, bucketindex.RepairStats{Fetched: 1}, e.RepairStats(), "one published part, one counted fetch and nothing else")
		assert.Empty(t, e.WantPrefixes())
		assert.Empty(t, k.loadIndex(t, be).Wanted)
	})
}

// lostWindow is the time range the i-th seeded part covers; the ranges are disjoint so a dropped
// want leaves a window no other want disclaims.
func lostWindow(i int) (int64, int64) {
	start := int64(i+1) * 1_000

	return start, start + 999
}

// seedGoneParts commits an index naming n parts that have no objects, the state a node is in after
// losing n parts from its disk, and returns their prefixes.
func (k Kind) seedGoneParts(t *testing.T, be backend.Backend, n int) []string {
	t.Helper()

	ix := bucketindex.Index{Generation: bucketindex.Generation{Term: 1, Counter: 1}}
	lost := make([]string, 0, n)

	for i := range n {
		prefix := fmt.Sprintf("%s/%016d", k.Prefix, i+1)
		start, end := lostWindow(i)
		ix.Add(bucketindex.Entry{
			Prefix: prefix, MinTime: start, MaxTime: end,
			Blocks: bucketindex.Interval{Min: uint64(i + 1), Max: uint64(i + 1)},
		})
		lost = append(lost, prefix)
	}

	_, err := ix.Save(context.Background(), be, k.indexKey(), backend.VersionAbsent)
	require.NoError(t, err)

	return lost
}

// owedParts returns every part the committed index still accounts for as lost: outstanding wants
// and acknowledged holes. A part in neither has left Entries into nothing, which is silent loss.
func owedParts(ix *bucketindex.Index) map[string]struct{} {
	owed := make(map[string]struct{}, len(ix.Wanted))
	for i := range ix.Wanted {
		owed[ix.Wanted[i].Prefix] = struct{}{}
	}

	holes := ix.Holes()
	for i := range holes {
		owed[holes[i].Prefix] = struct{}{}
	}

	return owed
}

// wantsPastBoundStayOwed pins that a part leaves Entries only into Removed or Wanted past the point
// where the index carries more wants than MaxWants: no lost part may vanish without a want, a hole
// or a raised LostParts, and every lost window stays disclaimed.
func wantsPastBoundStayOwed(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := backend.Memory()
	lost := k.seedGoneParts(t, be, bucketindex.MaxWants+1)

	e := k.open(t, be)
	require.NoError(t, e.LoadParts(ctx))

	check := func(when string) {
		t.Helper()

		ix := k.loadIndex(t, be)
		require.Empty(t, ix.Removed, "%s: a loss is not a removal", when)
		assert.Equal(t, uint64(len(ix.Holes())), ix.LostParts, "%s: every hole is a counted loss", when)

		owed := owedParts(ix)

		var forgotten []string

		for i, prefix := range lost {
			if _, ok := owed[prefix]; !ok {
				forgotten = append(forgotten, prefix)
			}

			start, end := lostWindow(i)
			assert.Truef(t, e.WantOverlaps(start, end), "%s: window of %s is served short", when, prefix)
		}

		assert.Emptyf(t, forgotten, "%s: %d of %d lost parts left the index into neither Wanted nor a hole",
			when, len(forgotten), len(lost))
	}

	check("after load")

	e.Append(t, api(1, 1))
	require.NoError(t, e.Flush(ctx))
	check("after flush")
}
