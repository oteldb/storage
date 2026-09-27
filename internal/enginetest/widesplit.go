package enginetest

import (
	"cmp"
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// wideSplitDays is how many days the lost straddler spans, and so how many fragments the peer's merge
// splits it into: more than the fetches one repair cycle makes, the want's own included.
const wideSplitDays = 8

// otherRow is the one row of the replica's other part, which the peer's merge splits along with the
// straddler.
var otherRow = Row{Stream: "other", Ts: hour, Val: 1}

// splitLoss is a replica that lost a straddler its peer has since split into a fragment per day.
type splitLoss struct {
	be, peer backend.Backend
	lost     bucketindex.Entry
	// fragments are the peer's split group, in its index order.
	fragments []bucketindex.Entry
	// want is the straddler's rows, one per day.
	want []Row
}

func (k Kind) loseSplitStraddler(t *testing.T, days int64) splitLoss {
	t.Helper()

	ctx := context.Background()
	s := splitLoss{be: backend.Memory(), peer: backend.Memory()}

	e := k.open(t, s.be)

	for d := range days {
		s.want = append(s.want, api(d*day+hour, d))
	}

	e.Append(t, s.want...)
	require.NoError(t, e.Flush(ctx))

	e.Append(t, otherRow)
	require.NoError(t, e.Flush(ctx))

	ix := k.loadIndex(t, s.be)
	require.Len(t, ix.Entries, 2)

	s.lost = ix.Entries[0]
	if s.lost.MaxTime-s.lost.MinTime < day {
		s.lost = ix.Entries[1]
	}

	copyObjects(ctx, t, s.be, s.peer, k.Prefix+"/")

	split := k.open(t, s.peer)
	require.NoError(t, split.LoadParts(ctx))
	require.NoError(t, split.ForceMerge(ctx))

	peerIx := k.loadIndex(t, s.peer)
	for i := range peerIx.Entries {
		if peerIx.Entries[i].Claim.Valid() {
			s.fragments = append(s.fragments, peerIx.Entries[i])
		}
	}

	require.Len(t, s.fragments, int(days), "the peer must hold the lost part as a split group of a fragment per day")

	_, id, _ := strings.Cut(s.lost.Prefix, k.Prefix+"/")
	k.erasePart(ctx, t, s.be, id)

	return s
}

// answer resolves a want against the peer's index, leaving out the entries skip names, and copies
// what it resolves to across.
func (s splitLoss) answer(t *testing.T, k Kind, skip func(w bucketindex.Want, ent bucketindex.Entry) bool) Answer {
	t.Helper()

	return func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		ctx := context.Background()

		ix, err := bucketindex.Load(ctx, s.peer, k.indexKey())
		if err != nil {
			return bucketindex.Entry{}, 0, err
		}

		kept := ix.Entries[:0]
		for i := range ix.Entries {
			if skip == nil || !skip(w, ix.Entries[i]) {
				kept = append(kept, ix.Entries[i])
			}
		}

		ix.Entries = kept

		ent, ok := ix.Satisfying(w)
		if !ok {
			return bucketindex.Entry{}, bucketindex.WantAbsent, nil
		}

		copyObjects(ctx, t, s.peer, s.be, ent.Prefix+"/")

		return ent, bucketindex.WantSatisfied, nil
	}
}

// repairCycles runs n maintenance cycles on r, failing t as soon as one leaves an api row stored
// twice.
func repairCycles(t *testing.T, r Engine, n int) {
	t.Helper()

	for cycle := range n {
		require.NoError(t, r.Merge(context.Background(), 0))

		seen := make(map[Row]struct{})
		for _, row := range rows(t, r, apiStream) {
			_, dup := seen[row]
			require.False(t, dup, "cycle %d stored %+v twice", cycle, row)

			seen[row] = struct{}{}
		}
	}
}

// wideSplitGroupIsRepaired is #717: a replica loses a straddler its peer has since split into a
// fragment per day, more fragments than one repair cycle fetches wants. The peer answers the want
// with one fragment, and the rest of the group must arrive in the same commit, or the fragment sits
// beside the ancestors it partly duplicates.
func wideSplitGroupIsRepaired(t *testing.T, k Kind) {
	t.Helper()

	splitGroupIsRepaired(t, k, wideSplitDays)
}

// splitGroupAtCycleBoundaryIsRepaired repairs the groups either side of the widest one a want and
// the per-cycle fetch cap could bring back in one cycle if members counted against that cap.
func splitGroupAtCycleBoundaryIsRepaired(t *testing.T, k Kind) {
	t.Helper()

	for _, days := range []int64{5, 6} {
		t.Run(strconv.FormatInt(days, 10), func(t *testing.T) {
			splitGroupIsRepaired(t, k, days)
		})
	}
}

func splitGroupIsRepaired(t *testing.T, k Kind, days int64) {
	t.Helper()

	s := k.loseSplitStraddler(t, days)
	r := k.openRepair(t, s.be, NewFetcher(s.answer(t, k, nil)))
	require.NoError(t, r.LoadParts(context.Background()))
	require.Equal(t, []string{s.lost.Prefix}, r.WantPrefixes())

	repairCycles(t, r, 4*int(days))

	st := r.Stats()
	assert.Empty(t, r.WantPrefixes(), "the whole group is on the peer, so the want must be discharged")
	assert.Zero(t, st.WantedParts)
	assert.Zero(t, st.Holes)
	assert.Zero(t, r.LostParts(), "the peer holds the data, so no loss may be acknowledged")
	assert.Equal(t, s.want, rows(t, r, apiStream), "repair must bring every lost row back, once")
	assert.Equal(t, []Row{otherRow}, rows(t, r, otherRow.Stream),
		"the group's claim retires the local ancestor it covers")

	stats := r.RepairStats()
	assert.Equal(t, days, stats.Fetched, "every fragment is published once")
	assert.Zero(t, stats.Failed)
}

// splitGroupMemberLostEverywhereBecomesHole loses one fragment of the peer's split group too: the
// group can be completed nowhere, so repair must acknowledge the loss rather than fetch the rest of
// it forever, and must never commit the fragments it can get.
func splitGroupMemberLostEverywhereBecomesHole(t *testing.T, k Kind) {
	t.Helper()

	for _, tc := range []struct {
		name string
		// named reports whether the want itself is answered from the whole index: a peer that names
		// the group, whose member is gone by the time repair asks for it.
		named bool
	}{
		{"AbsentFromEveryIndex", false},
		{"VanishesBetweenRounds", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := k.loseSplitStraddler(t, wideSplitDays)
			gone := s.fragments[len(s.fragments)-1].Prefix

			f := NewFetcher(s.answer(t, k, func(w bucketindex.Want, ent bucketindex.Entry) bool {
				return ent.Prefix == gone && (w.Prefix == "" || !tc.named)
			}))
			r := k.openRepair(t, s.be, f)
			require.NoError(t, r.LoadParts(context.Background()))

			repairCycles(t, r, 3*wideSplitDays)

			st := r.Stats()
			assert.Empty(t, r.WantPrefixes(), "the loss is acknowledged")
			assert.Zero(t, st.WantedParts)
			assert.Equal(t, 1, st.Holes)
			assert.Equal(t, uint64(1), r.LostParts())
			assert.Empty(t, rows(t, r, apiStream), "no fragment of an incomplete group is ever committed")
			assert.Equal(t, []Row{otherRow}, rows(t, r, otherRow.Stream))
			assert.Zero(t, r.RepairStats().Fetched, "nothing was published")
		})
	}
}

// splitMemberRepairsWithoutItsGroup loses two fragments of a group this node wrote itself, whose
// ancestors it has already retired, and one of them is gone from every owner. The survivor holds
// rows nothing here duplicates, so it must come back on its own; only the other becomes a hole.
func splitMemberRepairsWithoutItsGroup(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	s := splitLoss{be: backend.Memory(), peer: backend.Memory()}

	e := k.open(t, s.be)
	for d := range int64(3) {
		s.want = append(s.want, api(d*day+hour, d))
	}

	e.Append(t, s.want...)
	require.NoError(t, e.Flush(ctx))
	e.Append(t, otherRow)
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.ForceMerge(ctx))

	ix := k.loadIndex(t, s.be)
	fragments := slices.DeleteFunc(ix.Entries, func(e bucketindex.Entry) bool { return !e.Claim.Valid() })
	require.Len(t, fragments, 3, "a fragment per day")
	slices.SortFunc(fragments, func(a, b bucketindex.Entry) int { return cmp.Compare(a.MinTime, b.MinTime) })

	copyObjects(ctx, t, s.be, s.peer, k.Prefix+"/")

	back, gone := fragments[0], fragments[1]
	for _, lost := range []string{back.Prefix, gone.Prefix} {
		_, id, _ := strings.Cut(lost, k.Prefix+"/")
		k.erasePart(ctx, t, s.be, id)
	}

	r := k.openRepair(t, s.be, NewFetcher(s.answer(t, k, func(_ bucketindex.Want, ent bucketindex.Entry) bool {
		return ent.Prefix == gone.Prefix
	})))
	require.NoError(t, r.LoadParts(ctx))
	require.ElementsMatch(t, []string{back.Prefix, gone.Prefix}, r.WantPrefixes())

	repairCycles(t, r, 3*3)

	assert.Empty(t, r.WantPrefixes())
	assert.Equal(t, uint64(1), r.LostParts(), "only the fragment no owner holds is lost")
	assert.Equal(t, []Row{s.want[0], s.want[2]}, rows(t, r, apiStream))
	assert.Equal(t, []Row{otherRow}, rows(t, r, otherRow.Stream))
}
