package enginetest

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/reproduce"
)

// wideSplitDays is how many days the lost straddler spans, and so how many fragments the peer's merge
// splits it into: more than the fetches one repair cycle makes, the want's own included.
const wideSplitDays = 8

// wideSplitGroupIsRepaired is #717: a replica loses a straddler its peer has since split
// into a fragment per day. The peer answers the want with one fragment, and the rest of the group
// must arrive in the same commit — but one repair cycle fetches at most repairFetchesPerCycle parts,
// so a group wider than that is dropped as incomplete on every cycle and the want is never
// discharged. The data is whole on the peer; repair must bring it back.
func wideSplitGroupIsRepaired(t *testing.T, k Kind) {
	t.Helper()

	reproduce.Unfixed(t, 717, "a split group of more than five fragments is dropped as incomplete every cycle, "+
		"so the want stays outstanding while the member answering it is committed alone, again each cycle")

	ctx := context.Background()
	be, peer := backend.Memory(), backend.Memory()

	e := k.open(t, be)

	want := make([]Row, 0, wideSplitDays)
	for d := range int64(wideSplitDays) {
		want = append(want, api(d*day+hour, d))
	}

	e.Append(t, want...)
	require.NoError(t, e.Flush(ctx))

	e.Append(t, Row{Stream: "other", Ts: hour, Val: 1})
	require.NoError(t, e.Flush(ctx))

	ix := k.loadIndex(t, be)
	require.Len(t, ix.Entries, 2)

	lost := ix.Entries[0]
	if lost.MaxTime-lost.MinTime < day {
		lost = ix.Entries[1]
	}

	copyObjects(ctx, t, be, peer, k.Prefix+"/")

	split := k.open(t, peer)
	require.NoError(t, split.LoadParts(ctx))
	require.NoError(t, split.ForceMerge(ctx))

	fragments := 0
	entries := k.loadIndex(t, peer).Entries
	for i := range entries {
		if entries[i].Claim.Valid() {
			fragments++
		}
	}

	require.Greater(t, fragments, 5, "the peer must hold the lost part as a split group wider than a repair cycle")

	_, id, _ := strings.Cut(lost.Prefix, k.Prefix+"/")
	k.erasePart(ctx, t, be, id)

	f := NewFetcher(func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		ix, err := bucketindex.Load(ctx, peer, k.indexKey())
		if err != nil {
			return bucketindex.Entry{}, 0, err
		}

		ent, ok := ix.Satisfying(w)
		if !ok {
			return bucketindex.Entry{}, bucketindex.WantAbsent, nil
		}

		copyObjects(ctx, t, peer, be, ent.Prefix+"/")

		return ent, bucketindex.WantSatisfied, nil
	})

	r := k.openRepair(t, be, f)
	require.NoError(t, r.LoadParts(ctx))
	require.Equal(t, []string{lost.Prefix}, r.WantPrefixes())

	mergeTimes(t, r, 4*wideSplitDays)

	st := r.Stats()
	t.Logf("after %d cycles: %d rows back, wanted %d, holes %d, lost %d, fetched %d", 4*wideSplitDays,
		len(rows(t, r, apiStream)), st.WantedParts, st.Holes, st.LostParts, r.RepairStats().Fetched)

	assert.Empty(t, r.WantPrefixes(), "the whole group is on the peer, so the want must be discharged")
	assert.Zero(t, r.LostParts(), "the peer holds the data, so no loss may be acknowledged")
	assert.Equal(t, want, rows(t, r, apiStream), "repair must bring every lost row back, once")
}
