package partsync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func wantEntry(prefix string, blocks bucketindex.Interval, minT, maxT int64) bucketindex.Entry {
	return bucketindex.Entry{Prefix: prefix, Blocks: blocks, MinTime: minT, MaxTime: maxT}
}

func wantIndex(g bucketindex.Generation, ents ...bucketindex.Entry) *bucketindex.Index {
	ix := &bucketindex.Index{Generation: g}
	for i := range ents {
		ix.Add(ents[i])
	}

	return ix
}

// TestUnaccountedEntriesGating covers what a peer's index having a part this one does not may and may not
// be read as. The bar is higher than for a mirror because the answer becomes a fetch obligation.
func TestUnaccountedEntriesGating(t *testing.T) {
	t.Parallel()

	g := bucketindex.Generation{Term: 1, Counter: 7}
	mine := wantEntry("p/mine", bucketindex.Interval{Min: 5, Max: 5}, 500, 600)
	theirs := wantEntry("p/theirs", bucketindex.Interval{Min: 6, Max: 6}, 700, 800)

	for name, tc := range map[string]struct {
		local *bucketindex.Index
		peer  *bucketindex.Index
		want  []string
	}{
		"unaccounted for is wanted": {
			local: wantIndex(g, mine),
			peer:  wantIndex(g, mine, theirs),
			want:  []string{"p/theirs"},
		},
		"already named wants nothing": {
			local: wantIndex(g, mine, theirs),
			peer:  wantIndex(g, mine, theirs),
		},
		"older than everything kept is not wanted": {
			local: wantIndex(g, mine),
			peer: wantIndex(g, mine,
				wantEntry("p/ancient", bucketindex.Interval{Min: 6, Max: 6}, 1, 2)),
		},
		"a pre-tombstone local index wants nothing": {
			local: wantIndex(bucketindex.Generation{}, mine),
			peer:  wantIndex(g, mine, theirs),
		},
		"a hole is a claim, not a holding": {
			local: wantIndex(g, mine),
			peer: wantIndex(g, mine,
				bucketindex.Entry{Prefix: "p/hole", MinTime: 700, MaxTime: 800, Hole: true}),
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := unaccountedEntries(tc.peer, accountOf(tc.local), retentionHorizon(tc.local))

			prefixes := make([]string, 0, len(got))
			for i := range got {
				prefixes = append(prefixes, got[i].Prefix)
			}

			assert.ElementsMatch(t, tc.want, prefixes)
		})
	}
}

// TestWantsNeedConfirmation pins the quarantine: one pass is not evidence, since a rebalance
// in flight briefly leaves two indexes disagreeing, and a part that becomes accounted for resets.
func TestWantsNeedConfirmation(t *testing.T) {
	t.Parallel()

	g := bucketindex.Generation{Term: 1, Counter: 7}
	mine := wantEntry("p/mine", bucketindex.Interval{Min: 5, Max: 5}, 500, 600)
	theirs := wantEntry("p/theirs", bucketindex.Interval{Min: 6, Max: 6}, 700, 800)

	s := New(nil, nil)
	local, peer := wantIndex(g, mine), wantIndex(g, mine, theirs)

	for range wantAfterPasses - 1 {
		assert.Empty(t, s.confirmedWants("p", peer, local), "one sighting is not an obligation")
	}

	wants := s.confirmedWants("p", peer, local)
	require.Len(t, wants, 1)
	assert.Equal(t, "p/theirs", wants[0].Prefix)
	assert.Equal(t, g, wants[0].Generation)

	// The peer explains it: the count resets, and the next disagreement starts over.
	assert.Empty(t, s.confirmedWants("p", wantIndex(g, mine), local))
	assert.Empty(t, s.confirmedWants("p", peer, local), "a reset means confirming again from scratch")
}
