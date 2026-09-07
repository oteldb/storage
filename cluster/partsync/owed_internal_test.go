package partsync

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

func owedEntry(prefix string, blocks bucketindex.Interval, minT, maxT int64) bucketindex.Entry {
	return bucketindex.Entry{Prefix: prefix, Blocks: blocks, MinTime: minT, MaxTime: maxT}
}

func owedIndex(g bucketindex.Generation, ents ...bucketindex.Entry) *bucketindex.Index {
	ix := &bucketindex.Index{Generation: g}
	for i := range ents {
		ix.Add(ents[i])
	}

	return ix
}

// TestOwedEntriesGating covers what a peer's index having a part this one does not may and may not
// be read as. The bar is higher than for a mirror because the answer becomes a fetch obligation.
func TestOwedEntriesGating(t *testing.T) {
	t.Parallel()

	g := bucketindex.Generation{Term: 1, Counter: 7}
	mine := owedEntry("p/mine", bucketindex.Interval{Min: 5, Max: 5}, 500, 600)
	theirs := owedEntry("p/theirs", bucketindex.Interval{Min: 6, Max: 6}, 700, 800)

	for name, tc := range map[string]struct {
		local *bucketindex.Index
		peer  *bucketindex.Index
		want  []string
	}{
		"unaccounted for is owed": {
			local: owedIndex(g, mine),
			peer:  owedIndex(g, mine, theirs),
			want:  []string{"p/theirs"},
		},
		"already named owes nothing": {
			local: owedIndex(g, mine, theirs),
			peer:  owedIndex(g, mine, theirs),
		},
		"older than everything kept is not owed": {
			local: owedIndex(g, mine),
			peer: owedIndex(g, mine,
				owedEntry("p/ancient", bucketindex.Interval{Min: 6, Max: 6}, 1, 2)),
		},
		"a pre-tombstone local index owes nothing": {
			local: owedIndex(bucketindex.Generation{}, mine),
			peer:  owedIndex(g, mine, theirs),
		},
		"a hole is a claim, not a holding": {
			local: owedIndex(g, mine),
			peer: owedIndex(g, mine,
				bucketindex.Entry{Prefix: "p/hole", MinTime: 700, MaxTime: 800, Hole: true}),
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := owedEntries(tc.peer, accountOf(tc.local), retentionHorizon(tc.local))

			prefixes := make([]string, 0, len(got))
			for i := range got {
				prefixes = append(prefixes, got[i].Prefix)
			}

			assert.ElementsMatch(t, tc.want, prefixes)
		})
	}
}

// TestOwedWantsNeedsConfirmation pins the quarantine: one pass is not evidence, since a rebalance
// in flight briefly leaves two indexes disagreeing, and a part that becomes accounted for resets.
func TestOwedWantsNeedsConfirmation(t *testing.T) {
	t.Parallel()

	g := bucketindex.Generation{Term: 1, Counter: 7}
	mine := owedEntry("p/mine", bucketindex.Interval{Min: 5, Max: 5}, 500, 600)
	theirs := owedEntry("p/theirs", bucketindex.Interval{Min: 6, Max: 6}, 700, 800)

	s := New(nil, nil)
	local, peer := owedIndex(g, mine), owedIndex(g, mine, theirs)

	for range owedAfterPasses - 1 {
		assert.Empty(t, s.owedWants("p", peer, local), "one sighting is not an obligation")
	}

	owed := s.owedWants("p", peer, local)
	require.Len(t, owed, 1)
	assert.Equal(t, "p/theirs", owed[0].Prefix)
	assert.Equal(t, g, owed[0].Generation)

	// The peer explains it: the count resets, and the next disagreement starts over.
	assert.Empty(t, s.owedWants("p", owedIndex(g, mine), local))
	assert.Empty(t, s.owedWants("p", peer, local), "a reset means confirming again from scratch")
}
