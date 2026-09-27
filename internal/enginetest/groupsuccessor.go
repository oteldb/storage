package enginetest

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// splitGroupBesideSuccessorIsRepaired is #721: one repair brings back a complete split group and a
// part holding the group's whole claimed ancestry at a higher level — a peer that merged the same
// inputs down another lineage. The successor holds every member's rows, so it must be published
// alone, or every row of the group is live twice and the next merge persists the duplicates.
func splitGroupBesideSuccessorIsRepaired(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	s := k.loseSplitStraddler(t, 3)

	entries := k.loadIndex(t, s.be).Entries
	require.Len(t, entries, 2)

	other := entries[0]
	if other.Prefix == s.lost.Prefix {
		other = entries[1]
	}

	_, id, _ := strings.Cut(other.Prefix, k.Prefix+"/")
	k.erasePart(ctx, t, s.be, id)

	succBE := backend.Memory()
	w := k.open(t, succBE)
	w.Append(t, append(slices.Clone(s.want), otherRow)...)
	require.NoError(t, w.Flush(ctx))

	succIx := k.loadIndex(t, succBE)
	require.Len(t, succIx.Entries, 1)

	succ := succIx.Entries[0]
	succ.Blocks = s.lost.Blocks.Union(other.Blocks)
	succ.Level = 2

	for i := range s.fragments {
		require.True(t, succ.Blocks.Contains(s.fragments[i].Claim.Blocks), "the successor holds the group's whole ancestry")
		require.Greater(t, succ.Level, s.fragments[i].Level)
	}

	fromPeer := s.answer(t, k, nil)
	r := k.openRepair(t, s.be, NewFetcher(func(w bucketindex.Want) (bucketindex.Entry, bucketindex.WantOutcome, error) {
		if w.Prefix != other.Prefix {
			return fromPeer(w)
		}

		copyObjects(ctx, t, succBE, s.be, succ.Prefix+"/")

		return succ, bucketindex.WantSatisfied, nil
	}))
	require.NoError(t, r.LoadParts(ctx))
	require.ElementsMatch(t, []string{s.lost.Prefix, other.Prefix}, r.WantPrefixes())

	repairCycles(t, r, 3)

	assert.Empty(t, r.WantPrefixes(), "the successor discharges both wants")
	assert.Zero(t, r.LostParts())
	assert.Equal(t, int64(1), r.RepairStats().Fetched, "only the successor is published")

	live := prefixes(k.loadIndex(t, s.be).Entries)
	for i := range s.fragments {
		assert.NotContains(t, live, s.fragments[i].Prefix, "no member is live beside the successor")
	}

	assert.Equal(t, s.want, rows(t, r, apiStream), "every lost row is back once")
	assert.Equal(t, []Row{otherRow}, rows(t, r, otherRow.Stream))
}
