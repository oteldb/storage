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
	"github.com/oteldb/storage/internal/reproduce"
)

// splitGroupBesideSuccessorIsRepaired is #721: one repair brings back a complete split group and a
// part holding the group's whole claimed ancestry at a higher level — a peer that merged the same
// inputs down another lineage. The successor holds every member's rows, so it must be published
// alone, or every row of the group is live twice and the next merge persists the duplicates.
func splitGroupBesideSuccessorIsRepaired(t *testing.T, k Kind) {
	t.Helper()
	reproduce.Unfixed(t, 721, "the group and the successor covering its claim are both published")

	ctx := context.Background()
	s := k.loseSplitStraddler(t, 3)

	var other bucketindex.Entry
	for _, ent := range k.loadIndex(t, s.be).Entries {
		if ent.Prefix != s.lost.Prefix {
			other = ent
		}
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

	for _, f := range s.fragments {
		require.True(t, succ.Blocks.Contains(f.Claim.Blocks), "the successor holds the group's whole ancestry")
		require.Greater(t, succ.Level, f.Level)
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

	for _, f := range s.fragments {
		assert.NotContains(t, prefixes(k.loadIndex(t, s.be).Entries), f.Prefix, "no member is live beside the successor")
	}

	assert.Equal(t, s.want, rows(t, r, apiStream), "every lost row is back once")
	assert.Equal(t, []Row{otherRow}, rows(t, r, otherRow.Stream))
}
