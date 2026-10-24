package repair

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

// catalogOnlyOuter is {1,2} merged at level 1, split at level 2 into {10,11}, and each member split
// again at level 3, so the outer claim is on no entry, only in the catalog.
func catalogOnlyOuter() (outer bucketindex.Claim, fragments []bucketindex.Entry, catalog []bucketindex.Claim) {
	outer = bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2), Group: bucketindex.Blocks(10, 11)}
	inner10 := group("g", 20, 2, bucketindex.Blocks(10))
	inner11 := group("h", 22, 2, bucketindex.Blocks(11))
	fragments = slices.Concat(inner10, inner11)

	for i := range fragments {
		fragments[i].Level = 3
	}

	return outer, fragments, bucketindex.MergeLineage([]bucketindex.Claim{outer, inner10[0].Claim, inner11[0].Claim})
}

// TestAdmitRetiresAncestorOfCatalogOnlyGroup is a node still holding a divergent copy of block 1
// while repair fetches the four inner fragments for the lost {1,2}. Together they hold every row of
// {1,2}, so the copy is retired and the fragments commit, instead of being refused for overlapping
// it forever.
func TestAdmitRetiresAncestorOfCatalogOnlyGroup(t *testing.T) {
	t.Parallel()

	_, fragments, catalog := catalogOnlyOuter()
	live := []bucketindex.Entry{{Prefix: "anc", Blocks: bucketindex.Blocks(1)}}
	lost := bucketindex.Want{Prefix: "lost", Blocks: bucketindex.Blocks(1, 2), Level: 1, MinTime: 1, MaxTime: 2}

	u := make(Unit, 0, len(fragments))
	u = append(u, Result{Target: Target{Want: lost}, Entry: fragments[0]})

	for _, f := range fragments[1:] {
		u = append(u, Result{Target: Target{Want: bucketindex.Want{Blocks: f.Blocks}, Member: true}, Entry: f})
	}

	var stats bucketindex.RepairStats

	got, retired := Admit(context.Background(), live, catalog, []Unit{u}, func(*Result) error { return nil }, &stats)

	assert.Equal(t, fragments, got, "the fragments commit")
	assert.Equal(t, map[string]struct{}{"anc": {}}, retired, "the complete outer group retires the copy")
	assert.Zero(t, stats.Failed)
}

// TestRunMemberBesideDivergentAncestor is a lost outer member, block 10, owed by a node that still
// holds a divergent copy of block 1. Its inner fragments alone would duplicate rows of that copy
// without retiring it, and commit would refuse them forever; so repair completes the whole outer
// group, through member 11's own split, and the commit retires the copy.
func TestRunMemberBesideDivergentAncestor(t *testing.T) {
	t.Parallel()

	outer, fragments, catalog := catalogOnlyOuter()
	live := []bucketindex.Entry{{Prefix: "anc", Blocks: bucketindex.Blocks(1)}}
	p := &peer{ix: bucketindex.Index{Entries: fragments, Catalog: catalog}}
	lost := bucketindex.Want{
		Prefix: "m10", Blocks: bucketindex.Blocks(10), Claim: outer, Level: 2, MinTime: 1, MaxTime: 2,
	}

	plan := Pass{Fetcher: p, Catalog: catalog}.Run(context.Background(), live, []bucketindex.Want{lost}, nil)

	require.Len(t, plan.Units, 1)
	assert.ElementsMatch(t, []string{"g00", "g01", "h00", "h01"}, prefixes(plan.Units[0]))

	var stats bucketindex.RepairStats

	got, retired := Admit(context.Background(), live, catalog, plan.Units, func(*Result) error { return nil }, &stats)

	assert.ElementsMatch(t, fragments, got)
	assert.Equal(t, map[string]struct{}{"anc": {}}, retired)
	assert.Zero(t, stats.Failed)
}
