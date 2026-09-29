package bucketindex_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

// nested is the #725 shape: {1,2} split into the outer group {10..12}, member 12 split again with
// block 7 into the inner group {20..22}.
var (
	outerClaim = bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2), Group: bucketindex.Blocks(10, 11, 12)}
	innerClaim = bucketindex.Claim{Blocks: bucketindex.Blocks(7, 12), Group: bucketindex.Blocks(20, 21, 22)}
	innerWant  = bucketindex.Want{Prefix: "inner-20", Blocks: bucketindex.Blocks(20), Claim: innerClaim, Level: 2}
	successor  = bucketindex.Entry{Prefix: "succ", Blocks: bucketindex.Blocks(1, 2, 7), Level: 3}
)

func TestRecordLineageKeepsRetiredGroups(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{}
	ix.Add(bucketindex.Entry{Prefix: "m10", Blocks: bucketindex.Blocks(10), Claim: outerClaim, Level: 1})
	ix.RecordLineage()
	require.True(t, ix.Remove("m10"))

	ix.RecordWant(innerWant)
	ix.RecordLineage()

	assert.Equal(t, []bucketindex.Claim{outerClaim, innerClaim}, ix.Catalog, "sorted by group, the retired one kept")

	ix.RecordLineage()
	assert.Len(t, ix.Catalog, 2, "recording again adds nothing")
}

// newerGroups is n unrelated split groups above every block the nested shape uses.
func newerGroups(n int) []bucketindex.Claim {
	out := make([]bucketindex.Claim, 0, n)
	for i := range uint64(n) {
		out = append(out, bucketindex.Claim{
			Blocks: bucketindex.Blocks(1000 + 2*i), Group: bucketindex.Blocks(1001 + 2*i),
		})
	}

	return out
}

func TestTrimCatalogAgesOutUnreachableGroups(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{Catalog: bucketindex.MergeLineage(newerGroups(bucketindex.MaxLineage + 10))}

	assert.Zero(t, ix.TrimCatalog())
	require.Len(t, ix.Catalog, bucketindex.MaxLineage)
	assert.Equal(t, bucketindex.Blocks(1021), ix.Catalog[0].Group, "the oldest groups age out first")
	assert.Nil(t, bucketindex.MergeLineage(nil, []bucketindex.Claim{{}}), "an unset claim is not a group")
}

// TestTrimCatalogKeepsReachedAncestry is the saturation case: an old nested want whose outer claim
// lives only in the catalog, and more than MaxLineage newer groups after it. The outer claim is the
// oldest, and still kept, since the want reaches it; newer groups nothing reaches go instead.
func TestTrimCatalogKeepsReachedAncestry(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{Catalog: bucketindex.MergeLineage([]bucketindex.Claim{outerClaim})}
	ix.RecordWant(innerWant)
	ix.Catalog = bucketindex.MergeLineage(ix.Catalog, newerGroups(bucketindex.MaxLineage+10))

	assert.Zero(t, ix.RecordLineage())
	require.Len(t, ix.Catalog, bucketindex.MaxLineage)
	assert.True(t, ix.Catalog[0].Equal(outerClaim), "the want still reaches the oldest group")
	assert.True(t, ix.Catalog[1].Equal(innerClaim), "and its own")

	peer := &bucketindex.Index{Entries: []bucketindex.Entry{successor}}
	_, ok := peer.SatisfyingWith(innerWant, ix.Lineage())
	assert.True(t, ok, "so the successor still answers it")

	ix.SatisfyWant(innerWant.Prefix)
	ix.Catalog = bucketindex.MergeLineage(ix.Catalog, newerGroups(bucketindex.MaxLineage+20))
	assert.Zero(t, ix.RecordLineage())
	assert.False(t, ix.Catalog[0].Equal(outerClaim), "once nothing reaches it the old group ages out")
}

// TestTrimCatalogOutgrowsTheTargetOnlyByReachedClaims: when every claim is reached, none is aged out,
// and the catalog reports how far it stands above the target.
func TestTrimCatalogOutgrowsTheTargetOnlyByReachedClaims(t *testing.T) {
	t.Parallel()

	ix := &bucketindex.Index{}
	for i, c := range newerGroups(bucketindex.MaxLineage + 3) {
		ix.Add(bucketindex.Entry{Prefix: fmt.Sprintf("m%05d", i), Blocks: c.Group, Claim: c, Level: 1})
	}

	assert.Equal(t, 3, ix.RecordLineage())
	assert.Len(t, ix.Catalog, bucketindex.MaxLineage+3)
}

// TestJointCoverageThroughRetiredOuterClaim: every member of the outer group {10,11} over {1} was
// split again, into {20,21} and {30,31}. Only the complete inner groups remain, and only the catalog
// still says {10,11} holds {1}. A want for {1} is answered jointly by an inner member, and a peer short
// of one inner member is asked for exactly that one.
func TestJointCoverageThroughRetiredOuterClaim(t *testing.T) {
	t.Parallel()

	outer := bucketindex.Claim{Blocks: bucketindex.Blocks(1), Group: bucketindex.Blocks(10, 11)}
	innerA := bucketindex.Claim{Blocks: bucketindex.Blocks(10), Group: bucketindex.Blocks(20, 21)}
	innerB := bucketindex.Claim{Blocks: bucketindex.Blocks(11), Group: bucketindex.Blocks(30, 31)}
	want := bucketindex.Want{Prefix: "orig", Blocks: bucketindex.Blocks(1)}

	member := func(block uint64, c bucketindex.Claim) bucketindex.Entry {
		return bucketindex.Entry{
			Prefix: "m" + string(rune('a'+block%26)), Blocks: bucketindex.Blocks(block), Claim: c, Level: 2,
		}
	}

	peer := &bucketindex.Index{Entries: []bucketindex.Entry{
		member(20, innerA), member(21, innerA), member(30, innerB), member(31, innerB),
	}}

	_, ok := peer.Satisfying(want)
	require.False(t, ok, "without the outer claim nothing relates the inner groups to {1}")

	got, ok := peer.SatisfyingWith(want, bucketindex.Lineage{outer})
	require.True(t, ok, "the inner groups jointly cover {10,11}, which the outer claim says holds {1}")
	assert.True(t, got.Claim.Equal(innerA) || got.Claim.Equal(innerB), "answered by an inner member")

	short := &bucketindex.Index{Entries: peer.Entries[:3], Catalog: []bucketindex.Claim{outer}}
	assert.Equal(t, []bucketindex.Block{{N: 31}}, short.Missing(want), "only the absent inner member is asked for")

	local := &bucketindex.Index{Catalog: []bucketindex.Claim{outer}}
	assert.Equal(t, []bucketindex.Block{{N: 10}, {N: 11}}, local.Missing(want),
		"knowing only the outer split, repair asks for its members, and a peer answers each by its own lineage")
}

// TestSatisfyingThroughTheCatalog is the nested false hole at the index level: the want carries only
// the inner claim, and a successor of the outer ancestry answers it only through the catalog of the
// index that owes it.
func TestSatisfyingThroughTheCatalog(t *testing.T) {
	t.Parallel()

	peer := &bucketindex.Index{Entries: []bucketindex.Entry{successor}}

	_, ok := peer.Satisfying(innerWant)
	require.False(t, ok, "nothing the peer or the want carries relates 20 to {1,2}")

	got, ok := peer.SatisfyingWith(innerWant, bucketindex.Lineage{outerClaim})
	require.True(t, ok)
	assert.Equal(t, "succ", got.Prefix)

	own := &bucketindex.Index{Entries: []bucketindex.Entry{successor}, Catalog: []bucketindex.Claim{outerClaim}}
	_, ok = own.Satisfying(innerWant)
	assert.True(t, ok, "an index's own catalog relates them too")

	assert.Empty(t, bucketindex.TrimWants([]bucketindex.Want{innerWant}, own.Entries, outerClaim))
	assert.Len(t, bucketindex.TrimWants([]bucketindex.Want{innerWant}, own.Entries), 1)

	hole := innerWant.Entry()
	hole.Hole = true
	assert.Empty(t, bucketindex.TrimHoles([]bucketindex.Entry{hole}, own.Entries, outerClaim),
		"the successor revokes the hole through the catalog")
	assert.Len(t, bucketindex.TrimHoles([]bucketindex.Entry{hole}, own.Entries), 1)

	low := successor
	low.Level = 1
	_, ok = (&bucketindex.Index{Entries: []bucketindex.Entry{low}}).SatisfyingWith(innerWant, bucketindex.Lineage{outerClaim})
	assert.False(t, ok, "a successor must still sit above the member it answers for")
}

func TestLineageWithDoesNotAlias(t *testing.T) {
	t.Parallel()

	base := make(bucketindex.Lineage, 1, 4)
	base[0] = outerClaim

	a := base.With(innerClaim)
	b := base.With(bucketindex.Claim{Blocks: bucketindex.Blocks(3), Group: bucketindex.Blocks(30)})

	assert.Len(t, base, 1)
	assert.True(t, a[1].Equal(innerClaim), "a second extension does not overwrite the first")
	assert.Len(t, b, 2)
	assert.Equal(t, base, base.With(outerClaim, bucketindex.Claim{}), "a known or unset claim adds nothing")
}

// TestSubsumedByCatalogOnlyGroup is {1,2} split into {10,11}, each member split again, so the outer
// claim is on no entry. The four inner fragments hold every row of {1,2}: a lower copy of block 1
// beside them is subsumed, and neither the fragments nor a successor above them are.
func TestSubsumedByCatalogOnlyGroup(t *testing.T) {
	t.Parallel()

	outer := bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2), Group: bucketindex.Blocks(10, 11)}
	innerA := bucketindex.Claim{Blocks: bucketindex.Blocks(10), Group: bucketindex.Blocks(20, 21)}
	innerB := bucketindex.Claim{Blocks: bucketindex.Blocks(11), Group: bucketindex.Blocks(22, 23)}

	var fragments []bucketindex.Entry
	for b := uint64(20); b <= 23; b++ {
		c := innerA
		if b >= 22 {
			c = innerB
		}

		fragments = append(fragments, bucketindex.Entry{
			Prefix: fmt.Sprintf("f%d", b), Blocks: bucketindex.Blocks(b), Claim: c, Level: 3,
		})
	}

	ancestor := bucketindex.Entry{Prefix: "anc", Blocks: bucketindex.Blocks(1)}
	above := bucketindex.Entry{Prefix: "above", Blocks: bucketindex.Blocks(1, 7), Level: 4}
	live := []bucketindex.Entry{ancestor, above}

	assert.Empty(t, bucketindex.LineageOf(fragments).Subsumed(live, fragments),
		"without the outer claim nothing relates the fragments to block 1")

	got := bucketindex.LineageOf(fragments).With(outer).Subsumed(live, fragments)
	assert.Equal(t, map[string]struct{}{"anc": {}}, got)

	short := fragments[:3]
	assert.Empty(t, bucketindex.LineageOf(short).With(outer).Subsumed(live, short),
		"an incomplete inner group leaves the outer one incomplete")
}

// TestJointlySatisfyingByMergedFragments is the outer group completed by parts that carry no claim:
// each inner group was merged whole, folding its claim into the output's blocks. Only the catalog
// still relates them to {1}, and the want is answered by one of them.
func TestJointlySatisfyingByMergedFragments(t *testing.T) {
	t.Parallel()

	outer := bucketindex.Claim{Blocks: bucketindex.Blocks(1), Group: bucketindex.Blocks(10, 11)}
	innerA := bucketindex.Claim{Blocks: bucketindex.Blocks(10), Group: bucketindex.Blocks(20, 21)}
	innerB := bucketindex.Claim{Blocks: bucketindex.Blocks(11), Group: bucketindex.Blocks(30, 31)}
	want := bucketindex.Want{Prefix: "orig", Blocks: bucketindex.Blocks(1)}

	peer := &bucketindex.Index{
		Entries: []bucketindex.Entry{
			{Prefix: "ma", Blocks: bucketindex.Blocks(10, 20, 21), Level: 3},
			{Prefix: "mb", Blocks: bucketindex.Blocks(11, 30, 31), Level: 3},
			{Prefix: "other", Blocks: bucketindex.Blocks(5), Level: 3},
		},
		Catalog: []bucketindex.Claim{outer, innerA, innerB},
	}

	got, ok := peer.Satisfying(want)
	require.True(t, ok, "the merged inner groups jointly cover {10,11}, which the outer claim says holds {1}")
	assert.Contains(t, []string{"ma", "mb"}, got.Prefix, "answered by a part descending from the split")

	got, ok = peer.Discharging(want)
	require.True(t, ok)
	assert.Contains(t, []string{"ma", "mb"}, got.Prefix)
}

// TestTrimHolesRevokedByCompleteGroup is a hole for {1,2} whose rows came back only as a split: no
// one part holds them, but the complete group does, which is what [bucketindex.Index.Satisfying]
// already answers the hole's want with. So the hole is revoked, through an entry-carried group and
// through one recorded only in the catalog, and not by an incomplete one.
func TestTrimHolesRevokedByCompleteGroup(t *testing.T) {
	t.Parallel()

	outer := bucketindex.Claim{Blocks: bucketindex.Blocks(1, 2), Group: bucketindex.Blocks(10, 11)}
	innerA := bucketindex.Claim{Blocks: bucketindex.Blocks(10), Group: bucketindex.Blocks(20, 21)}
	innerB := bucketindex.Claim{Blocks: bucketindex.Blocks(11), Group: bucketindex.Blocks(22, 23)}
	hole := bucketindex.Entry{Prefix: "lost", Blocks: bucketindex.Blocks(1, 2), Level: 1, Hole: true}

	member := func(b uint64, c bucketindex.Claim, level uint32) bucketindex.Entry {
		return bucketindex.Entry{Prefix: fmt.Sprintf("m%d", b), Blocks: bucketindex.Blocks(b), Claim: c, Level: level}
	}

	members := []bucketindex.Entry{member(10, outer, 2), member(11, outer, 2)}
	fragments := []bucketindex.Entry{member(20, innerA, 3), member(21, innerA, 3), member(22, innerB, 3), member(23, innerB, 3)}

	assert.Empty(t, bucketindex.TrimHoles([]bucketindex.Entry{hole}, members))
	assert.Len(t, bucketindex.TrimHoles([]bucketindex.Entry{hole}, members[:1]), 1)
	assert.Empty(t, bucketindex.TrimHoles([]bucketindex.Entry{hole}, fragments, outer))
	assert.Len(t, bucketindex.TrimHoles([]bucketindex.Entry{hole}, fragments), 1,
		"without the outer claim nothing relates the fragments to {1,2}")
}

// TestTrimCatalogKeepsLiveEntryAncestry is the saturation case for a live nested member: it carries
// only the inner claim, and the outer one lives only in the catalog, with more than MaxLineage newer
// groups after it. The outer claim is kept, since the live member's ancestry passes through it, so
// once the member is lost its want is still answered by a successor of the outer ancestry.
func TestTrimCatalogKeepsLiveEntryAncestry(t *testing.T) {
	t.Parallel()

	member := innerWant.Entry()

	ix := &bucketindex.Index{Catalog: bucketindex.MergeLineage([]bucketindex.Claim{outerClaim})}
	ix.Add(member)
	ix.Catalog = bucketindex.MergeLineage(ix.Catalog, newerGroups(bucketindex.MaxLineage+10))

	assert.Zero(t, ix.RecordLineage())
	require.Len(t, ix.Catalog, bucketindex.MaxLineage)
	assert.True(t, ix.Catalog[0].Equal(outerClaim), "the live member still reaches the oldest group")

	require.True(t, ix.Remove(member.Prefix))
	ix.RecordWant(bucketindex.WantOf(member, bucketindex.Generation{}))

	peer := &bucketindex.Index{Entries: []bucketindex.Entry{successor}}
	_, ok := peer.SatisfyingWith(bucketindex.WantOf(member, bucketindex.Generation{}), ix.Lineage())
	assert.True(t, ok, "so a successor of the outer ancestry answers the lost member")
}
