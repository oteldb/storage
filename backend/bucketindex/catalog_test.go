package bucketindex_test

import (
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

func TestLineageCatalogIsBounded(t *testing.T) {
	t.Parallel()

	var catalog []bucketindex.Claim
	for i := range uint64(bucketindex.MaxLineage + 10) {
		catalog = bucketindex.MergeLineage(catalog, []bucketindex.Claim{{
			Blocks: bucketindex.Blocks(1), Group: bucketindex.Blocks(i + 2),
		}})
	}

	require.Len(t, catalog, bucketindex.MaxLineage)
	assert.Equal(t, bucketindex.Blocks(12), catalog[0].Group, "the oldest groups age out first")
	assert.Nil(t, bucketindex.TrimLineage(nil))
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
