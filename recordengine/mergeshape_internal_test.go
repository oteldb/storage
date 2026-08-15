package recordengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShapeOfAccounting(t *testing.T) {
	t.Parallel()

	const capBytes = 64 << 20

	// One sealed part, plus two unsealed ones two tiers apart: neither tier reaches minTierParts,
	// so nothing is selected although both remain mergeable.
	parts := []*part{
		partOfSize(0, capBytes),
		partOfSize(1, 1<<20),
		partOfSize(2, 16<<20),
	}

	sh := shapeOf(parts, capBytes)
	assert.Equal(t, 3, sh.Parts)
	assert.Equal(t, 1, sh.Sealed)
	assert.Equal(t, 2, sh.Backlog)
	assert.Zero(t, sh.Candidates, "no tier holds minTierParts — the stuck state")
	assert.Equal(t, int64(capBytes), sh.CapBytes)
	assert.Equal(t, 2, sh.Tiers)
	assert.Equal(t, 1, sh.LargestTierParts)
	assert.Equal(t, minTierParts, sh.MinTierParts)

	// Two parts in one tier are selectable, and the shape says so.
	sh = shapeOf([]*part{partOfSize(0, 1<<20), partOfSize(1, 2<<20)}, capBytes)
	assert.Equal(t, 1, sh.Tiers)
	assert.Equal(t, 2, sh.LargestTierParts)
	assert.Equal(t, 2, sh.Candidates)
}

func TestPickForcedGroup(t *testing.T) {
	t.Parallel()

	const capBytes = 64 << 20

	small, mid, big := partOfSize(0, 1<<20), partOfSize(1, 16<<20), partOfSize(2, capBytes)
	src := []*part{big, mid, small}

	require.Nil(t, pickTierGroup(src, capBytes), "the tier rule declines: one part per tier")

	got := pickForcedGroup(src, capBytes)
	assert.Equal(t, []*part{mid, small}, got,
		"the unsealed parts merge across tiers, in src order; the sealed one stays out")

	// The cumulative-bytes cap still bounds the group: the two smallest already reach it.
	a, b, c := partOfSize(0, 40<<20), partOfSize(1, 50<<20), partOfSize(2, 30<<20)
	assert.Equal(t, []*part{a, c}, pickForcedGroup([]*part{a, b, c}, capBytes),
		"the two smallest are chosen and reach the cap, and are handed back in src order")

	// Nothing to gain from a single unsealed part.
	assert.Nil(t, pickForcedGroup([]*part{partOfSize(0, capBytes), partOfSize(1, 1<<20)}, capBytes))
}

// TestSelectMergePartsForce pins the entry point: the same part set the plain selector declines is
// compacted when the caller forces it, and retention still wins the cycle when it applies.
func TestSelectMergePartsForce(t *testing.T) {
	t.Parallel()

	const capBytes = 64 << 20

	src := []*part{partOfSize(0, 1<<20), partOfSize(1, 16<<20)}

	require.Nil(t, selectMergeParts(src, 0, capBytes, false))
	assert.Len(t, selectMergeParts(src, 0, capBytes, true), 2)
}
