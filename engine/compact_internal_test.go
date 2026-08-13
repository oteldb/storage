package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// partOfSize builds a bare part of the given size on disk under one series, enough for the
// size-tiered selection logic (which only reads sizeBytes()/minTime/maxTime). seq disambiguates the
// series key so a group can hold several distinct parts.
func partOfSize(seq int, size int64) *part {
	return &part{
		prefix:    string(rune('a' + seq)),
		diskBytes: size,
		index: partIndex{
			ids:    []signal.SeriesID{{Hi: uint64(seq)}},
			starts: []int32{0, 1},
		},
	}
}

func TestSizeTier(t *testing.T) {
	t.Parallel()

	// Everything at or below the floor collapses to tier 0, so many tiny parts always share a tier.
	assert.Equal(t, 0, sizeTier(0))
	assert.Equal(t, 0, sizeTier(1))
	assert.Equal(t, 0, sizeTier(tierFloorBytes))

	// Above the floor, tiers advance at power-of-two size boundaries (⌊log2(n)⌋ − ⌊log2(floor)⌋).
	assert.Equal(t, 0, sizeTier(tierFloorBytes+1))
	assert.Equal(t, 1, sizeTier(2*tierFloorBytes))
	assert.Equal(t, 1, sizeTier(2*tierFloorBytes+1))
	assert.Equal(t, 2, sizeTier(4*tierFloorBytes))

	// Monotonic non-decreasing in size.
	prev := 0
	for n := int64(1); n < 64*tierFloorBytes; n *= 2 {
		got := sizeTier(n)
		assert.GreaterOrEqual(t, got, prev)
		prev = got
	}
}

func TestPickTierGroupUnlimited(t *testing.T) {
	t.Parallel()

	// Unlimited part size (cap 0): nothing is sealed, tiny parts share tier 0 and compact together.
	p0, p1, p2 := partOfSize(0, 1), partOfSize(1, 2), partOfSize(2, 3)
	group := pickTierGroup([]*part{p0, p1, p2}, 0)
	assert.ElementsMatch(t, []*part{p0, p1, p2}, group, "tiny parts all land in tier 0 and merge")

	// A single part is below the minimum group size, so there is nothing to compact.
	assert.Nil(t, pickTierGroup([]*part{p0}, 0))
}

func TestPickTierGroupSealedExcluded(t *testing.T) {
	t.Parallel()

	// cap 5: a part at the cap is sealed (re-merging it is pure churn) and never selected.
	full1, full2 := partOfSize(0, 5), partOfSize(1, 5)
	assert.Nil(t, pickTierGroup([]*part{full1, full2}, 5), "two sealed parts are not re-merged")

	// Unsealed parts of the same tier still compact, sealed siblings ignored.
	small1, small2 := partOfSize(2, 2), partOfSize(3, 2)
	group := pickTierGroup([]*part{full1, small1, full2, small2}, 5)
	assert.ElementsMatch(t, []*part{small1, small2}, group)
}

func TestPickTierGroupDifferentTiersDoNotMerge(t *testing.T) {
	t.Parallel()

	// One big part (its own tier) and one small part (tier 0): neither tier reaches the threshold, so
	// no compaction — the hallmark of size-tiered selection (don't merge across size classes).
	big := partOfSize(0, 8*tierFloorBytes)
	small := partOfSize(1, 1)
	assert.Nil(t, pickTierGroup([]*part{big, small}, 0))
}

func TestPickTierGroupByteBudgetCap(t *testing.T) {
	t.Parallel()

	// The cap bounds one merge's output: with cap 20 and eight 9-byte parts in one tier, only enough
	// to reach the cap are taken this cycle (the rest wait for the next).
	const mergeCap = 20

	parts := make([]*part, 0, 8)
	for i := range 8 {
		parts = append(parts, partOfSize(i, 9)) // all tier 0, all below the cap
	}

	group := pickTierGroup(parts, mergeCap)

	var total int64
	for _, p := range group {
		total += p.sizeBytes()
	}

	assert.GreaterOrEqual(t, len(group), minTierParts, "always makes progress")
	assert.Less(t, len(group), len(parts), "the byte budget caps the group below the full tier")
	assert.GreaterOrEqual(t, total, int64(mergeCap), "takes parts up to the cap")
}

// TestPickTierGroupPartCountCap pins the second bound: however large the byte budget grows, one
// merge never takes more than maxTierParts inputs, so a disk-derived cap cannot turn a single merge
// into an unbounded one.
func TestPickTierGroupPartCountCap(t *testing.T) {
	t.Parallel()

	parts := make([]*part, 0, maxTierParts*3)
	for i := range maxTierParts * 3 {
		parts = append(parts, partOfSize(i, 1))
	}

	assert.Len(t, pickTierGroup(parts, 1<<40), maxTierParts, "a huge budget is still bounded by parts")
	assert.Len(t, pickTierGroup(parts, 0), maxTierParts, "as is an unlimited one")
}

func TestSelectMergePartsForcedRetention(t *testing.T) {
	t.Parallel()

	// A lone part below the tier threshold is still selected when retention must drop some of its data.
	p := partOfSize(0, 1)
	p.minTime, p.maxTime = 100, 300

	assert.Nil(t, selectMergeParts([]*part{p}, MergeOptions{}, 0), "nothing forced, one part ⇒ no-op")

	got := selectMergeParts([]*part{p}, MergeOptions{RetainFrom: 200}, 0)
	require.Equal(t, []*part{p}, got, "retention forces the straddling part to be rewritten")
}
