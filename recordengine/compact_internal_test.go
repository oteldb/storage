package recordengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// partWithRows builds a bare part of the given row count under one stream, enough for the size-tiered
// selection logic (which only reads rows()/minTime). seq disambiguates the stream key so a group can
// hold several distinct parts.
func partWithRows(seq, rows int) *part {
	return &part{
		prefix: string(rune('a' + seq)),
		ranges: map[signal.SeriesID]rowRange{{Hi: uint64(seq)}: {start: 0, end: rows}},
	}
}

func TestSizeTier(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, sizeTier(0))
	assert.Equal(t, 0, sizeTier(1))
	assert.Equal(t, 0, sizeTier(tierFloorRows))

	// Above the floor, tiers advance at power-of-two row boundaries.
	assert.Equal(t, 0, sizeTier(tierFloorRows+1))
	assert.Equal(t, 1, sizeTier(2*tierFloorRows))
	assert.Equal(t, 2, sizeTier(4*tierFloorRows))

	prev := 0
	for r := 1; r < 64*tierFloorRows; r *= 2 {
		got := sizeTier(r)
		assert.GreaterOrEqual(t, got, prev)
		prev = got
	}
}

func TestPickTierGroupUnlimited(t *testing.T) {
	t.Parallel()

	// Unlimited part size (capRows 0): nothing seals, tiny parts share tier 0 and compact together.
	p0, p1, p2 := partWithRows(0, 1), partWithRows(1, 2), partWithRows(2, 3)
	group := pickTierGroup([]*part{p0, p1, p2}, 0)
	assert.ElementsMatch(t, []*part{p0, p1, p2}, group)

	// A single part is below the minimum group size, so there is nothing to compact.
	assert.Nil(t, pickTierGroup([]*part{p0}, 0))
}

func TestPickTierGroupSealedExcluded(t *testing.T) {
	t.Parallel()

	// capRows 5: a part at the cap is sealed and never selected (re-merging it is pure churn).
	full1, full2 := partWithRows(0, 5), partWithRows(1, 5)
	assert.Nil(t, pickTierGroup([]*part{full1, full2}, 5), "two sealed parts are not re-merged")

	// Unsealed parts of the same tier still compact, sealed siblings ignored.
	small1, small2 := partWithRows(2, 2), partWithRows(3, 2)
	group := pickTierGroup([]*part{full1, small1, full2, small2}, 5)
	assert.ElementsMatch(t, []*part{small1, small2}, group)
}

func TestPickTierGroupDifferentTiersDoNotMerge(t *testing.T) {
	t.Parallel()

	// One big part (its own tier) and one small part (tier 0): neither tier reaches the threshold, so
	// no compaction — the hallmark of size-tiered selection.
	big := partWithRows(0, 8*tierFloorRows)
	small := partWithRows(1, 1)
	assert.Nil(t, pickTierGroup([]*part{big, small}, 0))
}

func TestPickTierGroupRowBudgetCap(t *testing.T) {
	t.Parallel()

	// capRows bounds the decoded input: with cap 20 and eight 9-row parts in one tier, only enough to
	// reach the cap are taken this cycle (the rest wait for the next) — this is what keeps a single
	// merge's working set O(cap) instead of O(dataset).
	const capRows = 20

	parts := make([]*part, 0, 8)
	for i := range 8 {
		parts = append(parts, partWithRows(i, 9)) // all tier 0, all below the cap
	}

	group := pickTierGroup(parts, capRows)
	total := 0
	for _, p := range group {
		total += int(p.rows())
	}

	assert.GreaterOrEqual(t, len(group), minTierParts, "always makes progress")
	assert.Less(t, len(group), len(parts), "the row budget caps the group below the full tier")
	assert.GreaterOrEqual(t, total, capRows, "takes parts up to the cap")
}

func TestSelectMergePartsForcedRetention(t *testing.T) {
	t.Parallel()

	// A lone part below the tier threshold is still selected when retention must drop some of its data.
	p := partWithRows(0, 1)
	p.minTime, p.maxTime = 100, 300

	assert.Nil(t, selectMergeParts([]*part{p}, 0, 0), "nothing forced, one part ⇒ no-op")

	got := selectMergeParts([]*part{p}, 200, 0)
	require.Equal(t, []*part{p}, got, "retention forces the straddling part to be rewritten")
}

func TestMaxRowsAndMergeCap(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, maxRowsPerPart(0), "unlimited byte cap ⇒ unlimited rows")
	assert.Equal(t, 0, mergeCapRows(0), "unlimited ⇒ never seal")

	assert.Equal(t, 1, maxRowsPerPart(1), "a sub-record cap still makes progress with one row")
	assert.Equal(t, 4, maxRowsPerPart(int64(4*recordRowBytes)))
	assert.Equal(t, 4*mergeHeight, mergeCapRows(4))
}
