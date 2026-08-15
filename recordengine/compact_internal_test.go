package recordengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// partOfSize builds a bare part of the given decoded size under one stream, enough for the
// size-tiered selection logic (which reads sizeBytes()/minTime). seq disambiguates the stream key so
// a group can hold several distinct parts.
func partOfSize(seq int, bytes int64) *part {
	return &part{
		prefix:   string(rune('a' + seq)),
		ranges:   map[signal.SeriesID]rowRange{{Hi: uint64(seq)}: {start: 0, end: 1}},
		rawBytes: bytes,
	}
}

func TestSizeTier(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, sizeTier(0))
	assert.Equal(t, 0, sizeTier(1))
	assert.Equal(t, 0, sizeTier(tierFloorBytes))

	// Above the floor, tiers advance at power-of-two byte boundaries.
	assert.Equal(t, 0, sizeTier(tierFloorBytes+1))
	assert.Equal(t, 1, sizeTier(2*tierFloorBytes))
	assert.Equal(t, 2, sizeTier(4*tierFloorBytes))

	prev := 0
	for b := int64(1); b < 64*tierFloorBytes; b *= 2 {
		got := sizeTier(b)
		assert.GreaterOrEqual(t, got, prev)
		prev = got
	}
}

// TestPartSizeBytesFallsBackToRows pins the compatibility path: a part written before the manifest
// carried a decoded size tiers on the old per-row model rather than as an empty part.
func TestPartSizeBytesFallsBackToRows(t *testing.T) {
	t.Parallel()

	sized := partOfSize(0, 4096)
	assert.Equal(t, int64(4096), sized.sizeBytes())

	legacy := &part{ranges: map[signal.SeriesID]rowRange{{Hi: 1}: {start: 0, end: 10}}}
	assert.Equal(t, int64(10*recordRowBytes), legacy.sizeBytes())
}

func TestPickTierGroupUnlimited(t *testing.T) {
	t.Parallel()

	// Unlimited part size (capBytes 0): nothing seals, tiny parts share tier 0 and compact together.
	p0, p1, p2 := partOfSize(0, 1), partOfSize(1, 2), partOfSize(2, 3)
	group := pickTierGroup([]*part{p0, p1, p2}, 0)
	assert.ElementsMatch(t, []*part{p0, p1, p2}, group)

	// A single part is below the minimum group size, so there is nothing to compact.
	assert.Nil(t, pickTierGroup([]*part{p0}, 0))
}

func TestPickTierGroupSealedExcluded(t *testing.T) {
	t.Parallel()

	// capBytes 5: a part at the cap is sealed and never selected (re-merging it is pure churn).
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
	// no compaction — the hallmark of size-tiered selection.
	big := partOfSize(0, 8*tierFloorBytes)
	small := partOfSize(1, 1)
	assert.Nil(t, pickTierGroup([]*part{big, small}, 0))
}

func TestPickTierGroupByteBudgetCap(t *testing.T) {
	t.Parallel()

	// capBytes bounds the decoded input: with cap 20 and eight 9-byte parts in one tier, only enough
	// to reach the cap are taken this cycle (the rest wait for the next) — this is what keeps a
	// single merge's working set O(cap) instead of O(dataset).
	const capBytes = 20

	parts := make([]*part, 0, 8)
	for i := range 8 {
		parts = append(parts, partOfSize(i, 9)) // all tier 0, all below the cap
	}

	group := pickTierGroup(parts, capBytes)

	var total int64
	for _, p := range group {
		total += p.sizeBytes()
	}

	assert.GreaterOrEqual(t, len(group), minTierParts, "always makes progress")
	assert.Less(t, len(group), len(parts), "the byte budget caps the group below the full tier")
	assert.GreaterOrEqual(t, total, int64(capBytes), "takes parts up to the cap")
}

// TestPickTierGroupIgnoresRowCountForSizing pins what the byte accounting buys: two parts of equal
// row count but very different record sizes are not the same size, and only the byte figure knows.
func TestPickTierGroupIgnoresRowCountForSizing(t *testing.T) {
	t.Parallel()

	fat := partOfSize(0, 16*tierFloorBytes)
	thin1, thin2 := partOfSize(1, 1024), partOfSize(2, 1024)

	// All three hold one row; under row-count tiering they shared a tier and the fat part would be
	// pulled into a merge whose working set is 16 MiB, not 2 KiB.
	group := pickTierGroup([]*part{fat, thin1, thin2}, 0)
	assert.ElementsMatch(t, []*part{thin1, thin2}, group)
}

func TestSelectMergePartsForcedRetention(t *testing.T) {
	t.Parallel()

	// A lone part below the tier threshold is still selected when retention must drop some of its data.
	p := partOfSize(0, 1)
	p.minTime, p.maxTime = 100, 300

	assert.Nil(t, selectMergeParts([]*part{p}, 0, 0, false), "nothing forced, one part ⇒ no-op")

	got := selectMergeParts([]*part{p}, 200, 0, false)
	require.Equal(t, []*part{p}, got, "retention forces the straddling part to be rewritten")
}

func TestMergeCapBytes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		maxPart     int64
		memory      int64
		concurrency int
		want        int64
	}{
		{
			name:    "unlimited part size never seals",
			memory:  1 << 40,
			maxPart: 0,
			want:    0,
		},
		{
			// Roomy memory: the tiering target stands.
			name:    "the tiering target stands",
			maxPart: 64 << 20,
			memory:  1 << 40,
			want:    mergeHeight * (64 << 20),
		},
		{
			// The shape that OOMed: a target sized for the disk, in a process that cannot hold it.
			// 384 MiB of merge memory, thirded for sources + output + encode, is 128 MiB.
			name:    "memory lowers the target",
			maxPart: 64 << 20,
			memory:  384 << 20,
			want:    128 << 20,
		},
		{
			name:        "concurrency divides the memory",
			maxPart:     64 << 20,
			memory:      384 << 20,
			concurrency: 4,
			want:        64 << 20, // 32 MiB share, floored at one flushed part
		},
		{
			name:    "negative memory opts out",
			maxPart: 64 << 20,
			memory:  -1,
			want:    mergeHeight * (64 << 20),
		},
		{
			// A merge must still be able to rewrite one flushed part, or retention could never run.
			name:    "the flush cap floors the merge cap",
			maxPart: 64 << 20,
			memory:  1 << 10,
			want:    64 << 20,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var concurrency func() int
			if tc.concurrency > 0 {
				concurrency = func() int { return tc.concurrency }
			}

			e := &Engine{cfg: Config{
				MaxPartBytes:     tc.maxPart,
				MergeMemoryBytes: tc.memory,
				MergeConcurrency: concurrency,
			}}

			assert.Equal(t, tc.want, e.mergeCapBytes())
		})
	}
}
