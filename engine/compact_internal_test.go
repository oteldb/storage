package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// partOfSize builds a bare part of the given size on disk under one series, enough for the merge
// selection logic (which only reads sizeBytes()/minTime/maxTime). seq disambiguates the series key
// so a group can hold several distinct parts, and is also the part's order in src.
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

func sizesOf(parts []*part) []int64 {
	out := make([]int64, len(parts))
	for i, p := range parts {
		out[i] = p.sizeBytes()
	}

	return out
}

// TestPickMergeRunStrandedTiers is #285: five leftover parts, one per former power-of-two size
// tier, that the tier selector could never pair because each was alone in its tier.
//
// Geometric spacing is what makes this hard — every run over them scores below minMergeMultiplier,
// so a purely score-driven selector strands them just as permanently as the tier one did. They must
// merge once the engine has nothing better to do.
func TestPickMergeRunStrandedTiers(t *testing.T) {
	t.Parallel()

	const capBytes = 1 << 30

	strays := []*part{
		partOfSize(0, 1<<20),
		partOfSize(1, 4<<20),
		partOfSize(2, 16<<20),
		partOfSize(3, 64<<20),
		partOfSize(4, 256<<20),
	}

	assert.Nil(t, pickMergeRun(strays, capBytes, 0),
		"while there is other work, the write-amplification guard still applies")

	got := pickMergeRun(strays, capBytes, mergeIdleRounds)
	require.NotEmpty(t, got, "after idling, stranded parts must merge rather than persist forever")
	assert.Equal(t, strays, got, "the whole stray set merges in one pass, in src order")
}

// TestPickMergeRunPrefersLowAmplification pins the scoring: given both a balanced run and a lone
// large part, the selector takes the balanced run and leaves the large one alone.
func TestPickMergeRunPrefersLowAmplification(t *testing.T) {
	t.Parallel()

	small1, small2, small3 := partOfSize(0, 10<<20), partOfSize(1, 11<<20), partOfSize(2, 9<<20)
	big := partOfSize(3, 500<<20)

	got := pickMergeRun([]*part{small1, small2, small3, big}, 1<<30, 0)
	assert.Equal(t, []*part{small1, small2, small3}, got,
		"the similarly-sized run wins; folding them into the big part would rewrite it for little gain")
}

// TestPickMergeRunSpansSizeClasses is the property the tier selector lacked: parts of *different*
// sizes merge, so long as the run stays balanced enough to be worth it.
func TestPickMergeRunSpansSizeClasses(t *testing.T) {
	t.Parallel()

	parts := []*part{partOfSize(0, 10<<20), partOfSize(1, 15<<20), partOfSize(2, 20<<20)}

	got := pickMergeRun(parts, 1<<30, 0)
	assert.Equal(t, parts, got, "adjacent size classes merge; they never shared a power-of-two tier")
}

func TestPickMergeRunSealedExcluded(t *testing.T) {
	t.Parallel()

	// The seal bound is cap/minMergeMultiplier: a part above it can only merge into something over
	// the cap, which would be sealed at once — maximum rewriting for no lasting gain.
	const capBytes = 100

	full1, full2 := partOfSize(0, 90), partOfSize(1, 95)
	assert.Nil(t, pickMergeRun([]*part{full1, full2}, capBytes, mergeIdleRounds), "sealed parts never merge")

	small1, small2 := partOfSize(2, 20), partOfSize(3, 20)
	assert.Equal(t, []*part{small1, small2}, pickMergeRun([]*part{full1, small1, full2, small2}, capBytes, 0),
		"unsealed parts still merge, sealed siblings ignored")
}

func TestPickMergeRunBounds(t *testing.T) {
	t.Parallel()

	t.Run("byte budget", func(t *testing.T) {
		t.Parallel()

		// Eight 9-byte parts against a cap of 20: only what fits is taken this cycle.
		parts := make([]*part, 0, 8)
		for i := range 8 {
			parts = append(parts, partOfSize(i, 9))
		}

		got := pickMergeRun(parts, 20, 0)

		var total int64
		for _, p := range got {
			total += p.sizeBytes()
		}

		assert.GreaterOrEqual(t, len(got), minMergeParts, "always makes progress")
		assert.LessOrEqual(t, total, int64(20), "never exceeds the cap")
	})

	t.Run("part count", func(t *testing.T) {
		t.Parallel()

		// However large the budget, one merge stays bounded in inputs.
		parts := make([]*part, 0, maxMergeParts*3)
		for i := range maxMergeParts * 3 {
			parts = append(parts, partOfSize(i, 1))
		}

		assert.Len(t, pickMergeRun(parts, 1<<40, 0), maxMergeParts)
		assert.Len(t, pickMergeRun(parts, 0, 0), maxMergeParts, "unlimited cap is still bounded by parts")
	})

	t.Run("too few parts", func(t *testing.T) {
		t.Parallel()

		assert.Nil(t, pickMergeRun([]*part{partOfSize(0, 1)}, 0, mergeIdleRounds), "one part is not a run")
		assert.Nil(t, pickMergeRun(nil, 0, mergeIdleRounds))
	})
}

// TestPickMergeRunReturnsSrcOrder guards a correctness dependency: the merge visits its sources
// oldest → newest so a later part's value wins a duplicate timestamp, so the size ordering the
// selector works in must not leak into the result.
func TestPickMergeRunReturnsSrcOrder(t *testing.T) {
	t.Parallel()

	// Deliberately src-ordered opposite to size order.
	newest, middle, oldest := partOfSize(0, 12), partOfSize(1, 11), partOfSize(2, 10)

	got := pickMergeRun([]*part{newest, middle, oldest}, 1<<30, 0)
	require.Len(t, got, 3)
	assert.Equal(t, []*part{newest, middle, oldest}, got, "src order, not size order")
	assert.Equal(t, []int64{12, 11, 10}, sizesOf(got))
}

func TestSelectMergePartsForcedRetention(t *testing.T) {
	t.Parallel()

	// A lone part is still selected when retention must drop some of its data.
	p := partOfSize(0, 1)
	p.minTime, p.maxTime = 100, 300

	assert.Nil(t, selectMergeParts([]*part{p}, MergeOptions{}, 0, 0), "nothing forced, one part ⇒ no-op")

	got := selectMergeParts([]*part{p}, MergeOptions{RetainFrom: 200}, 0, 0)
	require.Equal(t, []*part{p}, got, "retention forces the straddling part to be rewritten")
}

// TestMergeShapeExplainsNoOp pins the diagnostic the no-op log carries: an operator must be able to
// tell "everything is sealed" from "nothing scores well enough yet".
func TestMergeShapeExplainsNoOp(t *testing.T) {
	t.Parallel()

	sealedN, eligible, bestM := mergeShape([]*part{
		partOfSize(0, 90), // sealed: 90 >= 100/1.7
		partOfSize(1, 10),
		partOfSize(2, 10),
	}, 100)

	assert.Equal(t, 1, sealedN)
	assert.Equal(t, 2, eligible)
	assert.InDelta(t, 2.0, bestM, 0.001, "two equal parts score exactly 2x")
}
