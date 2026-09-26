package engine

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	straddleCapBytes  = 64 << 20
	straddlePartBytes = 1 << 20
	straddlePartCount = 5
	// straddleRounds bounds a merge loop that must reach its fixpoint well before it.
	straddleRounds = 1000
)

// partsAcrossDayBoundary returns five 2-minute parts that each cross the same day boundary, so each
// straddles every ladder level.
func partsAcrossDayBoundary() []*part {
	out := make([]*part, 0, straddlePartCount)
	for i := range straddlePartCount {
		start := day - int64(time.Minute) + int64(i)*int64(10*time.Second)
		out = append(out, partAt(i, straddlePartBytes, start, start+2*int64(time.Minute)))
	}

	return out
}

// partsWithinOneHour returns the same five 2-minute parts shifted to sit inside one 1h bucket: same
// width, count, size and co-location, differing from [partsAcrossDayBoundary] only in alignment.
func partsWithinOneHour() []*part {
	out := make([]*part, 0, straddlePartCount)
	for i := range straddlePartCount {
		start := hour + int64(time.Minute) + int64(i)*int64(10*time.Second)
		out = append(out, partAt(i, straddlePartBytes, start, start+2*int64(time.Minute)))
	}

	return out
}

// compactToFixpoint applies selectMergeParts until it selects nothing, replacing each selection
// with the parts that merge would produce. idle is saturated so the score guard never decides the
// outcome.
func compactToFixpoint(tb testing.TB, src []*part) []*part {
	tb.Helper()

	parts := slices.Clone(src)
	seq := len(src)

	for range straddleRounds {
		selected := selectMergeParts(parts, MergeOptions{}, straddleCapBytes, mergeIdleRounds)
		if len(selected) == 0 {
			return parts
		}

		parts, _ = applyMerge(parts, selected, &seq)
	}

	require.FailNow(tb, "merge selection did not reach a fixpoint", "%d rounds", straddleRounds)

	return nil
}

// mergeOutputs models what merging selected writes: the union span cut on top-level boundaries,
// one part per bucket, the bytes shared evenly. The real merge writes a bucket only when a sample
// lands in it, so the model is an upper bound on the part count.
func mergeOutputs(selected []*part, seq *int) []*part {
	lo, hi := spanOf(selected)

	var total int64
	for _, p := range selected {
		total += p.sizeBytes()
	}

	n := (bucketOf(hi, topLevel())-bucketOf(lo, topLevel()))/topLevel() + 1
	out := make([]*part, 0, n)

	for b := bucketOf(lo, topLevel()); b <= hi; b += topLevel() {
		out = append(out, partAt(*seq, max(total/n, 1), max(lo, b), min(hi, b+topLevel()-1)))
		*seq++
	}

	return out
}

// applyMerge replaces selected with [mergeOutputs] of it, returning the new part set and the
// outputs.
func applyMerge(parts, selected []*part, seq *int) (next, merged []*part) {
	merged = mergeOutputs(selected, seq)

	next = make([]*part, 0, len(parts)-len(selected)+len(merged))
	for _, p := range parts {
		if !slices.Contains(selected, p) {
			next = append(next, p)
		}
	}

	return append(next, merged...), merged
}

// TestStraddlingPartsCompact is #450: a part that crosses a boundary at every ladder level fits no
// level, so partitionGroups drops it from every group and size-driven selection never sees it,
// however small it is.
//
// The assertion is only that merging reduces the part count, not the shape straddlers end up in.
func TestStraddlingPartsCompact(t *testing.T) {
	t.Parallel()

	src := partsAcrossDayBoundary()
	for _, p := range src {
		require.NotEqual(t, bucketOf(p.minTime, day), bucketOf(p.maxTime, day),
			"the fixture must cross the day boundary")
	}

	got := compactToFixpoint(t, src)
	assert.Less(t, len(got), len(src), "repeated merges must reduce the part count")

	for _, p := range got {
		_, ok := finestLevel(p)
		assert.True(t, ok, "[%v,%v] still fits no ladder level", time.Duration(p.minTime), time.Duration(p.maxTime))
	}
}

// TestCoLocatedPartsCompact is the control for [TestStraddlingPartsCompact]: the same five parts
// aligned inside one 1h bucket, under the same assertion.
func TestCoLocatedPartsCompact(t *testing.T) {
	t.Parallel()

	src := partsWithinOneHour()

	got := compactToFixpoint(t, src)
	assert.Less(t, len(got), len(src), "repeated merges must reduce the part count")
}

// partsAcrossDaysWithStraddlers is the adversarial shape: parts spread across four days at wildly
// differing sizes — what the size-only selector used to collapse into one store-wide part — plus
// the day-boundary straddlers, so grouping has something to get wrong.
func partsAcrossDaysWithStraddlers() []*part {
	src := make([]*part, 0, 24+straddlePartCount)
	for i := range 24 {
		start := int64(i) * 4 * hour
		src = append(src, partAt(i, int64(1<<uint(10+i%7)), start, start+hour-1))
	}

	for i, p := range partsAcrossDayBoundary() {
		src = append(src, partAt(24+i, p.sizeBytes(), p.minTime, p.maxTime))
	}

	return src
}

// randomPopulation returns normal parts, each inside one random 1h/6h/24h bucket, mixed with
// straddlers of random span up to four days, across a week, at sizes from 1 KiB to 32 MiB.
func randomPopulation(rng *rand.Rand) []*part {
	n := 1 + rng.IntN(64)
	out := make([]*part, 0, n)

	for i := range n {
		size := int64(1) << (10 + rng.IntN(16))
		start := rng.Int64N(7 * day)

		if rng.IntN(3) == 0 {
			out = append(out, partAt(i, size, start, start+1+rng.Int64N(4*day)))

			continue
		}

		level := mergeLadder[rng.IntN(len(mergeLadder))]
		lo := bucketOf(start, level)
		out = append(out, partAt(i, size, lo+rng.Int64N(level/2), bucketEnd(lo, level)-rng.Int64N(level/2)))
	}

	return out
}

// TestMergeConvergesWithStraddlers is the convergence property over random populations: repeated
// merges reach a fixpoint and the fixpoint holds no straddler.
func TestMergeConvergesWithStraddlers(t *testing.T) {
	t.Parallel()

	for seed := range uint64(256) {
		got := compactToFixpoint(t, randomPopulation(rand.New(rand.NewPCG(seed, seed+1))))

		for _, p := range got {
			_, ok := finestLevel(p)
			require.True(t, ok, "seed %d: [%v,%v] straddles at the fixpoint",
				seed, time.Duration(p.minTime), time.Duration(p.maxTime))
		}
	}
}

// TestSelectMergePartsOutputFitsALadderLevel is the alignment half of the no-widening property. The
// width bound alone admits a narrow part that fits no bucket at any level — five 2-minute parts
// across a day boundary merge to 2m40s, well under the top level — and such a part is then invisible
// to the ladder. So every part a merge of the selection writes must fit a level.
func TestSelectMergePartsOutputFitsALadderLevel(t *testing.T) {
	t.Parallel()

	for idle := range mergeIdleRounds + 1 {
		parts := partsAcrossDaysWithStraddlers()
		seq := len(parts)

		for round := range straddleRounds {
			selected := selectMergeParts(parts, MergeOptions{}, straddleCapBytes, idle)
			if len(selected) == 0 {
				break
			}

			var merged []*part

			parts, merged = applyMerge(parts, selected, &seq)

			for _, p := range merged {
				_, ok := finestLevel(p)
				require.True(t, ok,
					"idle=%d round=%d: merging %d parts writes [%v,%v], %s wide, which fits no ladder level",
					idle, round, len(selected), time.Duration(p.minTime), time.Duration(p.maxTime),
					time.Duration(p.maxTime-p.minTime))
			}
		}
	}
}
