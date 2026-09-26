package recordengine

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
		out = append(out, partAt(straddlePartBytes, start, start+2*int64(time.Minute)))
	}

	return out
}

// partsWithinOneHour returns the same five 2-minute parts shifted to sit inside one 1h bucket: same
// width, count, size and co-location, differing from [partsAcrossDayBoundary] only in alignment.
func partsWithinOneHour() []*part {
	out := make([]*part, 0, straddlePartCount)
	for i := range straddlePartCount {
		start := hour + int64(time.Minute) + int64(i)*int64(10*time.Second)
		out = append(out, partAt(straddlePartBytes, start, start+2*int64(time.Minute)))
	}

	return out
}

// compactToFixpoint applies selectMergeParts until it selects nothing, replacing each selection
// with the parts that merge would produce.
func compactToFixpoint(tb testing.TB, src []*part, force bool) []*part {
	tb.Helper()

	parts := slices.Clone(src)

	for range straddleRounds {
		selected := selectMergeParts(parts, 0, straddleCapBytes, force)
		if len(selected) == 0 {
			return parts
		}

		parts, _ = applyMerge(parts, selected)
	}

	require.FailNow(tb, "merge selection did not reach a fixpoint", "%d rounds", straddleRounds)

	return nil
}

// mergeOutputs models what merging selected writes: the union span cut on top-level boundaries,
// one part per bucket, the bytes shared evenly. The real merge writes a bucket only when a record
// lands in it, so the model is an upper bound on the part count.
func mergeOutputs(selected []*part) []*part {
	lo, hi := spanOf(selected)

	var total int64
	for _, p := range selected {
		total += p.sizeBytes()
	}

	var out []*part

	for b := bucketOf(lo, topLevel()); b <= hi; b += topLevel() {
		out = append(out, partAt(0, max(lo, b), min(hi, b+topLevel()-1)))
	}

	for _, p := range out {
		p.rawBytes = max(total/int64(len(out)), 1)
	}

	return out
}

// applyMerge replaces selected with [mergeOutputs] of it, returning the new part set and the
// outputs.
func applyMerge(parts, selected []*part) (next, merged []*part) {
	merged = mergeOutputs(selected)

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
// however small it is. On a live stand that was 12,735 exemplar parts and no merge in days.
//
// The assertion is only that merging reduces the part count, not the shape straddlers end up in.
func TestStraddlingPartsCompact(t *testing.T) {
	t.Parallel()

	src := partsAcrossDayBoundary()
	for _, p := range src {
		require.NotEqual(t, bucketOf(p.minTime, day), bucketOf(p.maxTime, day),
			"the fixture must cross the day boundary")
	}

	got := compactToFixpoint(t, src, false)
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

	got := compactToFixpoint(t, src, false)
	assert.Less(t, len(got), len(src), "repeated merges must reduce the part count")
}

// partsAcrossDaysWithStraddlers is the adversarial shape: parts spread across four days at wildly
// differing sizes — what the size-only selector used to collapse into one store-wide part — plus
// the day-boundary straddlers, so grouping has something to get wrong.
func partsAcrossDaysWithStraddlers() []*part {
	src := make([]*part, 0, 24+straddlePartCount)
	for i := range 24 {
		start := int64(i) * 4 * hour
		src = append(src, partAt(int64(1<<uint(10+i%7)), start, start+hour-1))
	}

	return append(src, partsAcrossDayBoundary()...)
}

// randomPopulation returns normal parts, each inside one random 1h/6h/24h bucket, mixed with
// straddlers of random span up to four days, across a week, at sizes from 1 KiB to 32 MiB.
func randomPopulation(rng *rand.Rand) []*part {
	n := 1 + rng.IntN(64)
	out := make([]*part, 0, n)

	for range n {
		size := int64(1) << (10 + rng.IntN(16))
		start := rng.Int64N(7 * day)

		if rng.IntN(3) == 0 {
			out = append(out, partAt(size, start, start+1+rng.Int64N(4*day)))

			continue
		}

		level := mergeLadder[rng.IntN(len(mergeLadder))]
		lo := bucketOf(start, level)
		out = append(out, partAt(size, lo+rng.Int64N(level/2), bucketEnd(lo, level)-rng.Int64N(level/2)))
	}

	return out
}

// TestMergeConvergesWithStraddlers is the convergence property over random populations: repeated
// merges reach a fixpoint, forced or not, and the fixpoint holds no straddler.
func TestMergeConvergesWithStraddlers(t *testing.T) {
	t.Parallel()

	for seed := range uint64(256) {
		for _, force := range []bool{false, true} {
			got := compactToFixpoint(t, randomPopulation(rand.New(rand.NewPCG(seed, seed+1))), force)

			for _, p := range got {
				_, ok := finestLevel(p)
				require.True(t, ok, "seed %d force=%v: [%v,%v] straddles at the fixpoint",
					seed, force, time.Duration(p.minTime), time.Duration(p.maxTime))
			}
		}
	}
}

// TestSelectMergePartsOutputFitsALadderLevel is the alignment half of the no-widening property. The
// width bound alone admits a narrow part that fits no bucket at any level — five 2-minute parts
// across a day boundary merge to 2m40s, well under the top level — and such a part is then invisible
// to the ladder. So every part a merge of the selection writes must fit a level.
func TestSelectMergePartsOutputFitsALadderLevel(t *testing.T) {
	t.Parallel()

	for _, force := range []bool{false, true} {
		parts := partsAcrossDaysWithStraddlers()

		for round := range straddleRounds {
			selected := selectMergeParts(parts, 0, straddleCapBytes, force)
			if len(selected) == 0 {
				break
			}

			var merged []*part

			parts, merged = applyMerge(parts, selected)

			for _, p := range merged {
				_, ok := finestLevel(p)
				require.True(t, ok,
					"force=%v round=%d: merging %d parts writes [%v,%v], %s wide, which fits no ladder level",
					force, round, len(selected), time.Duration(p.minTime), time.Duration(p.maxTime),
					time.Duration(p.maxTime-p.minTime))
			}
		}
	}
}
