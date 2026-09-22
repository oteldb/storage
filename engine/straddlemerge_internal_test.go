package engine

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/reproduce"
)

const (
	straddleCapBytes  = 64 << 20
	straddlePartBytes = 1 << 20
	straddlePartCount = 5
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

// compactToFixpoint applies selectMergeParts until it stops reducing the part count, replacing each
// selection with the part that merge would produce. A selection of fewer than two parts rewrites a
// part into itself and ends the loop; every other round drops at least one part, so len(src) rounds
// reach the fixpoint. idle is saturated so the score guard never decides the outcome.
func compactToFixpoint(tb testing.TB, src []*part) []*part {
	tb.Helper()

	parts := slices.Clone(src)

	for round := range src {
		selected := selectMergeParts(parts, MergeOptions{}, straddleCapBytes, mergeIdleRounds)
		if len(selected) < 2 {
			break
		}

		parts = applyMerge(parts, selected, straddlePartCount+round)
	}

	return parts
}

// applyMerge replaces selected with the single part merging them yields: their union span, their
// summed size.
func applyMerge(parts, selected []*part, seq int) []*part {
	lo, hi := spanOf(selected)

	var total int64
	for _, p := range selected {
		total += p.sizeBytes()
	}

	out := make([]*part, 0, len(parts)-len(selected)+1)
	for _, p := range parts {
		if !slices.Contains(selected, p) {
			out = append(out, p)
		}
	}

	return append(out, partAt(seq, total, lo, hi))
}

// TestStraddlingPartsCompact is the defect: a part that crosses a boundary at every ladder level
// fits no level, so partitionGroups drops it from every group and size-driven selection can never
// see it, however small it is. Five such parts sit two minutes apart and still never merge.
//
// The assertion is only that merging reduces the part count, not that the straddlers end up in one
// part: which shape a fix gives them is the fix's choice, not the defect's.
func TestStraddlingPartsCompact(t *testing.T) {
	t.Parallel()
	reproduce.Unfixed(t, 450, "a part straddling every ladder level joins no merge group, so no number of merges reduces the part count")

	src := partsAcrossDayBoundary()
	for _, p := range src {
		require.NotEqual(t, bucketOf(p.minTime, day), bucketOf(p.maxTime, day),
			"the fixture must cross the day boundary")
	}

	got := compactToFixpoint(t, src)
	assert.Less(t, len(got), len(src), "repeated merges must reduce the part count")
}

// TestCoLocatedPartsCompact is the control for [TestStraddlingPartsCompact]: the same five parts
// aligned inside one 1h bucket, under the same assertion. It passes today, which is what makes
// alignment the only variable between the two.
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

	return append(src, partsAcrossDayBoundary()...)
}

// TestSelectMergePartsOutputFitsALadderLevel is the alignment half of the no-widening property. The
// width bound alone admits a narrow part that fits no bucket at any level — five 2-minute parts
// across a day boundary merge to 2m40s, well under the top level — and such a part is then invisible
// to every later merge, which is the defect [TestStraddlingPartsCompact] holds open. So whatever the
// size-driven path selects, the part merging it would produce must still fit a level.
//
// The forced path is exempt: retention rewrites a lone straddler on purpose
// ([TestSelectForcedRewritesStraddlerAlone]), so only selections of two or more parts are checked.
func TestSelectMergePartsOutputFitsALadderLevel(t *testing.T) {
	t.Parallel()

	for idle := range mergeIdleRounds + 1 {
		parts := partsAcrossDaysWithStraddlers()

		for round := range parts {
			selected := selectMergeParts(parts, MergeOptions{}, straddleCapBytes, idle)
			if len(selected) < 2 {
				break
			}

			parts = applyMerge(parts, selected, len(parts))

			merged := parts[len(parts)-1]
			_, ok := finestLevel(merged)
			require.True(t, ok,
				"idle=%d round=%d: merging %d parts yields [%v,%v], %s wide, which fits no ladder level",
				idle, round, len(selected), time.Duration(merged.minTime), time.Duration(merged.maxTime),
				time.Duration(merged.maxTime-merged.minTime))
		}
	}
}
