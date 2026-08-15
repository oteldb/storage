package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	hour = int64(time.Hour)
	day  = 24 * hour
)

// partAt builds a bare part spanning [minTime, maxTime] for the selection logic, which reads only
// sizeBytes()/minTime/maxTime.
func partAt(seq int, size, minTime, maxTime int64) *part {
	p := partOfSize(seq, size)
	p.minTime, p.maxTime = minTime, maxTime

	return p
}

// spanOf returns the span a merge of parts would produce — the union of their bounds, which is what
// the output part's own bounds become.
func spanOf(parts []*part) (lo, hi int64) {
	lo, hi = maxInt64, minInt64
	for _, p := range parts {
		lo, hi = min(lo, p.minTime), max(hi, p.maxTime)
	}

	return lo, hi
}

// TestMergeLadderDivides is the invariant the ladder rests on: each level divides the next, so a
// bucket nests exactly inside its parent and a part that fits level L still fits level L+1. Without
// it a part could fit a narrow bucket yet straddle the wide one containing it, and promotion would
// silently widen a part past the level it was promoted to.
func TestMergeLadderDivides(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, mergeLadder)

	for i, level := range mergeLadder {
		require.Positive(t, level, "level %d must be positive", i)

		if i == 0 {
			continue
		}

		prev := mergeLadder[i-1]
		require.Greater(t, level, prev, "levels must ascend")
		require.Zero(t, level%prev, "level %d (%s) must divide by %s", i, time.Duration(level), time.Duration(prev))
	}
}

// TestBucketOfFloorsTowardNegativeInfinity pins the rounding. Go's % keeps the dividend's sign, so
// the naive ts-ts%level puts ts=-1 in the bucket starting at 0 — the same bucket as ts=+1 — and two
// parts an hour apart would be judged co-located. Timestamps before the epoch are unusual but not
// impossible, and a selector that silently widens on them is worse than one that refuses.
func TestBucketOfFloorsTowardNegativeInfinity(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		ts, want int64
	}{
		{0, 0},
		{1, 0},
		{hour - 1, 0},
		{hour, hour},
		{-1, -hour},
		{-hour, -hour},
		{-hour - 1, -2 * hour},
	} {
		assert.Equal(t, tt.want, bucketOf(tt.ts, hour), "bucketOf(%d)", tt.ts)
	}
}

func TestFinestLevel(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		minT, maxT int64
		want       int64
		ok         bool
	}{
		{"within an hour", 0, hour - 1, hour, true},
		{"straddles an hour, within six", hour - 1, hour + 1, 6 * hour, true},
		{"straddles six hours, within a day", 6*hour - 1, 6*hour + 1, day, true},
		{"straddles a day", day - 1, day + 1, 0, false},
		{"wider than the top level", 0, 3 * day, 0, false},
		{"instant", 5 * hour, 5 * hour, hour, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			level, ok := finestLevel(partAt(0, 1, tt.minT, tt.maxT))
			assert.Equal(t, tt.ok, ok)

			if tt.ok {
				assert.Equal(t, time.Duration(tt.want), time.Duration(level))
			}
		})
	}
}

// TestSelectMergePartsNeverWidensPastTopLevel is the property issue #308 is about: whatever the
// selector returns, merging it must not produce a part wider than the widest ladder level. The
// inputs are deliberately adversarial — parts spread across four days at wildly different sizes,
// which is exactly the shape the old size-only selector collapsed into one store-wide part.
func TestSelectMergePartsNeverWidensPastTopLevel(t *testing.T) {
	t.Parallel()

	top := mergeLadder[len(mergeLadder)-1]

	src := make([]*part, 0, 24)
	for i := range 24 {
		start := int64(i) * 4 * hour
		src = append(src, partAt(i, int64(1<<uint(10+i%7)), start, start+hour-1))
	}

	for idle := range mergeIdleRounds + 1 {
		selected := selectMergeParts(src, MergeOptions{}, 64<<20, idle)
		if len(selected) == 0 {
			continue
		}

		lo, hi := spanOf(selected)
		assert.LessOrEqual(t, hi-lo, top,
			"idle=%d: merging the selection would span %s, past the %s top level",
			idle, time.Duration(hi-lo), time.Duration(top))
	}
}

// TestSelectMergePartsSpansStoreWithoutBuckets is the regression the whole change exists to
// prevent, stated as the counterfactual: fed parts from opposite ends of a 48h store, the selector
// must not put them in one run. Before bucketing it did, and the result spanned the store.
func TestSelectMergePartsSpansStoreWithoutBuckets(t *testing.T) {
	t.Parallel()

	src := []*part{
		partAt(0, 1<<20, 0, hour),
		partAt(1, 1<<20, 47*hour, 48*hour),
	}

	assert.Empty(t, selectMergeParts(src, MergeOptions{}, 64<<20, mergeIdleRounds),
		"parts two days apart share no bucket at any level, so no merge may pair them")
}

// TestSelectLadderRunPrefersNarrowestLevel checks the ladder is walked bottom-up: two parts inside
// one hour must be collapsed before anything at the 6h level is considered, so each part is
// rewritten once per level instead of repeatedly at the widest one.
func TestSelectLadderRunPrefersNarrowestLevel(t *testing.T) {
	t.Parallel()

	// Two parts inside one hour, plus two more in different hours of the same 6h bucket: the
	// latter are mergeable at 6h, but not before the pair above is collapsed at 1h.
	spread := []*part{
		partAt(0, 1<<20, 0, hour-1),
		partAt(1, 1<<20, 0, hour-1),
		partAt(2, 1<<20, 2*hour, 3*hour-1),
		partAt(3, 1<<20, 3*hour, 4*hour-1),
	}

	got := selectLadderRun(spread, 64<<20, 0)
	require.Len(t, got, 2)

	lo, hi := spanOf(got)
	assert.Less(t, hi-lo, hour, "the 1h bucket must be collapsed before the 6h one is considered")
}

// TestPartitionGroupsSkipsFillingBucketAboveFinest checks the premature-compaction guard: the
// newest bucket is still receiving data, so merging it now guarantees merging it again. The finest
// level is exempt because that is where flushes land, and letting them accumulate uncompacted is
// the part-count growth size-tiering exists to bound.
func TestPartitionGroupsSkipsFillingBucketAboveFinest(t *testing.T) {
	t.Parallel()

	src := []*part{
		partAt(0, 1<<20, 0, hour-1),        // settled 6h bucket
		partAt(1, 1<<20, hour, 2*hour-1),   // same settled 6h bucket
		partAt(2, 1<<20, 6*hour, 7*hour-1), // newest 6h bucket, still filling
		partAt(3, 1<<20, 7*hour, 8*hour-1), // same
	}

	starts, _ := partitionGroups(src, 6*hour)
	assert.Equal(t, []int64{0}, starts, "the bucket holding the newest sample must be left alone")

	starts, _ = partitionGroups(src, hour)
	assert.Equal(t, []int64{0, hour, 6 * hour, 7 * hour}, starts,
		"the finest level is exempt: freshly flushed parts must still compact")
}

// TestSelectForcedConfinedToBucket is the forced-rewrite half of the widening bug. Retention forces
// the oldest part; the selector must not drag in a part two days newer just because it also needs
// rewriting, because the merged result would span both.
func TestSelectForcedConfinedToBucket(t *testing.T) {
	t.Parallel()

	oldA := partAt(0, 1<<20, 0, hour-1)
	oldB := partAt(1, 1<<20, hour, 2*hour-1)
	farNewer := partAt(2, 1<<20, 40*hour, 41*hour-1)

	// A cutoff past all three forces every part.
	got := selectForced([]*part{oldA, oldB, farNewer}, MergeOptions{RetainFrom: 50 * hour}, 64<<20)
	require.NotEmpty(t, got)

	assert.NotContains(t, got, farNewer, "a part 40h newer shares no bucket with the oldest")

	lo, hi := spanOf(got)
	assert.LessOrEqual(t, hi-lo, mergeLadder[len(mergeLadder)-1])
}

// TestSelectForcedAbsorbsBucketNeighbours checks the forced path still folds in the rest of its
// bucket. Merging inside a bucket cannot widen the output, so leaving a co-located part behind
// would fragment where the old unconfined union did not.
func TestSelectForcedAbsorbsBucketNeighbours(t *testing.T) {
	t.Parallel()

	forced := partAt(0, 1<<20, 0, hour-1)
	neighbor := partAt(1, 1<<20, 10*time.Minute.Nanoseconds(), hour-1)

	got := selectForced([]*part{forced, neighbor}, MergeOptions{RetainFrom: 30 * time.Minute.Nanoseconds()}, 64<<20)
	assert.Equal(t, []*part{forced, neighbor}, got,
		"a co-located unforced part rides along rather than being left as a fragment")
}

// TestSelectForcedRewritesStraddlerAlone checks retention correctness does not wait on straddle
// splitting: a part crossing every level's boundary belongs to no bucket, and must still be
// rewritten rather than silently skipped forever.
func TestSelectForcedRewritesStraddlerAlone(t *testing.T) {
	t.Parallel()

	straddler := partAt(0, 1<<20, 0, 3*day)
	other := partAt(1, 1<<20, 5*day, 5*day+hour)

	got := selectForced([]*part{straddler, other}, MergeOptions{RetainFrom: day}, 64<<20)
	assert.Equal(t, []*part{straddler}, got)
}

// TestSelectForcedWinsTheCycle checks forced work is not unioned with a size-tiered run from another
// bucket — the union is precisely what widened parts before.
func TestSelectForcedWinsTheCycle(t *testing.T) {
	t.Parallel()

	old := partAt(0, 1<<20, 0, hour-1)
	pairA := partAt(1, 1<<20, 30*hour, 30*hour+60)
	pairB := partAt(2, 1<<20, 30*hour+61, 31*hour-1)

	got := selectMergeParts([]*part{old, pairA, pairB}, MergeOptions{RetainFrom: 30 * time.Minute.Nanoseconds()}, 64<<20, 0)
	assert.Equal(t, []*part{old}, got, "the size-tiered pair waits for the next cycle")
}
