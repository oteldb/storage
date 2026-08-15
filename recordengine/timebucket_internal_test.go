package recordengine

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

// partAt builds a bare part spanning [minTime, maxTime] of the given decoded size — all the
// selection logic reads.
func partAt(size, minTime, maxTime int64) *part {
	return &part{rawBytes: size, minTime: minTime, maxTime: maxTime}
}

// spanOf returns the span a merge of parts would produce: the union of their bounds, which becomes
// the output part's own.
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

// TestBucketOfFloorsTowardNegativeInfinity pins the rounding: Go's % keeps the dividend's sign, so
// the naive ts-ts%level puts ts=-1 in the bucket starting at 0 — the same bucket as ts=+1 — and two
// parts an hour apart would be judged co-located.
func TestBucketOfFloorsTowardNegativeInfinity(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		ts, want int64
	}{
		{0, 0},
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
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			level, ok := finestLevel(partAt(1, tt.minT, tt.maxT))
			assert.Equal(t, tt.ok, ok)

			if tt.ok {
				assert.Equal(t, time.Duration(tt.want), time.Duration(level))
			}
		})
	}
}

// TestSelectMergePartsNeverWidensPastTopLevel is the property the change exists for: whatever the
// selector returns, merging it must not produce a part wider than the widest ladder level. The
// inputs are the shape the old size-only selector collapsed into one store-wide part — parts spread
// across four days at wildly different sizes.
func TestSelectMergePartsNeverWidensPastTopLevel(t *testing.T) {
	t.Parallel()

	top := mergeLadder[len(mergeLadder)-1]

	src := make([]*part, 0, 24)
	for i := range 24 {
		start := int64(i) * 4 * hour
		src = append(src, partAt(int64(1<<uint(10+i%7)), start, start+hour-1))
	}

	selected := selectMergeParts(src, 0, 64<<20, false)
	if len(selected) == 0 {
		return
	}

	lo, hi := spanOf(selected)
	assert.LessOrEqual(t, hi-lo, top,
		"merging the selection would span %s, past the %s top level",
		time.Duration(hi-lo), time.Duration(top))
}

// TestSelectMergePartsRefusesDistantParts is the regression stated as a counterfactual: fed parts
// from opposite ends of a 48h store, the selector must not put them in one group. Before bucketing
// it did, and the merged result spanned the store.
func TestSelectMergePartsRefusesDistantParts(t *testing.T) {
	t.Parallel()

	src := []*part{
		partAt(1<<20, 0, hour),
		partAt(1<<20, 47*hour, 48*hour),
	}

	assert.Empty(t, selectMergeParts(src, 0, 64<<20, false),
		"parts two days apart share no bucket at any level, so no merge may pair them")
}

// TestSelectLadderGroupPrefersNarrowestLevel checks the ladder is walked bottom-up: two parts inside
// one hour are collapsed before anything at the 6h level is considered, so each part is rewritten
// once per level instead of repeatedly at the widest one.
func TestSelectLadderGroupPrefersNarrowestLevel(t *testing.T) {
	t.Parallel()

	spread := []*part{
		partAt(1<<20, 0, hour-1),
		partAt(1<<20, 0, hour-1),
		partAt(1<<20, 2*hour, 3*hour-1),
		partAt(1<<20, 3*hour, 4*hour-1),
	}

	got := selectLadderGroup(spread, 64<<20, false)
	require.Len(t, got, 2)

	lo, hi := spanOf(got)
	assert.Less(t, hi-lo, hour, "the 1h bucket must be collapsed before the 6h one is considered")
}

// TestPartitionGroupsSkipsFillingBucketAboveFinest checks the premature-compaction guard: the newest
// bucket is still receiving records, so merging it now guarantees merging it again. The finest level
// is exempt, since that is where flushes land.
func TestPartitionGroupsSkipsFillingBucketAboveFinest(t *testing.T) {
	t.Parallel()

	src := []*part{
		partAt(1<<20, 0, hour-1),
		partAt(1<<20, hour, 2*hour-1),
		partAt(1<<20, 6*hour, 7*hour-1),
		partAt(1<<20, 7*hour, 8*hour-1),
	}

	starts, _ := partitionGroups(src, 6*hour)
	assert.Equal(t, []int64{0}, starts, "the bucket holding the newest record must be left alone")

	starts, _ = partitionGroups(src, hour)
	assert.Equal(t, []int64{0, hour, 6 * hour, 7 * hour}, starts,
		"the finest level is exempt: freshly flushed parts must still compact")
}

// TestSelectForcedConfinedToBucket is the retention half of the widening bug: retention forces every
// part, and the selector must not drag in one two days newer, because the merged result would span
// both.
func TestSelectForcedConfinedToBucket(t *testing.T) {
	t.Parallel()

	oldA := partAt(1<<20, 0, hour-1)
	oldB := partAt(1<<20, hour, 2*hour-1)
	farNewer := partAt(1<<20, 40*hour, 41*hour-1)

	got := selectForced([]*part{oldA, oldB, farNewer}, 50*hour, 64<<20)
	require.NotEmpty(t, got)

	assert.NotContains(t, got, farNewer, "a part 40h newer shares no bucket with the oldest")

	lo, hi := spanOf(got)
	assert.LessOrEqual(t, hi-lo, mergeLadder[len(mergeLadder)-1])
}

// TestSelectForcedRewritesStraddlerAlone checks retention correctness does not wait on straddle
// splitting: a part crossing every level's boundary belongs to no bucket and must still be
// rewritten rather than skipped forever.
func TestSelectForcedRewritesStraddlerAlone(t *testing.T) {
	t.Parallel()

	straddler := partAt(1<<20, 0, 3*day)
	other := partAt(1<<20, 5*day, 5*day+hour)

	got := selectForced([]*part{straddler, other}, day, 64<<20)
	assert.Equal(t, []*part{straddler}, got)
}

// TestSelectForcedWinsTheCycle checks forced work is not unioned with a tier group from another
// bucket — that union is precisely what widened parts before.
func TestSelectForcedWinsTheCycle(t *testing.T) {
	t.Parallel()

	old := partAt(1<<20, 0, hour-1)
	pairA := partAt(1<<20, 30*hour, 30*hour+60)
	pairB := partAt(1<<20, 30*hour+61, 31*hour-1)

	got := selectMergeParts([]*part{old, pairA, pairB}, int64(30*time.Minute), 64<<20, false)
	assert.Equal(t, []*part{old}, got, "the tier group waits for the next cycle")
}
