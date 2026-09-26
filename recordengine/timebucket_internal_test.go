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

	// The newer part ends on the day boundary, so it is a straddler and may be selected alone.
	assert.Less(t, len(selectMergeParts(src, 0, 64<<20, false)), 2,
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

// TestSelectForcedRewritesStraddlerAlone checks a straddler retention forces is rewritten alone:
// it belongs to no bucket, and the merge splits it on day boundaries.
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
