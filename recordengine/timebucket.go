package recordengine

import (
	"cmp"
	"slices"
	"time"
)

// Time-bucketed merge selection for the record engine, mirroring engine/timebucket.go. Size-tiered
// selection has no notion of time, so a merge folded a part covering one hour into one covering the
// whole retention and the output spanned all of it; every part then overlapped every query window.
//
// This matters more for records than for metrics. Log and trace queries are overwhelmingly narrow
// and recent ("the last 15 minutes of one service"), and a record row carries far more bytes than a
// sample, so opening a part the window did not need costs more.
//
// mergeLadder is the bucket ladder, ascending; each level must divide the next so buckets nest
// exactly (TestMergeLadderDivides). The top level is the widest part the engine will build, and so
// the coarsest time locality a query can rely on.
var mergeLadder = []int64{
	int64(time.Hour),
	int64(6 * time.Hour),
	int64(24 * time.Hour),
}

// bucketOf returns the start of the level-aligned bucket holding ts, rounding toward negative
// infinity so buckets stay contiguous across the epoch (Go's % keeps the dividend's sign, which
// would make the bucket at ts = -1 start at 0 and overlap its successor).
func bucketOf(ts, level int64) int64 {
	m := ts % level
	if m < 0 {
		m += level
	}

	return ts - m
}

// fitsLevel reports whether p lies entirely within one level-aligned bucket.
func fitsLevel(p *part, level int64) bool {
	return bucketOf(p.minTime, level) == bucketOf(p.maxTime, level)
}

// finestLevel returns the narrowest ladder level whose bucket contains p whole, reporting false for
// a part that straddles a boundary at every level — one wider than the top level, or one written
// before bucketing existed. Such a part cannot join a group without widening its group's output.
func finestLevel(p *part) (int64, bool) {
	for _, level := range mergeLadder {
		if fitsLevel(p, level) {
			return level, true
		}
	}

	return 0, false
}

// newestBucket returns the start of the level-aligned bucket holding the newest record in src —
// the bucket still filling, and so the one merging is premature in.
func newestBucket(src []*part, level int64) int64 {
	newest := minInt64
	for _, p := range src {
		newest = max(newest, p.maxTime)
	}

	return bucketOf(newest, level)
}

// partitionGroups groups the parts that fit level by their aligned bucket, oldest bucket first so
// selection is deterministic and prefers settled data.
//
// Above the finest level the still-filling newest bucket is skipped: merging it now only guarantees
// merging it again once the rest of the bucket arrives. The finest level is exempt because that is
// where freshly flushed parts land, and leaving them uncompacted until the bucket closes would let
// part count grow unbounded within it.
func partitionGroups(src []*part, level int64) ([]int64, [][]*part) {
	groups := make(map[int64][]*part, len(src))

	for _, p := range src {
		if fitsLevel(p, level) {
			b := bucketOf(p.minTime, level)
			groups[b] = append(groups[b], p)
		}
	}

	if level != mergeLadder[0] {
		delete(groups, newestBucket(src, level))
	}

	starts := make([]int64, 0, len(groups))
	for b := range groups {
		starts = append(starts, b)
	}

	slices.Sort(starts)

	out := make([][]*part, 0, len(starts))
	for _, b := range starts {
		out = append(out, groups[b])
	}

	return starts, out
}

// selectLadderGroup walks the ladder from the narrowest level up and returns the first tier group
// found in any bucket. Narrowest-first is what makes the ladder a ladder: a bucket's parts are
// collapsed at level L before the result is eligible to merge with its neighbors at level L+1, so
// each part is rewritten once per level rather than repeatedly at the widest one.
func selectLadderGroup(src []*part, capBytes int64, force bool) []*part {
	for _, level := range mergeLadder {
		_, groups := partitionGroups(src, level)

		for _, group := range groups {
			// A group of one is already collapsed at this level; the next level promotes it.
			if len(group) < minTierParts {
				continue
			}

			if picked := pickTierGroup(group, capBytes); len(picked) > 0 {
				return picked
			}

			if force {
				if picked := pickForcedGroup(group, capBytes); len(picked) > 0 {
					return picked
				}
			}
		}
	}

	return nil
}

// selectForced returns the parts retention must rewrite this cycle, confined to a single bucket.
//
// The confinement is the point: the forced set was previously unioned with the tier group and merged
// as one part, so a retention pass over a store with any age spread rewrote parts from opposite ends
// of it into a single part spanning both — re-widening on the cycle most likely to run.
//
// The oldest forced part picks the bucket, so age-driven work still progresses oldest-first, one
// bucket per cycle. The rest of that bucket rides along when it fits the cap, since merging inside a
// bucket cannot widen the output and leaving a co-located part behind would only fragment. A part
// straddling every level is rewritten alone rather than skipped: retention correctness does not wait
// on straddle splitting.
func selectForced(src []*part, retainFrom, capBytes int64) []*part {
	var oldest *part

	for _, p := range src {
		if retentionForces(p, retainFrom) && (oldest == nil || p.minTime < oldest.minTime) {
			oldest = p
		}
	}

	if oldest == nil {
		return nil
	}

	level, ok := finestLevel(oldest)
	if !ok {
		return []*part{oldest}
	}

	bucket := bucketOf(oldest.minTime, level)
	inBucket := func(p *part) bool {
		return fitsLevel(p, level) && bucketOf(p.minTime, level) == bucket
	}

	var (
		out    []*part
		others []*part
		total  int64
		order  = make(map[*part]int, len(src))
	)

	for i, p := range src {
		order[p] = i

		switch {
		case !inBucket(p):
		case retentionForces(p, retainFrom):
			out = append(out, p)
			total += p.sizeBytes()
		case capBytes <= 0 || p.sizeBytes() < capBytes:
			others = append(others, p)
		}
	}

	// Smallest first, so a cap cutoff strands the part that is cheapest to carry.
	slices.SortFunc(others, func(a, b *part) int { return cmp.Compare(a.sizeBytes(), b.sizeBytes()) })

	for _, p := range others {
		if capBytes > 0 && total+p.sizeBytes() > capBytes {
			break
		}

		out = append(out, p)
		total += p.sizeBytes()
	}

	// Restore the engine's part order: the merge visits sources oldest → newest so a later part's
	// record wins a duplicate.
	slices.SortFunc(out, func(a, b *part) int { return cmp.Compare(order[a], order[b]) })

	return out
}
