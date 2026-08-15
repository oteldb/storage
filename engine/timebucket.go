package engine

import (
	"cmp"
	"slices"
	"time"
)

// Time-bucketed merge selection. Size-tiered selection alone has no notion of time, so a merge
// happily folds a part covering hour 3 into one covering hours 0-48 and the result spans the whole
// store. Every part then overlaps every query window, and a one-hour query opens every part
// (issue #308).
//
// The fix every reference system shares: a merge may never emit a part wider than a pre-agreed
// bucket, and the buckets nest. ClickHouse merges only within a partition; VictoriaMetrics keeps
// monthly partitions; Prometheus compacts along an exponential range ladder; Mimir truncates each
// block to a range start. Parts are grouped by aligned time bucket and the existing size-tiered
// selector runs unchanged inside a group, so a merge's output stays inside its group's bucket.
//
// mergeLadder is that ladder, ascending. Each level must divide the next, so a bucket at one level
// nests exactly inside a bucket at the next and a part promoted upward never straddles: this is
// checked by TestMergeLadderDivides. The top level is the widest part the engine will build, and
// therefore the coarsest time locality a query can rely on.
var mergeLadder = []int64{
	int64(time.Hour),
	int64(6 * time.Hour),
	int64(24 * time.Hour),
}

// bucketOf returns the start of the level-aligned bucket holding ts. The rounding is toward
// negative infinity, not toward zero, so buckets stay contiguous across the epoch — Go's % keeps
// the dividend's sign, which would make the bucket at ts = -1 start at 0 and overlap its successor.
func bucketOf(ts, level int64) int64 {
	m := ts % level
	if m < 0 {
		m += level
	}

	return ts - m
}

// fitsLevel reports whether p lies entirely within one level-aligned bucket, i.e. whether merging
// it with others in that bucket can produce a part no wider than the bucket.
func fitsLevel(p *part, level int64) bool {
	return bucketOf(p.minTime, level) == bucketOf(p.maxTime, level)
}

// finestLevel returns the narrowest ladder level whose bucket contains p whole. It reports false
// for a part that straddles a boundary at every level — one wider than the top level, or one
// written across a boundary before flush-time splitting existed. Such a part cannot join any group
// without widening its group's output, so the selector leaves it alone; splitting it is the
// straddle-split change, not this one.
func finestLevel(p *part) (int64, bool) {
	for _, level := range mergeLadder {
		if fitsLevel(p, level) {
			return level, true
		}
	}

	return 0, false
}

// newestBucket returns the start of the level-aligned bucket holding the newest sample in src. That
// bucket is still filling, so it is the one merging is premature in.
func newestBucket(src []*part, level int64) int64 {
	newest := minInt64
	for _, p := range src {
		newest = max(newest, p.maxTime)
	}

	return bucketOf(newest, level)
}

// partitionGroups groups the parts that fit level by their aligned bucket, returning the bucket
// starts in ascending order alongside the groups so selection is deterministic and prefers older
// data (which is settled, and whose compaction therefore is not about to be undone by the next
// flush).
//
// Above the finest level the still-filling newest bucket is skipped: merging it now only guarantees
// merging it again once the rest of the bucket arrives, which is write amplification bought for
// nothing. The finest level is exempt because that is where freshly flushed parts land — leaving
// them uncompacted until the bucket closes would let part count grow unbounded within it, the very
// thing size-tiered selection exists to prevent.
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

// selectLadderRun walks the ladder from the narrowest level up and returns the first size-tiered run
// found in any bucket. Narrowest-first is what makes the ladder a ladder: a bucket's parts are
// collapsed at level L before the result is eligible to merge with its neighbors at level L+1, so
// each part is rewritten once per level rather than repeatedly at the widest one.
func selectLadderRun(src []*part, capBytes int64, idle int) []*part {
	for _, level := range mergeLadder {
		_, groups := partitionGroups(src, level)

		for _, group := range groups {
			// A group of one is already collapsed at this level; it is promoted by the next.
			if len(group) < minMergeParts {
				continue
			}

			if run := pickMergeRun(group, capBytes, idle); len(run) > 0 {
				return run
			}
		}
	}

	return nil
}

// selectForced returns the parts a forced rewrite (retention, downsampling, recompression,
// precision) must touch this cycle, confined to a single bucket.
//
// The confinement is the point. The forced set was previously unioned with the size-tiered run and
// merged as one part, so a retention pass over a store with any age spread would rewrite parts from
// opposite ends of it into a single part spanning both — re-widening exactly what the ladder
// narrows, and doing it on the cycle most likely to run.
//
// The oldest forced part decides the bucket, so age-driven work still progresses oldest-first, one
// bucket per cycle. A part straddling every level is rewritten alone rather than skipped: retention
// correctness does not get to wait on the straddle-split change.
//
// The rest of that bucket is folded in when it fits the cap. Merging inside a bucket cannot widen
// the output past the bucket, so absorbing the neighbors costs one rewrite that was already
// happening and leaves no fragment behind — which is what the old unconfined union got right, and
// all it got right.
func selectForced(src []*part, opts MergeOptions, capBytes int64) []*part {
	var oldest *part

	for _, p := range src {
		if forcedRewrite(p, opts) && (oldest == nil || p.minTime < oldest.minTime) {
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
	)

	for _, p := range src {
		switch {
		case !inBucket(p):
		case forcedRewrite(p, opts):
			out = append(out, p)
			total += p.sizeBytes()
		case !sealed(p, capBytes):
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

	return inSrcOrder(out, srcOrder(src))
}

// srcOrder indexes src by position so a selection can be restored to it; the merge visits sources
// oldest → newest so a later part's value wins a duplicate timestamp.
func srcOrder(src []*part) map[*part]int {
	order := make(map[*part]int, len(src))
	for i, p := range src {
		order[p] = i
	}

	return order
}
