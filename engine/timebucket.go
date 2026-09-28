package engine

import (
	"slices"

	"github.com/oteldb/storage/internal/timebucket"
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
// The ladder and its bucket arithmetic are [timebucket]'s, shared with the record engine.
var mergeLadder = timebucket.Ladder

func bucketOf(ts, level int64) int64 { return timebucket.Of(ts, level) }

func partSpan(p *part) (lo, hi int64) { return p.minTime, p.maxTime }

func fitsLevel(p *part, level int64) bool { return timebucket.Fits(p.minTime, p.maxTime, level) }

func spanOf(parts []*part) (lo, hi int64) { return timebucket.Union(parts, partSpan) }

// splitsOutput reports whether merging src from start on writes more than one top-level bucket, which
// only [Engine.compactStream] cuts on.
func splitsOutput(src []*part, start int64) bool { return timebucket.SplitsFrom(src, partSpan, start) }

// selectStraddlers returns the straddlers to split this cycle, at most maxMergeParts of them. A
// straddler crosses a top-level boundary, which one late sample makes since flush does not split by
// time, so it fits no level and joins no ladder group.
func selectStraddlers(src []*part, capBytes int64) []*part {
	return timebucket.Straddlers(src, partSpan, (*part).sizeBytes, capBytes, maxMergeParts)
}

// newestSample is the newest sample in src; the level-aligned bucket holding it is still filling, so
// it is the one merging is premature in.
func newestSample(src []*part) int64 {
	newest := minInt64
	for _, p := range src {
		newest = max(newest, p.maxTime)
	}

	return newest
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
	return partitionGroupsBefore(src, level, newestSample(src))
}

// partitionGroupsBefore is [partitionGroups] with the still-filling bucket the one holding newest,
// which a caller selecting from a subset of the parts takes from all of them.
func partitionGroupsBefore(src []*part, level, newest int64) ([]int64, [][]*part) {
	groups := make(map[int64][]*part, len(src))

	for _, p := range src {
		if fitsLevel(p, level) {
			b := bucketOf(p.minTime, level)
			groups[b] = append(groups[b], p)
		}
	}

	if level != mergeLadder[0] {
		delete(groups, bucketOf(newest, level))
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
	return ladderRun(src, capBytes, idle, newestSample(src))
}

func ladderRun(src []*part, capBytes int64, idle int, newest int64) []*part {
	for _, level := range mergeLadder {
		_, groups := partitionGroupsBefore(src, level, newest)

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
// precision) must touch this cycle: one bucket's worth, up to the cap and maxMergeParts
// ([timebucket.Forced]).
func selectForced(src []*part, opts MergeOptions, capBytes int64) []*part {
	return timebucket.Forced(src, partSpan, (*part).sizeBytes,
		func(p *part) bool { return forcedRewrite(p, opts) },
		func(p *part) bool { return sealed(p, capBytes) },
		capBytes, maxMergeParts)
}
