package recordengine

import (
	"slices"

	"github.com/oteldb/storage/internal/timebucket"
)

// Time-bucketed merge selection for the record engine, mirroring engine/timebucket.go. Size-tiered
// selection has no notion of time, so a merge folded a part covering one hour into one covering the
// whole retention and the output spanned all of it; every part then overlapped every query window.
//
// This matters more for records than for metrics. Log and trace queries are overwhelmingly narrow
// and recent ("the last 15 minutes of one service"), and a record row carries far more bytes than a
// sample, so opening a part the window did not need costs more.
//
// The ladder and its bucket arithmetic are [timebucket]'s, shared with the metric engine.
var mergeLadder = timebucket.Ladder

func bucketOf(ts, level int64) int64 { return timebucket.Of(ts, level) }

func partSpan(p *part) (lo, hi int64) { return p.minTime, p.maxTime }

func fitsLevel(p *part, level int64) bool { return timebucket.Fits(p.minTime, p.maxTime, level) }

func spanOf(parts []*part) (lo, hi int64) { return timebucket.Union(parts, partSpan) }

// selectStraddlers returns the straddlers to split this cycle. A straddler crosses a top-level
// boundary, which one late record makes since flush does not split by time, so it fits no level and
// joins no ladder group.
func selectStraddlers(src []*part, capBytes int64) []*part {
	return timebucket.Straddlers(src, partSpan, (*part).sizeBytes, capBytes, uncappedMergeParts(capBytes))
}

// uncappedMergeParts bounds a merge's part count only when part size is unlimited, where no byte
// budget does.
func uncappedMergeParts(capBytes int64) int {
	if capBytes <= 0 {
		return maxTierParts
	}

	return 0
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

// selectForced returns the parts retention must rewrite this cycle: one bucket's worth, up to the cap
// ([timebucket.Forced]).
func selectForced(src []*part, retainFrom, capBytes int64) []*part {
	return timebucket.Forced(src, partSpan, (*part).sizeBytes,
		func(p *part) bool { return retentionForces(p, retainFrom) },
		func(p *part) bool { return sealed(p, capBytes) },
		capBytes, uncappedMergeParts(capBytes))
}
