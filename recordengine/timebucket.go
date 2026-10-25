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

func spanOf(parts []*part) (lo, hi int64) { return timebucket.Union(parts, partSpan) }

// selectStraddlers returns the straddlers to split this cycle. A straddler crosses a top-level
// boundary, which one late record makes since flush does not split by time, so it fits no level and
// joins no ladder group.
func selectStraddlers(src []*part, capBytes int64) []*part {
	return timebucket.Straddlers(src, partSpan, (*part).sizeBytes, capBytes, maxTierParts)
}

// newestBucket is the level-aligned bucket holding the newest record in src.
func newestBucket(src []*part, level int64) int64 {
	newest := minInt64
	for _, p := range src {
		newest = max(newest, p.maxTime)
	}

	return bucketOf(newest, level)
}

// partitionGroups groups compactable parts by aligned bucket, oldest first.
// Fine parts in a newest larger bucket wait for promotion; already-wide parts may merge there.
func partitionGroups(src []*part, level int64) ([]int64, [][]*part) {
	groups := make(map[int64][]*part, len(src))
	newest := newestBucket(src, level)

	for _, p := range src {
		if timebucket.CanCompact(p.minTime, p.maxTime, level, newest) {
			b := bucketOf(p.minTime, level)
			groups[b] = append(groups[b], p)
		}
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
	idle := 0
	if force {
		idle = mergeIdleRounds
	}
	return selectLadderGroupIdle(src, capBytes, idle)
}

func selectLadderGroupIdle(src []*part, capBytes int64, idle int) []*part {
	return selectLadderGroupExpected(src, capBytes, idle, nil)
}

func selectLadderGroupExpected(src []*part, capBytes int64, idle int, expected func([]*part) int) []*part {
	for _, level := range mergeLadder {
		_, groups := partitionGroups(src, level)

		for _, group := range groups {
			// A group of one is already collapsed at this level; the next level promotes it.
			if len(group) < minTierParts {
				continue
			}

			if picked := pickMergeRunExpected(group, capBytes, idle, expected); len(picked) > 0 {
				return picked
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
		capBytes, maxTierParts)
}
