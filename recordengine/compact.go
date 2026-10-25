package recordengine

import (
	"cmp"
	"math/bits"
	"slices"

	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/signal"
)

const (
	minTierParts       = 2
	maxTierParts       = 16
	minMergeMultiplier = 1.7
	smallRunBytes      = 128 << 10
	mergeIdleRounds    = 3
	// Size tiers remain diagnostic only; selection scores stored-byte runs.
	tierFloorBytes       = 128 << 10
	legacyRecordRowBytes = 1024
)

// sizeTier buckets a part by its stored size into a tier, so two parts within 2× of each other
// (above the floor) share a tier. Parts at or below tierFloorBytes are all tier 0.
func sizeTier(bytes int64) int {
	if bytes <= tierFloorBytes {
		return 0
	}

	return bits.Len64(uint64(bytes)) - bits.Len64(uint64(tierFloorBytes))
}

// retentionForces reports whether retention must rewrite part p this merge (it holds a record old
// enough to drop). retainFrom ≤ 0 disables retention.
func retentionForces(p *part, retainFrom int64) bool {
	return retainFrom > 0 && p.minTime < retainFrom
}

// selectMergeParts prefers retention, a scored run in one ladder bucket, then straddlers.
// Force waives scoring; stored bytes and source count still bound every selection.
func selectMergeParts(src []*part, retainFrom, capBytes int64, force bool) []*part {
	idle := 0
	if force {
		idle = mergeIdleRounds
	}
	return selectMergePartsIdle(src, retainFrom, capBytes, idle)
}

func selectMergePartsIdle(src []*part, retainFrom, capBytes int64, idle int) []*part {
	if forced := selectForced(src, retainFrom, capBytes); len(forced) > 0 {
		return forced
	}
	if group := selectLadderGroupIdle(src, capBytes, idle); len(group) > 0 {
		return group
	}
	return selectStraddlers(src, capBytes)
}

// pickTierGroup retains the selector's test seam; selection no longer requires a shared tier.
func pickTierGroup(src []*part, capBytes int64) []*part { return pickMergeRun(src, capBytes, 0) }

func pickForcedGroup(src []*part, capBytes int64) []*part {
	return pickMergeRun(src, capBytes, mergeIdleRounds)
}

func sealed(p *part, capBytes int64) bool {
	return capBytes > 0 && float64(p.sizeBytes()) >= float64(capBytes)/minMergeMultiplier
}

func unsealedOf(src []*part, capBytes int64) []*part {
	out := make([]*part, 0, len(src))
	for _, p := range src {
		if !sealed(p, capBytes) {
			out = append(out, p)
		}
	}
	return out
}

func pickMergeRun(src []*part, capBytes int64, idle int) []*part {
	return pickMergeRunExpected(src, capBytes, idle, nil)
}

func pickMergeRunExpected(src []*part, capBytes int64, idle int, expected func([]*part) int) []*part {
	bySize, order := eligibleBySize(src, capBytes)
	if len(bySize) < minTierParts {
		return nil
	}

	best, fallback, _ := scanRunsExpected(bySize, capBytes, expected)
	if best == nil {
		if idle < mergeIdleRounds {
			return nil
		}

		best = fallback
	}

	return inSrcOrder(best, order)
}

// eligibleBySize returns the unsealed parts ordered by size, plus each part's position in src so
// the chosen run can be restored to that order.
func eligibleBySize(src []*part, capBytes int64) ([]*part, map[*part]int) {
	bySize := make([]*part, 0, len(src))
	order := make(map[*part]int, len(src))

	for i, p := range src {
		order[p] = i

		if !sealed(p, capBytes) {
			bySize = append(bySize, p)
		}
	}

	slices.SortFunc(bySize, func(a, b *part) int {
		if c := cmp.Compare(a.sizeBytes(), b.sizeBytes()); c != 0 {
			return c
		}

		return cmp.Compare(order[a], order[b])
	})

	return bySize, order
}

// scanRuns returns the cheapest qualifying and unguarded runs, plus the largest balance multiplier.
func scanRuns(bySize []*part, capBytes int64) (best, fallback []*part, bestMultiplier float64) {
	return scanRunsExpected(bySize, capBytes, nil)
}

func scanRunsExpected(bySize []*part, capBytes int64, expected func([]*part) int) (best, fallback []*part, bestMultiplier float64) {
	bestCost, fallbackCost := 0.0, 0.0
	for n := minTierParts; n <= min(maxTierParts, len(bySize)); n++ {
		for i := 0; i+n <= len(bySize); i++ {
			run := bySize[i : i+n]
			var total int64
			for _, p := range run {
				total += p.sizeBytes()
			}
			if capBytes > 0 && total > capBytes {
				break
			}
			outputs := 1
			if expected != nil {
				outputs = expected(run)
			}
			removed := n - outputs
			if removed <= 0 {
				continue
			}
			cost := float64(total) / float64(removed)
			m := float64(total) / float64(run[n-1].sizeBytes())
			bestMultiplier = max(bestMultiplier, m)
			if fallback == nil || cost < fallbackCost || (cost == fallbackCost && len(run) > len(fallback)) {
				fallbackCost, fallback = cost, run
			}
			if runQualifies(run, total, m) && (best == nil || cost < bestCost || (cost == bestCost && len(run) > len(best))) {
				bestCost, best = cost, run
			}
		}
	}
	return best, fallback, bestMultiplier
}

// runQualifies reports whether a run is worth merging on its own merits. Both tests argue about the
// *proportion* of bytes rewritten, which says nothing about a run whose whole rewrite is cheap —
// refusing those would leave a low-volume tenant's many tiny parts uncompacted forever.
func runQualifies(run []*part, total int64, m float64) bool {
	if total <= smallRunBytes {
		return true
	}

	// A run whose smallest member cannot carry its share of the largest is unbalanced: it rewrites
	// the large part to absorb parts that barely change its size.
	if run[0].sizeBytes()*int64(len(run)) < run[len(run)-1].sizeBytes() {
		return false
	}

	return m >= minMergeMultiplier
}

// inSrcOrder preserves source order for records sharing a timestamp.
func inSrcOrder(run []*part, order map[*part]int) []*part {
	if len(run) == 0 {
		return nil
	}

	out := slices.Clone(run)
	slices.SortFunc(out, func(a, b *part) int { return cmp.Compare(order[a], order[b]) })

	return out
}

// streamIDs adapts a part's stream ranges, which [buildRanges] leaves sorted by id, to
// [mergestream.Source].
type streamIDs []streamRange

func (s streamIDs) Len() int                 { return len(s) }
func (s streamIDs) At(i int) signal.SeriesID { return s[i].id }

// mergeKeys arms k over the union of every stream id across parts, so a compaction visits each
// stream once in (stream, ts) order without building a set of them.
func mergeKeys(parts []*part, k *mergestream.Keys) {
	src := make([]mergestream.Source, len(parts))
	for i, p := range parts {
		src[i] = streamIDs(p.ranges)
	}

	k.Reset(src)
}
