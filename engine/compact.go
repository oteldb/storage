package engine

import (
	"cmp"
	"context"
	"slices"

	"github.com/oteldb/storage/signal"
)

// sortedSeriesIDs returns the union of every series id across src, sorted, so a compaction visits
// each series once in (series, ts) part order.
func sortedSeriesIDs(ctx context.Context, src []*part) ([]signal.SeriesID, error) {
	idSet := make(map[signal.SeriesID]struct{})

	for _, p := range src {
		if err := p.index.forEachID(ctx, func(id signal.SeriesID) { idSet[id] = struct{}{} }); err != nil {
			return nil, err
		}
	}

	ids := make([]signal.SeriesID, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}

	slices.SortFunc(ids, func(a, b signal.SeriesID) int { return a.Compare(b) })

	return ids, nil
}

// Size-tiered compaction (DESIGN.md §4). The engine does not re-merge its whole part set on every
// maintenance tick — that re-reads, re-materializes, and re-encodes the entire (growing) dataset
// each cycle, so a single merge's working set and write amplification grow with the dataset (it is
// what made the object-store backend pin multi-GB of churned garbage). Instead a merge selects a
// bounded run of similarly-sized parts and compacts only those:
//
//   - A part at the merge cap is "sealed": re-merging it only re-splits it into the same number of
//     equally-full parts, pure churn. Parts below the cap roll up through progressively larger
//     merges, so part count is bounded at ≈ dataset / cap instead of growing with every flush
//     (issue #25 root cause A).
//   - Among the unsealed parts, [pickMergeRun] scores runs of similarly-sized parts and takes the
//     one that reduces part count for the least rewriting.
//   - A part that retention, downsampling, recompression, or precision coarsening must rewrite is
//     selected regardless (forced), so age-driven work is never starved by sealing.
const (
	// minMergeParts is the smallest run worth merging. Two keeps part count low under continuous
	// flushing without over-eager single-part merges.
	minMergeParts = 2
	// maxMergeParts keeps one merge bounded in inputs (and wall-clock) however large the byte budget
	// grows; the rest are picked up next cycle. VictoriaMetrics bounds by parts as well as bytes too.
	maxMergeParts = 16
	// minMergeMultiplier is the least output-to-largest-input ratio a run must reach to be worth
	// merging, the write-amplification guard: folding a 1 MiB part into a 1 GiB one rewrites a
	// gigabyte to remove one part. VictoriaMetrics uses the same constant and name
	// (lib/storage/partition.go).
	minMergeMultiplier = 1.7
	// smallRunBytes is the total size below which the ratio guard is skipped. The guard reasons
	// about the *proportion* of bytes rewritten, which says nothing about a run this small — the
	// whole rewrite is cheap in absolute terms, and refusing it would leave a low-volume tenant's
	// many tiny parts uncompacted, which is what the guard is meant to prevent.
	smallRunBytes = 128 << 10
	// mergeIdleRounds is how many consecutive no-op merges must pass before the guard above is
	// waived. See [pickMergeRun] for why waiving it is required, not merely nice.
	mergeIdleRounds = 3
)

// sealed reports whether p is too large to be a useful merge input. The bound is the cap divided by
// [minMergeMultiplier] rather than the cap itself: a part just under the cap can only merge into
// something over it, which would be sealed immediately — maximum rewriting for no lasting gain.
func sealed(p *part, capBytes int64) bool {
	return capBytes > 0 && float64(p.sizeBytes()) >= float64(capBytes)/minMergeMultiplier
}

// forcedRewrite reports whether a part must be rewritten this merge regardless of its size: it holds
// data old enough for retention to drop, downsampling to roll up, recompression to recompress, or
// precision coarsening to re-encode. The recompress/precision/downsample tests are fixed points (a
// part already at its target is not forced), so a lone cold part does not churn the backend.
func forcedRewrite(p *part, opts MergeOptions) bool {
	if opts.RetainFrom > 0 && p.minTime < opts.RetainFrom {
		return true
	}

	return downsampleApplies(opts.Downsample, p.minTime) ||
		recompressApplies(p, opts.Recompress) ||
		precisionApplies(p, opts.Precision)
}

// selectMergeParts chooses the source parts to compact this cycle. Both paths — the forced rewrite
// (retention/downsample/recompress/precision) and the size-tiered run — are confined to one aligned
// time bucket, so a merge can never emit a part wider than a ladder level and time locality
// survives compaction (see timebucket.go). It returns nil when nothing is worth doing, so the merge
// is a no-op without decoding anything. capBytes is the seal threshold in bytes on disk
// (0 ⇒ unlimited, nothing is ever sealed); idle is the number of consecutive no-op merges that
// preceded this one.
//
// Forced work wins the cycle outright rather than being unioned with a size-tiered run, because the
// two are now in general in different buckets and merging across them is the widening this exists to
// prevent. The size-tiered run is picked up on the next cycle.
func selectMergeParts(src []*part, opts MergeOptions, capBytes int64, idle int) []*part {
	if forced := selectForced(src, opts, capBytes); len(forced) > 0 {
		return forced
	}

	return selectLadderRun(src, capBytes, idle)
}

// pickMergeRun returns the unsealed parts to compact for size reduction, or nil when none are worth
// it. Parts are ordered by size and every run of [minMergeParts, maxMergeParts] adjacent parts is
// scored by m = output size / largest input — the inverse of write amplification, so the winning run
// removes the most parts for the least rewriting. Runs over capBytes are rejected, as are runs whose
// smallest member cannot carry its share of the largest (VictoriaMetrics' appendPartsToMerge).
//
// Ordering by size rather than bucketing by it is what fixes the stranding: a part alone in its size
// class used to be unmergeable, because the selector required two parts inside one power-of-two
// tier and nothing would ever land in exactly that tier again (#285).
//
// That alone is not enough, which is why idle exists. Leftovers are spread geometrically — one per
// former tier — so every run over them either fails the spread test or scores below
// minMergeMultiplier, and a purely score-driven selector strands them just as permanently. After
// mergeIdleRounds cycles with nothing else to do, the best run is taken regardless of its score:
// rewriting a large part once to absorb a stray costs far less than carrying that stray in every
// query for the lifetime of the engine.
func pickMergeRun(src []*part, capBytes int64, idle int) []*part {
	bySize, order := eligibleBySize(src, capBytes)
	if len(bySize) < minMergeParts {
		return nil
	}

	best, fallback, _ := scanRuns(bySize, capBytes)
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

// scanRuns scores every run of [minMergeParts, maxMergeParts] adjacent parts that fits the cap and
// returns the best qualifying run, the best run ignoring the guards (what the idle escape falls
// back on), and that run's score.
func scanRuns(bySize []*part, capBytes int64) (best, fallback []*part, fallbackM float64) {
	bestM := 0.0

	for n := minMergeParts; n <= min(maxMergeParts, len(bySize)); n++ {
		for i := 0; i+n <= len(bySize); i++ {
			run := bySize[i : i+n]

			var total int64
			for _, p := range run {
				total += p.sizeBytes()
			}

			if capBytes > 0 && total > capBytes {
				break // runs further along this row are only larger
			}

			m := float64(total) / float64(run[n-1].sizeBytes())
			if m > fallbackM {
				fallbackM, fallback = m, run
			}

			if m > bestM && runQualifies(run, total, m) {
				bestM, best = m, run
			}
		}
	}

	return best, fallback, fallbackM
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

// inSrcOrder restores a run to the caller's part order. The merge visits its sources oldest → newest
// so a later part's value wins a duplicate timestamp, so the size ordering used to choose the run
// must not leak into the result.
func inSrcOrder(run []*part, order map[*part]int) []*part {
	if len(run) == 0 {
		return nil
	}

	out := slices.Clone(run)
	slices.SortFunc(out, func(a, b *part) int { return cmp.Compare(order[a], order[b]) })

	return out
}

// mergeShape summarizes why a merge selected nothing: how many parts are sealed, how many remain
// eligible, and the best score any run of them reached — the number [runQualifies] compares against
// minMergeMultiplier.
func mergeShape(src []*part, capBytes int64) (sealedN, eligible int, bestM float64) {
	bySize, _ := eligibleBySize(src, capBytes)

	_, _, bestM = scanRuns(bySize, capBytes)

	return len(src) - len(bySize), len(bySize), bestM
}
