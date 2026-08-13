package engine

import (
	"context"
	"math/bits"
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
// bounded group of similarly-sized parts and compacts only those, so its working set is O(part
// size), not O(dataset):
//
//   - A part that has reached the merge cap is "sealed": re-merging sealed parts only re-splits
//     them into the same number of equally-full parts, which is pure churn — so they are never
//     compacted again. Parts below the cap roll up through progressively taller size tiers (each
//     merge of same-tier siblings produces a larger part), so part count is bounded at
//     ≈ dataset / cap instead of growing with every flush (issue #25 root cause A).
//   - Among the unsealed parts, those of similar size share a tier; a tier is compacted once it
//     holds at least minTierParts of them, so small freshly-flushed parts merge up into larger ones
//     without re-reading the already-compacted large parts.
//   - A part that retention, downsampling, recompression, or precision coarsening must rewrite is
//     selected regardless of its tier (forced), so that age-driven work is never starved by sealing.
const (
	// minTierParts is the number of same-tier parts that must accumulate before they are compacted.
	// Two keeps the part count low under continuous flushing without over-eager single-part merges.
	minTierParts = 2
	// maxTierParts keeps one merge bounded in inputs (and wall-clock) however large the byte budget
	// grows; the rest are picked up next cycle. VictoriaMetrics bounds by parts as well as bytes too.
	maxTierParts = 16
	// tierFloorBytes collapses every part below this size into tier 0, so the many tiny parts of a
	// low-volume tenant share a tier and compact together.
	tierFloorBytes = 128 << 10
)

// sizeTier buckets a part by size on disk into a tier, so two parts within 2× of each other (above
// the floor) share a tier. Parts at or below tierFloorBytes are all tier 0.
func sizeTier(size int64) int {
	if size <= tierFloorBytes {
		return 0
	}

	// bits.Len(size) − bits.Len(floor) is ⌊log2(size)⌋ − ⌊log2(floor)⌋: how many size-doublings above
	// the floor, i.e. the power-of-two tier.
	return bits.Len64(uint64(size)) - bits.Len64(uint64(tierFloorBytes))
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

// selectMergeParts chooses the source parts to compact this cycle (size-tiered compaction): the
// union of the parts a forced rewrite (retention/downsample/recompress/precision) must touch and the
// best same-tier group of unsealed parts. It returns nil when nothing is worth doing — fewer than
// minTierParts in every tier and no forced part — so the merge is a no-op without decoding anything.
// capBytes is the seal threshold in bytes on disk (0 ⇒ unlimited, so no part is ever sealed).
func selectMergeParts(src []*part, opts MergeOptions, capBytes int64) []*part {
	var (
		selected []*part
		chosen   = make(map[*part]struct{}, len(src))
	)

	add := func(p *part) {
		if _, ok := chosen[p]; !ok {
			chosen[p] = struct{}{}
			selected = append(selected, p)
		}
	}

	for _, p := range src {
		if forcedRewrite(p, opts) {
			add(p)
		}
	}

	for _, p := range pickTierGroup(src, capBytes) {
		add(p)
	}

	return selected
}

// pickTierGroup returns the group of unsealed parts to compact for size reduction: the tier holding
// the most parts (ties broken toward the smaller tier, to drain small parts first), once it holds at
// least minTierParts. The group is bounded both by cumulative bytes at the seal threshold (so one
// merge produces at most one full-size part) and by maxTierParts. Returns nil when no tier
// qualifies. Parts keep their src (sequence) order within the group.
func pickTierGroup(src []*part, capBytes int64) []*part {
	sealed := func(p *part) bool { return capBytes > 0 && p.sizeBytes() >= capBytes }

	byTier := make(map[int][]*part)
	for _, p := range src {
		if !sealed(p) {
			t := sizeTier(p.sizeBytes())
			byTier[t] = append(byTier[t], p)
		}
	}

	bestTier, bestN := -1, 0
	for t, ps := range byTier {
		if len(ps) > bestN || (len(ps) == bestN && (bestTier < 0 || t < bestTier)) {
			bestTier, bestN = t, len(ps)
		}
	}

	if bestN < minTierParts {
		return nil
	}

	group := byTier[bestTier]

	if capBytes > 0 {
		// One merge produces at most one full-size part, but always takes minTierParts so it makes
		// progress even when two parts already approach the cap.
		var total int64

		for i, p := range group {
			total += p.sizeBytes()
			if i+1 >= minTierParts && total >= capBytes {
				return group[:i+1]
			}
		}
	}

	if len(group) > maxTierParts {
		return group[:maxTierParts]
	}

	return group
}

// tierShape summarizes why a merge selected nothing: how many parts are sealed, how many distinct
// size tiers the unsealed ones occupy, and the largest tier's part count — which is the number
// [pickTierGroup] compares against minTierParts.
func tierShape(src []*part, capBytes int64) (sealed, tiers, largestTier int) {
	byTier := make(map[int]int, len(src))

	for _, p := range src {
		if capBytes > 0 && p.sizeBytes() >= capBytes {
			sealed++

			continue
		}

		byTier[sizeTier(p.sizeBytes())]++
	}

	for _, n := range byTier {
		if n > largestTier {
			largestTier = n
		}
	}

	return sealed, len(byTier), largestTier
}
