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
//   - A part that has reached the merge cap (mergeHeight × MaxPartBytes) is "sealed": re-merging
//     sealed parts only re-splits them into the same number of equally-full parts, which is pure
//     churn — so they are never compacted again. Parts below the cap roll up through progressively
//     taller size tiers (each merge of same-tier siblings produces a larger part), so part count is
//     bounded at ≈ dataset / (mergeHeight × MaxPartBytes) instead of growing with every flush
//     (issue #25 root cause A).
//   - Among the unsealed parts, those of similar size share a tier; a tier is compacted once it
//     holds at least minTierParts of them, so small freshly-flushed parts merge up into larger ones
//     without re-reading the already-compacted large parts.
//   - A part that retention, downsampling, recompression, or precision coarsening must rewrite is
//     selected regardless of its tier (forced), so that age-driven work is never starved by sealing.
const (
	// minTierParts is the number of same-tier parts that must accumulate before they are compacted.
	// Two keeps the part count low under continuous flushing without over-eager single-part merges.
	minTierParts = 2
	// maxTierParts caps how many parts one merge compacts when a row budget does not apply
	// (unlimited part size); the rest are picked up on the next cycle.
	maxTierParts = 16
	// mergeHeight is how many flush-tier sizes a part may grow to through tiered merging before it is
	// sealed. A freshly-flushed part is at most MaxPartBytes; the background merge combines
	// same-tier parts into larger ones, so a promoted part grows toward mergeHeight × MaxPartBytes.
	// Once it reaches that size it is sealed (re-merging it would only re-split it — pure churn),
	// bounding part count to ≈ dataset / (mergeHeight × MaxPartBytes) instead of dataset /
	// MaxPartBytes. The decoded input of one merge is capped at the same mergeHeight × MaxPartBytes,
	// so the background merge's working set stays bounded regardless of how tall the tier being
	// compacted is.
	mergeHeight = 8
	// tierFloorRows collapses every part below this row count into tier 0, so the many tiny parts of
	// a test or a low-volume tenant always share a tier and compact together (the power-of-two
	// bucketing below only differentiates parts large enough for their sizes to matter).
	tierFloorRows = 1 << 12
)

// mergeCapRows returns the row count at which a part is sealed (never re-compacted): mergeHeight ×
// the flush-tier cap. 0 (never seal) when the flush cap is 0 (unlimited part size), so unlimited
// parts always merge into one.
func mergeCapRows(maxRows int) int {
	if maxRows <= 0 {
		return 0
	}

	return maxRows * mergeHeight
}

// sizeTier buckets a part by row count into a tier, so two parts within 2× of each other (above the
// floor) share a tier. Parts at or below tierFloorRows are all tier 0.
func sizeTier(rows int) int {
	if rows <= tierFloorRows {
		return 0
	}

	// bits.Len(rows) − bits.Len(floor) is ⌊log2(rows)⌋ − ⌊log2(floor)⌋: how many size-doublings above
	// the floor, i.e. the power-of-two tier.
	return bits.Len(uint(rows)) - bits.Len(uint(tierFloorRows))
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
// capRows is the seal threshold in rows (0 ⇒ unlimited, so no part is ever sealed).
func selectMergeParts(src []*part, opts MergeOptions, capRows int) []*part {
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

	for _, p := range pickTierGroup(src, capRows) {
		add(p)
	}

	return selected
}

// pickTierGroup returns the group of unsealed parts to compact for size reduction: the tier holding
// the most parts (ties broken toward the smaller tier, to drain small parts first), once it holds at
// least minTierParts. The group is capped by cumulative rows at the seal threshold (so one merge's
// decoded input is at most one sealed-tier part's worth), or by maxTierParts when part size is
// unlimited. Returns nil when no tier qualifies. Parts keep their src (sequence) order within the
// group.
func pickTierGroup(src []*part, capRows int) []*part {
	sealed := func(p *part) bool { return capRows > 0 && p.rows() >= capRows }

	byTier := make(map[int][]*part)
	for _, p := range src {
		if !sealed(p) {
			t := sizeTier(p.rows())
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

	if capRows > 0 {
		// Cap the group's cumulative rows at the seal threshold, so the decoded input of one merge is
		// at most one sealed-tier part's worth — taking at least minTierParts so a merge always makes
		// progress even when two parts already approach the cap.
		total := 0

		for i, p := range group {
			total += p.rows()
			if i+1 >= minTierParts && total >= capRows {
				return group[:i+1]
			}
		}

		return group
	}

	if len(group) > maxTierParts {
		return group[:maxTierParts]
	}

	return group
}
