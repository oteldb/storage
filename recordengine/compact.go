package recordengine

import (
	"math/bits"
	"slices"

	"github.com/oteldb/storage/signal"
)

// Size-tiered compaction for the record engine (logs/traces/profiles), mirroring the metric engine
// (engine/compact.go, DESIGN.md §4). The engine must not re-merge its whole part set on every
// maintenance tick — that re-reads, re-materializes, and re-encodes the entire (growing) dataset each
// cycle, so a single merge's working set and write amplification grow with the dataset (O(dataset) per
// merge, O(dataset²) total; the OOM seen bulk-loading real data). Instead a merge selects a bounded
// group of similarly-sized parts and compacts only those, so its working set is O(part size):
//
//   - A part that has reached the merge cap (mergeHeight × MaxPartBytes) is "sealed": re-merging it
//     would only re-split it into the same number of equally-full parts (pure churn), so it is never
//     compacted again. Parts below the cap roll up through progressively taller size tiers, so part
//     count is bounded at ≈ dataset / (mergeHeight × MaxPartBytes) instead of growing with every flush.
//   - Among the unsealed parts, those of similar size share a tier; a tier is compacted once it holds
//     at least minTierParts of them, so small freshly-flushed parts merge up into larger ones without
//     re-reading the already-compacted large parts.
//   - A part old enough for retention to drop records from is selected regardless of its tier
//     (forced), so age-driven work is never starved by sealing. (The record signals have no
//     downsampling/recompression — retention is the only forced rewrite, unlike the metric engine.)
const (
	// minTierParts is the number of same-tier parts that must accumulate before they are compacted.
	minTierParts = 2
	// maxTierParts caps how many parts one merge compacts when no byte budget applies (unlimited part
	// size); the rest are picked up next cycle.
	maxTierParts = 16
	// mergeHeight is how many flush-tier sizes a part may grow to through tiered merging before it is
	// sealed. A freshly-flushed part is at most MaxPartBytes; the merge combines same-tier parts into
	// larger ones, so a promoted part grows toward mergeHeight × MaxPartBytes, then is sealed. This
	// bounds part count to ≈ dataset / (mergeHeight × MaxPartBytes), and one merge's decoded input to
	// mergeHeight × MaxPartBytes regardless of how tall the tier being compacted is.
	mergeHeight = 8
	// tierFloorBytes collapses every part below this size into tier 0, so the many tiny parts of a
	// test or a low-volume engine always share a tier and compact together.
	tierFloorBytes = 4 << 20

	// mergeBufferAmplification converts the merge's memory allowance into a cap on the bytes it may
	// hold. A merge holds its selected sources decoded *and* the output buffer it is filling from
	// them, and the write then encodes that buffer, so the peak is about three times the cap.
	mergeBufferAmplification = 3

	// recordRowBytes is the assumed average uncompressed size of one record. It survives only for a
	// part written before the manifest recorded its decoded size ([part.sizeBytes]); every other
	// path measures. Calibrated to a realistic structured-log row (body + attributes + resource): a
	// real homelab logs table averaged ~950 B/row.
	recordRowBytes = 1024
)

// sizeTier buckets a part by its decoded size into a tier, so two parts within 2× of each other
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

// selectMergeParts chooses the source parts to compact this cycle (size-tiered compaction): the union
// of the parts retention must rewrite and the best same-tier group of unsealed parts. It returns nil
// when nothing is worth doing — fewer than minTierParts in every tier and no retention-forced part —
// so the merge is a no-op without decoding anything. capBytes is the seal threshold in decoded bytes
// (0 ⇒ unlimited, so no part is ever sealed and the whole set is one tier).
func selectMergeParts(src []*part, retainFrom, capBytes int64) []*part {
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
		if retentionForces(p, retainFrom) {
			add(p)
		}
	}

	for _, p := range pickTierGroup(src, capBytes) {
		add(p)
	}

	return selected
}

// pickTierGroup returns the group of unsealed parts to compact for size reduction: the tier holding the
// most parts (ties broken toward the smaller tier, to drain small parts first), once it holds at least
// minTierParts. The group is capped by cumulative decoded bytes at the seal threshold (so one merge's
// decoded input is at most one sealed-tier part's worth), or by maxTierParts when part size is
// unlimited. Returns nil when no tier qualifies. Parts keep their src (sequence) order within the group.
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
		// Cap the group's cumulative decoded bytes at the seal threshold, taking at least minTierParts
		// so a merge always makes progress even when two parts already approach the cap.
		var total int64

		for i, p := range group {
			total += p.sizeBytes()
			if i+1 >= minTierParts && total >= capBytes {
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

// idSetOf returns the sorted union of every stream id across parts, so a compaction visits each stream
// once in (stream, ts) order.
func idSetOf(parts []*part) []signal.SeriesID {
	set := make(map[signal.SeriesID]struct{})
	for _, p := range parts {
		for id := range p.ranges {
			set[id] = struct{}{}
		}
	}

	ids := make([]signal.SeriesID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}

	slices.SortFunc(ids, func(a, b signal.SeriesID) int { return a.Compare(b) })

	return ids
}
