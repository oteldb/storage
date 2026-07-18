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
	// maxTierParts caps how many parts one merge compacts when no row budget applies (unlimited part
	// size); the rest are picked up next cycle.
	maxTierParts = 16
	// mergeHeight is how many flush-tier sizes a part may grow to through tiered merging before it is
	// sealed. A freshly-flushed part is at most MaxPartBytes; the merge combines same-tier parts into
	// larger ones, so a promoted part grows toward mergeHeight × MaxPartBytes, then is sealed. This
	// bounds part count to ≈ dataset / (mergeHeight × MaxPartBytes), and one merge's decoded input to
	// mergeHeight × MaxPartBytes regardless of how tall the tier being compacted is.
	mergeHeight = 8
	// tierFloorRows collapses every part below this row count into tier 0, so the many tiny parts of a
	// test or a low-volume engine always share a tier and compact together.
	tierFloorRows = 1 << 12

	// recordRowBytes is the assumed average uncompressed size of one record, used to convert the
	// byte-denominated MaxPartBytes into a row cap (records are variable-width, so — unlike the
	// metric engine's exact 32 B/row — this is a rough model; MaxPartBytes remains the tuning knob).
	// It is deliberately a modest estimate: too-large biases toward fewer, bigger parts (larger merge
	// working set), too-small toward many tiny parts (slower queries).
	recordRowBytes = 256
)

// maxRowsPerPart converts the byte cap MaxPartBytes into a row cap (0 ⇒ unlimited). At least one row,
// so a cap smaller than a single record still makes progress.
func maxRowsPerPart(maxBytes int64) int {
	if maxBytes <= 0 {
		return 0
	}

	if r := int(maxBytes / recordRowBytes); r >= 1 {
		return r
	}

	return 1
}

// mergeCapRows returns the row count at which a part is sealed (never re-compacted): mergeHeight × the
// flush-tier cap. 0 (never seal) when maxRows is 0 (unlimited part size), so unlimited parts merge into
// one — the legacy behavior.
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

	return bits.Len(uint(rows)) - bits.Len(uint(tierFloorRows))
}

// retentionForces reports whether retention must rewrite part p this merge (it holds a record old
// enough to drop). retainFrom ≤ 0 disables retention.
func retentionForces(p *part, retainFrom int64) bool {
	return retainFrom > 0 && p.minTime < retainFrom
}

// selectMergeParts chooses the source parts to compact this cycle (size-tiered compaction): the union
// of the parts retention must rewrite and the best same-tier group of unsealed parts. It returns nil
// when nothing is worth doing — fewer than minTierParts in every tier and no retention-forced part —
// so the merge is a no-op without decoding anything. capRows is the seal threshold in rows (0 ⇒
// unlimited, so no part is ever sealed and the whole set is one tier).
func selectMergeParts(src []*part, retainFrom int64, capRows int) []*part {
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

	for _, p := range pickTierGroup(src, capRows) {
		add(p)
	}

	return selected
}

// pickTierGroup returns the group of unsealed parts to compact for size reduction: the tier holding the
// most parts (ties broken toward the smaller tier, to drain small parts first), once it holds at least
// minTierParts. The group is capped by cumulative rows at the seal threshold (so one merge's decoded
// input is at most one sealed-tier part's worth), or by maxTierParts when part size is unlimited.
// Returns nil when no tier qualifies. Parts keep their src (sequence) order within the group.
func pickTierGroup(src []*part, capRows int) []*part {
	sealed := func(p *part) bool { return capRows > 0 && int(p.rows()) >= capRows }

	byTier := make(map[int][]*part)
	for _, p := range src {
		if !sealed(p) {
			t := sizeTier(int(p.rows()))
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
		// Cap the group's cumulative rows at the seal threshold, taking at least minTierParts so a
		// merge always makes progress even when two parts already approach the cap.
		total := 0

		for i, p := range group {
			total += int(p.rows())
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
