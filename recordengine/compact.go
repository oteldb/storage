package recordengine

import (
	"cmp"
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

// selectMergeParts chooses the source parts to compact this cycle. Both paths — the retention-forced
// rewrite and the size-tiered group — are confined to one aligned time bucket, so a merge can never
// emit a part wider than a ladder level and time locality survives compaction (see timebucket.go).
// It returns nil when nothing is worth doing — fewer than minTierParts in every tier of every bucket
// and no retention-forced part — so the merge is a no-op without decoding anything. capBytes is the
// seal threshold in decoded bytes (0 ⇒ unlimited, so no part is ever sealed and a bucket is one
// tier).
//
// Forced work wins the cycle outright rather than being unioned with a tier group, because the two
// are now in general in different buckets and merging across them is the widening this prevents. The
// tier group is picked up on the next cycle.
// force takes a bucket's unsealed parts whatever their tiers, so an operator can compact a part set
// the tier rule declines to touch; the seal threshold and the cumulative-bytes cap still bound the
// merge, and the ladder still confines it to one time bucket.
func selectMergeParts(src []*part, retainFrom, capBytes int64, force bool) []*part {
	if forced := selectForced(src, retainFrom, capBytes); len(forced) > 0 {
		return forced
	}

	return selectLadderGroup(src, capBytes, force)
}

// pickTierGroup returns the group of unsealed parts to compact for size reduction: the tier holding the
// most parts (ties broken toward the smaller tier, to drain small parts first), once it holds at least
// minTierParts. The group is capped by cumulative decoded bytes at the seal threshold (so one merge's
// decoded input is at most one sealed-tier part's worth), or by maxTierParts when part size is
// unlimited. Returns nil when no tier qualifies. Parts keep their src (sequence) order within the group.
func pickTierGroup(src []*part, capBytes int64) []*part {
	byTier := make(map[int][]*part)
	for _, p := range unsealedOf(src, capBytes) {
		t := sizeTier(p.sizeBytes())
		byTier[t] = append(byTier[t], p)
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

	return capGroup(byTier[bestTier], capBytes)
}

// pickForcedGroup returns the unsealed parts to compact when the tier rule has selected nothing and
// the caller wants a merge anyway: the smallest ones first (they cost the least to rewrite and
// reduce the part count the most per byte), taken across tiers up to the same cumulative-bytes cap
// the tiered path obeys, then restored to src order — the merge visits its sources in sequence
// order. nil when fewer than minTierParts parts remain unsealed, where no merge can help.
func pickForcedGroup(src []*part, capBytes int64) []*part {
	eligible := unsealedOf(src, capBytes)
	if len(eligible) < minTierParts {
		return nil
	}

	order := make(map[*part]int, len(eligible))
	for i, p := range eligible {
		order[p] = i
	}

	bySize := slices.Clone(eligible)
	slices.SortFunc(bySize, func(a, b *part) int {
		if c := cmp.Compare(a.sizeBytes(), b.sizeBytes()); c != 0 {
			return c
		}

		return cmp.Compare(order[a], order[b])
	})

	group := slices.Clone(capGroup(bySize, capBytes))
	slices.SortFunc(group, func(a, b *part) int { return cmp.Compare(order[a], order[b]) })

	return group
}

// unsealedOf returns the parts below the seal threshold, in src order. A sealed part is at the cap
// already: re-merging it would only re-split it into equally-full parts.
func unsealedOf(src []*part, capBytes int64) []*part {
	out := make([]*part, 0, len(src))

	for _, p := range src {
		if capBytes <= 0 || p.sizeBytes() < capBytes {
			out = append(out, p)
		}
	}

	return out
}

// capGroup truncates a group at what one merge may hold: its cumulative decoded bytes at the seal
// threshold (taking at least minTierParts, so a merge always makes progress even when two parts
// already approach the cap), or maxTierParts when part size is unlimited.
func capGroup(group []*part, capBytes int64) []*part {
	if capBytes <= 0 {
		return group[:min(len(group), maxTierParts)]
	}

	var total int64

	for i, p := range group {
		total += p.sizeBytes()
		if i+1 >= minTierParts && total >= capBytes {
			return group[:i+1]
		}
	}

	return group
}

// idSetOf returns the sorted union of every stream id across parts, so a compaction visits each stream
// once in (stream, ts) order.
func idSetOf(parts []*part) []signal.SeriesID {
	set := make(map[signal.SeriesID]struct{})
	for _, p := range parts {
		for _, sr := range p.ranges {
			set[sr.id] = struct{}{}
		}
	}

	ids := make([]signal.SeriesID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}

	slices.SortFunc(ids, func(a, b signal.SeriesID) int { return a.Compare(b) })

	return ids
}
