package recordengine

// MergeShape is the merge selector's view of the flushed parts: the inputs to the decision the
// background merge makes each cycle. Without them an engine sitting on a part count it will never
// reduce is indistinguishable from an idle healthy one — the two differ only in whether the parts
// are sealed and whether one tier has accumulated enough of the rest.
type MergeShape struct {
	// Parts is the flushed parts; Sealed those already at the cap, which no merge reconsiders;
	// Backlog the rest — the parts a merge may still take.
	Parts   int
	Sealed  int
	Backlog int
	// Bytes is what the flushed parts occupy on disk. Divided by Parts it is the average part size,
	// which is what says whether a rising part count is a merge that stopped or an ingest that grew.
	Bytes int64
	// Candidates is how many parts the next merge would select right now. 0 with a non-zero Backlog
	// is the stuck state: parts remain mergeable but no tier of any time bucket holds minTierParts
	// of them, which is what [MergeOptions.Force] exists to break.
	Candidates int
	// CapBytes is the seal threshold in effect, in decoded bytes (0 ⇒ sealing disabled).
	CapBytes int64
	// Tiers is how many distinct size tiers the unsealed parts fall into and LargestTierParts the
	// count in the fullest of them — a LargestTierParts below minTierParts is why nothing merges.
	// Tiers span the whole engine here, not one time bucket, so LargestTierParts is an upper bound
	// on what the ladder can select.
	Tiers            int
	LargestTierParts int
	// MinTierParts is the same-tier count a merge waits for.
	MinTierParts int
}

// MergeShape returns the selector's view of the engine's parts. It takes a brief read lock, does no
// backend I/O and decodes nothing, so it is safe to poll at dashboard cadence.
func (e *Engine) MergeShape() MergeShape {
	e.mu.RLock()
	src := e.parts
	e.mu.RUnlock()

	return shapeOf(src, e.mergeCapBytes())
}

// shapeOf summarizes what the selector sees in src at the given seal threshold.
func shapeOf(src []*part, capBytes int64) MergeShape {
	unsealed := unsealedOf(src, capBytes)

	var bytes int64
	for _, p := range src {
		bytes += p.sizeBytes()
	}

	byTier := make(map[int]int, len(unsealed))
	for _, p := range unsealed {
		byTier[sizeTier(p.sizeBytes())]++
	}

	largest := 0
	for _, n := range byTier {
		largest = max(largest, n)
	}

	return MergeShape{
		Parts:            len(src),
		Sealed:           len(src) - len(unsealed),
		Backlog:          len(unsealed),
		Bytes:            bytes,
		Candidates:       len(selectLadderGroup(src, capBytes, false)),
		CapBytes:         capBytes,
		Tiers:            len(byTier),
		LargestTierParts: largest,
		MinTierParts:     minTierParts,
	}
}
