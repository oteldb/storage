package recordengine

// MergeShape describes compaction eligibility without reading or decoding backend objects.
type MergeShape struct {
	// Parts is the flushed parts; Sealed those excluded from size compaction at the cap;
	// Backlog the rest. Retention and compression rewrites may also take sealed parts.
	Parts   int
	Sealed  int
	Backlog int
	// Bytes is what the flushed parts occupy on disk. Divided by Parts it is the average part size,
	// which is what says whether a rising part count is a merge that stopped or an ingest that grew.
	Bytes int64
	// Candidates counts the next merge's inputs, including retention drops and compression rewrites.
	// ForceCandidates applies the idle waiver immediately; the time ladder still bounds both.
	Candidates      int
	ForceCandidates int
	// CapBytes is the stored-byte seal threshold of the last merge (0 before the first merge).
	CapBytes int64
	// Tiers and LargestTierParts describe stored-byte sizes; they do not govern eligibility.
	Tiers            int
	LargestTierParts int
	// MinTierParts is the minimum ordinary merge input count (retained for compatibility).
	MinTierParts int
	// BestMultiplier is the best output-to-largest-input ratio; IdleRounds reaches WaiveAfter
	// before selection waives the write-amplification guard. Tiers are diagnostic only.
	BestMultiplier float64
	IdleRounds     int
	WaiveAfter     int
	// NonReducingMerges counts rejected ordinary rewrites, not durable part changes.
	NonReducingMerges uint64
}

// MergeShape is [Engine.MergeShapeWith] without retention.
func (e *Engine) MergeShape() MergeShape { return e.MergeShapeWith(MergeOptions{}) }

// MergeShapeWith returns the selector's view of the engine's parts under the retention a merge
// would run with (opts.Force is ignored; ForceCandidates sets it). It takes a brief read lock, does
// no backend I/O and decodes nothing, so it is safe to poll at dashboard cadence.
func (e *Engine) MergeShapeWith(opts MergeOptions) MergeShape {
	e.mu.RLock()
	src := e.parts
	e.mu.RUnlock()

	capBytes := e.lastMergeCap.Load()
	if e.mergeCap > 0 {
		capBytes = e.mergeCap
	}
	return e.shapeOf(src, opts.RetainFrom, capBytes)
}

// shapeOf summarizes what the selector sees in src at the given retention cutoff and seal threshold.
func (e *Engine) shapeOf(src []*part, retainFrom, capBytes int64) MergeShape {
	unsealed := unsealedOf(src, capBytes)
	bySize, _ := eligibleBySize(src, capBytes)
	_, _, best := scanRuns(bySize, capBytes)

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
		Candidates:       e.nextMergeParts(src, retainFrom, capBytes, false),
		ForceCandidates:  e.nextMergeParts(src, retainFrom, capBytes, true),
		CapBytes:         capBytes,
		Tiers:            len(byTier),
		LargestTierParts: largest,
		MinTierParts:     minTierParts,
		BestMultiplier:   best, IdleRounds: int(e.idleMerges.Load()), WaiveAfter: mergeIdleRounds,
		NonReducingMerges: e.nonReducingMerges.Load(),
	}
}

// nextMergeParts counts the parts one merge over src takes: those retention drops whole, then the
// selection over the rest.
func (e *Engine) nextMergeParts(src []*part, retainFrom, capBytes int64, force bool) int {
	live := src

	if retainFrom > 0 {
		live = make([]*part, 0, len(src))

		for _, p := range src {
			if p.maxTime >= retainFrom {
				live = append(live, p)
			}
		}
	}

	return len(src) - len(live) + len(e.selectMergeParts(live, retainFrom, capBytes, force))
}
