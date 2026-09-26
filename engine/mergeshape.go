package engine

// MergeShape is the merge selector's view of the flushed parts: the inputs to the decision the
// background merge makes each cycle. Without them an engine sitting on a part count it will never
// reduce is indistinguishable from an idle healthy one — the two differ only in whether the parts
// are sealed and whether any run of the rest is worth rewriting.
type MergeShape struct {
	// Parts is the flushed parts; Sealed those too large to be a useful merge input, which no merge
	// will reconsider; Backlog the rest — the parts a merge may still take.
	Parts   int
	Sealed  int
	Backlog int
	// Bytes is what the flushed parts occupy on disk. Divided by Parts it is the average part size,
	// which is what says whether a rising part count is a merge that stopped or an ingest that grew.
	Bytes int64
	// Candidates is how many parts the next merge under the given policy would take right now —
	// retention's whole-part drops included — and ForceCandidates how many a [MergeOptions.Force]
	// merge would. Candidates 0 with ForceCandidates above it is a run the score guard declines
	// until the idle waiver; both 0 with a non-zero Backlog is every unsealed part alone in its time
	// bucket, the ladder's resting state, which no merge reduces without widening a part.
	Candidates      int
	ForceCandidates int
	// CapBytes is the seal threshold in effect, in bytes on disk. It is derived per merge from free
	// space and the merge memory allowance, so it is reported as of the last merge — 0 before the
	// first one, and 0 when sealing is disabled.
	CapBytes int64
	// BestMultiplier is the best output-to-largest-input ratio any eligible run reaches;
	// MinMultiplier the ratio a run must reach to be selected on its own merits.
	BestMultiplier float64
	MinMultiplier  float64
	// IdleRounds is the consecutive merges that selected nothing; after WaiveAfter of them the
	// selector takes its best run regardless of the ratio.
	IdleRounds int
	WaiveAfter int
}

// MergeShape is [Engine.MergeShapeWith] under no policy: no retention, downsampling,
// recompression or precision.
func (e *Engine) MergeShape() MergeShape { return e.MergeShapeWith(MergeOptions{}) }

// MergeShapeWith returns the selector's view of the engine's parts under the policy a merge would
// run with (opts.Force is ignored; ForceCandidates sets it). It takes a brief read lock, does no
// backend I/O and decodes nothing, so it is safe to poll at dashboard cadence.
func (e *Engine) MergeShapeWith(opts MergeOptions) MergeShape {
	e.mu.RLock()
	src := e.parts
	e.mu.RUnlock()

	capBytes := e.lastMergeCap.Load()
	idle := int(e.idleMerges.Load())
	sealedN, backlog, bestM := mergeShape(src, capBytes)

	var bytes int64
	for _, p := range src {
		bytes += p.sizeBytes()
	}

	return MergeShape{
		Parts:           len(src),
		Bytes:           bytes,
		Sealed:          sealedN,
		Backlog:         backlog,
		Candidates:      nextMergeParts(src, opts, capBytes, idle),
		ForceCandidates: nextMergeParts(src, opts, capBytes, mergeIdleRounds),
		CapBytes:        capBytes,
		BestMultiplier:  bestM,
		MinMultiplier:   minMergeMultiplier,
		IdleRounds:      idle,
		WaiveAfter:      mergeIdleRounds,
	}
}

// nextMergeParts counts the parts one merge over src takes: those retention drops whole, then the
// selection over the rest.
func nextMergeParts(src []*part, opts MergeOptions, capBytes int64, idle int) int {
	live := src

	if opts.RetainFrom > 0 {
		live = make([]*part, 0, len(src))

		for _, p := range src {
			if p.maxTime >= opts.RetainFrom {
				live = append(live, p)
			}
		}
	}

	return len(src) - len(live) + len(selectMergeParts(live, opts, capBytes, idle))
}

// mergeIdle is the idle-round count the selector sees for this merge: the real one, or one that has
// already reached the waiver when the caller forces the merge. A forced merge is therefore the same
// selection the engine would make on its own after [mergeIdleRounds] fruitless cycles — one code
// path, and the seal threshold and cumulative-bytes cap still apply.
func (e *Engine) mergeIdle(opts MergeOptions) int {
	if opts.Force {
		return mergeIdleRounds
	}

	return int(e.idleMerges.Load())
}
