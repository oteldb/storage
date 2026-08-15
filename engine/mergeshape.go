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
	// Candidates is how many parts the next size-driven merge would select right now. 0 with a
	// non-zero Backlog is the stuck state: parts remain mergeable but no run of them qualifies.
	Candidates int
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

// MergeShape returns the selector's view of the engine's parts. It takes a brief read lock, does no
// backend I/O and decodes nothing, so it is safe to poll at dashboard cadence.
func (e *Engine) MergeShape() MergeShape {
	e.mu.RLock()
	src := e.parts
	e.mu.RUnlock()

	capBytes := e.lastMergeCap.Load()
	idle := int(e.idleMerges.Load())
	sealedN, backlog, bestM := mergeShape(src, capBytes)

	return MergeShape{
		Parts:          len(src),
		Sealed:         sealedN,
		Backlog:        backlog,
		Candidates:     len(pickMergeRun(src, capBytes, idle)),
		CapBytes:       capBytes,
		BestMultiplier: bestM,
		MinMultiplier:  minMergeMultiplier,
		IdleRounds:     idle,
		WaiveAfter:     mergeIdleRounds,
	}
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
