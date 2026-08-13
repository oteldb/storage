package recordengine

import "github.com/oteldb/storage/internal/memlimit"

// mergeCapBytes returns the decoded size at which a merged part is sealed, and with it the bound on
// what one merge may hold: the tiering target (mergeHeight × MaxPartBytes), lowered to the memory
// this merge may claim.
//
// Unlike the metric engine's cap this is denominated in *decoded* bytes, because that is what the
// record merge holds — the selected sources are decoded up front and the output accumulates as
// decoded columns before it is encoded. Free space therefore does not enter into it: the disk is
// bounded by the flush cap and the tiering target, memory by this.
//
// 0 (never seal, one merge takes maxTierParts) when MaxPartBytes is unlimited, the legacy behavior.
func (e *Engine) mergeCapBytes() int64 {
	if e.cfg.MaxPartBytes <= 0 {
		return 0
	}

	target := e.cfg.MaxPartBytes * mergeHeight

	concurrency := 1
	if e.cfg.MergeConcurrency != nil {
		concurrency = max(e.cfg.MergeConcurrency(), 1)
	}

	share := memlimit.MergeShare(e.cfg.MergeMemoryBytes, concurrency, mergeBufferAmplification)

	// A merge that cannot even hold one flushed part's worth would make no progress; the flush cap
	// already bounds that much, so the floor is the flush cap rather than the share.
	return max(min(target, share), e.cfg.MaxPartBytes)
}
