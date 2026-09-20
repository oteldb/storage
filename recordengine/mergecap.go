package recordengine

import (
	"context"

	"github.com/oteldb/storage/internal/memlimit"
)

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

	share := memlimit.MergeShare(e.cfg.MergeMemoryBytes, e.mergeConcurrency(), mergeBufferAmplification)

	// A merge that cannot even hold one flushed part's worth would make no progress; the flush cap
	// already bounds that much, so the floor is the flush cap rather than the share.
	return max(min(target, share), e.cfg.MaxPartBytes)
}

// mergeConcurrency is how many merges may be running at once, derived from the memory budget so each
// gets a usable allowance, with [Config.MergeConcurrency] as the ceiling rather than the divisor.
func (e *Engine) mergeConcurrency() int {
	cpu := 1
	if e.cfg.MergeConcurrency != nil {
		cpu = e.cfg.MergeConcurrency()
	}

	return memlimit.MergeConcurrency(e.cfg.MergeMemoryBytes, cpu)
}

// mergeMemoryBudgetBytes is what one merge may hold resident: its share of the budget, undoubled.
// It is what [Config.MergeAdmission] is asked for, so the share a merge is sized against is one it
// has actually been given.
func (e *Engine) mergeMemoryBudgetBytes() int64 {
	return memlimit.MergeShare(e.cfg.MergeMemoryBytes, e.mergeConcurrency(), 1)
}

// admitMerge reserves the memory this merge intends to hold, blocking until the process has it to
// spare. It is called once the merge knows it has work: a no-op cycle must not queue behind a merge
// that is running, or one busy engine would stall every other engine's maintenance pass.
//
// The engine holds flushMu across the whole merge, so a merge waiting here also delays this engine's
// next flush. That is the intended back-pressure — the alternative is merges that collectively hold
// more than the process has — but it is why admission is taken after selection and not before.
func (e *Engine) admitMerge(ctx context.Context) (func(), error) {
	if e.cfg.MergeAdmission == nil {
		return func() {}, nil
	}

	return e.cfg.MergeAdmission(ctx, e.mergeMemoryBudgetBytes())
}
