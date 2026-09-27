package recordengine

import (
	"context"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/memlimit"
)

// mergeCapBytes returns the decoded size at which a merged part is sealed: the tiering target
// (mergeHeight × MaxPartBytes), lowered by the memory share this merge may claim.
//
// Unlike the metric engine's cap this is denominated in *decoded* bytes, the unit tiering compares
// parts in ([part.sizeBytes]). Free space does not enter into it: the disk is bounded by the flush cap
// and the tiering target.
//
// 0 (never seal, one merge takes maxTierParts) when MaxPartBytes is unlimited, the legacy behavior.
func (e *Engine) mergeCapBytes() int64 {
	if e.mergeCap > 0 {
		return e.mergeCap
	}

	if e.cfg.MaxPartBytes <= 0 {
		return 0
	}

	target := e.cfg.MaxPartBytes * mergeHeight

	share := memlimit.MergeShare(e.cfg.MergeMemoryBytes, e.mergeConcurrency(), mergeBufferAmplification)

	// A merge that cannot even hold one flushed part's worth would make no progress; the flush cap
	// already bounds that much, so the floor is the flush cap rather than the share.
	return max(min(target, share), e.cfg.MaxPartBytes)
}

// mergeBounds are the two numbers a merge seals output parts on, in units that do not convert:
// partBytes is the decoded size one part may reach, the unit tiering compares parts in, and
// residentBytes is what the open writers may hold in RAM together, the merge's admitted share. Zero
// bounds nothing.
type mergeBounds struct {
	partBytes, residentBytes int64
}

// mergeBounds returns the bounds of a merge whose cap is capBytes. A side store (profiles) writes
// the whole unioned symbol sidecar under every part, so it lifts the part bound to write as few as it
// can; the resident bound holds for every engine and may still split a day.
func (e *Engine) mergeBounds(capBytes int64) mergeBounds {
	b := mergeBounds{partBytes: capBytes}
	if e.cfg.SideStore != nil {
		b.partBytes = 0
	}

	if share := e.mergeMemoryBudgetBytes(); share != math.MaxInt64 {
		b.residentBytes = share
	}

	return b
}

// mergeConcurrency is how many merges the memory budget admits: enough that each gets a usable
// allowance, capped by the [Config.MergeConcurrency] fan-out. Deriving it from memory is the point —
// taking it from the fan-out, which tracks the core count, prices a memory quantity in CPUs. This
// engine's cap has no free-space term, so unlike the metric engine it needs no second number.
func (e *Engine) mergeConcurrency() int {
	fanout := 1
	if e.cfg.MergeConcurrency != nil {
		fanout = max(e.cfg.MergeConcurrency(), 1)
	}

	return memlimit.MergeConcurrency(e.cfg.MergeMemoryBytes, fanout)
}

// mergeMemoryBudgetBytes is what one merge may hold resident: its share of the budget, undoubled.
// It is what [Config.MergeAdmission] is asked for, so the share a merge is sized against is one it
// has actually been given.
func (e *Engine) mergeMemoryBudgetBytes() int64 {
	return memlimit.MergeShare(e.cfg.MergeMemoryBytes, e.mergeConcurrency(), 1)
}

// admitMerge reserves the memory this merge intends to hold. It is called once the merge knows it
// has work, so a no-op cycle never consults the budget at all.
//
// A [MergeOptions.Background] merge does not wait: the facade cannot service a size-triggered flush
// until a whole maintenance cycle's fan-out returns, so parking there would delay every engine's
// flush — the mechanism that gives memory back — in order to bound the memory merges take. It
// declines and the next cycle retries. Every other caller waits, because a merge someone asked for
// must not silently no-op.
//
// Waiting still holds this engine's flushMu, which the merge takes before reaching here, so a
// waiting merge delays its own engine's flush for as long as it queues. The pool admits no new
// holders while anyone is queued, so the wait is bounded by the merges already running plus
// whatever is queued ahead.
func (e *Engine) admitMerge(ctx context.Context, background bool) (func(), bool, error) {
	if e.cfg.MergeAdmission == nil {
		return func() {}, true, nil
	}

	release, ok, err := e.cfg.MergeAdmission(ctx, e.mergeMemoryBudgetBytes(), !background)
	switch {
	case err != nil:
		return nil, false, err
	case ok, background:
		return release, ok, nil
	}

	// A caller that said it would wait and was refused anyway is a broken admission callback. The
	// alternative to erroring is returning nil having compacted nothing, which is the silent no-op
	// this whole path exists to prevent — so it surfaces rather than disappears.
	return nil, false, errors.New("merge admission declined a merge that asked to wait")
}
