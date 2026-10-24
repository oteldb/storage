package engine

import (
	"context"

	"go.uber.org/zap"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/internal/mergestream"
)

const (
	defaultMergeCeilingBytes = mergestream.DefaultPartCeiling
	minMergeCapBytes         = mergestream.MinPartCap
)

func (e *Engine) mergeCapBytes(ctx context.Context) int64 {
	capBytes, err := mergestream.PartCap(ctx, e.cfg.Backend, e.cfg.MergeCeilingBytes, e.mergeFanout())
	if err != nil {
		zctx.From(ctx).Warn("free space unavailable; merge cap falls back to the ceiling",
			zap.String("prefix", e.cfg.Prefix), zap.Error(err))
	}
	return capBytes
}

// mergeBudget is the pair a streamed merge seals an output part on: capBytes on disk, and this
// merge's share of memory resident. The two are separate units and neither bounds the other.
func (e *Engine) mergeBudget(capBytes int64) mergestream.Budget {
	return mergestream.Budget{DiskBytes: capBytes, ResidentBytes: e.mergeMemoryBudgetBytes()}
}

// mergeMemoryBudgetBytes is what one merge may hold resident, the bound that replaces the part-size
// cap once column frames stream out to the backend. What is left in RAM then is not the part but the
// per-series state built alongside it — the id runs, the series index, the aggregate sidecar — which
// is O(distinct series) and so is not bounded by the part's bytes at all: a merge of very short
// series holds far more of it per encoded byte than a merge of long ones. Sealing on it directly
// bounds memory without pricing the part against it.
//
// It is a share of the same allowance the cap comes from, taken at face value rather than doubled:
// the buffer it prices is the working set itself, not an encoded copy about to be serialized.
func (e *Engine) mergeMemoryBudgetBytes() int64 {
	return memlimit.MergeShare(e.cfg.MergeMemoryBytes, e.mergeConcurrency())
}

// mergeFanout is how many merges may be running against this engine's backend at once, per
// [Config.MergeConcurrency]. It is the bound on *disk* a merge may claim.
func (e *Engine) mergeFanout() int {
	if e.cfg.MergeConcurrency == nil {
		return 1
	}

	return max(e.cfg.MergeConcurrency(), 1)
}

// mergeConcurrency is how many merges the memory budget admits: enough that each gets a usable
// allowance, capped by [Engine.mergeFanout]. It is the divisor for a merge's *memory* share, and
// deriving it from memory is the point — taking it from the fan-out, which tracks the core count,
// prices a memory quantity in CPUs.
func (e *Engine) mergeConcurrency() int {
	return memlimit.MergeConcurrency(e.cfg.MergeMemoryBytes, e.mergeFanout())
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
