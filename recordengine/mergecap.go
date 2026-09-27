package recordengine

import (
	"context"
	"math"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

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

// mergePartBytes is the decoded size one output part of a merge whose cap is capBytes may reach, the
// unit tiering compares parts in; 0 bounds nothing. A side store (profiles) writes a symbol sidecar
// under every part, copying the entries its parts share into each, so it lifts the part bound to write
// as few as it can; the merge's grant holds for every engine and may still split a day.
func (e *Engine) mergePartBytes(capBytes int64) int64 {
	if e.cfg.SideStore != nil {
		return 0
	}

	return capBytes
}

// mergeWriterBudget splits a merge's grant. What the sources hold from the moment they open
// (read-ahead windows, frame buffers, dictionaries or whole decodes) and the encoders' workspace come
// off the top; the writers get the rest, of which the router keeps room for one append
// ([appendReserve]). need is what the grant must be for the writers to get their floor: two appends
// and finish, one writer's finish at the cap. A grant of 0 bounds nothing.
func mergeWriterBudget(
	schema *Schema, grant int64, sources []mergeSource, coders *mergeCoders, runBytes, finish int64,
) (limit, reserve, need int64) {
	held := coders.workspace()
	entries := 0

	for _, s := range sources {
		held += s.residentBytes()
		entries += s.dictEntries()
	}

	reserve = appendReserve(schema, entries, runBytes)
	need = held + 2*reserve + finish

	if grant <= 0 {
		return 0, reserve, need
	}

	return grant - held, reserve, need
}

// mergeGrant is the memory a merge holds admission for: sources, encoder and writers together.
// bytes 0 bounds nothing.
type mergeGrant struct {
	bytes   int64
	release func()
	wait    bool
	admit   func(ctx context.Context, bytes int64, wait bool) (func(), bool, error)
}

// done hands the grant back.
func (g *mergeGrant) done() {
	if g.release != nil {
		g.release()
		g.release = nil
	}
}

// top takes n more bytes if they are free now, never waiting: a merge that waited for more while
// holding its grant could deadlock with another doing the same. It reports whether it got them.
func (g *mergeGrant) top(ctx context.Context, n int64) (bool, error) {
	if g.admit == nil {
		g.bytes += n

		return true, nil
	}

	release, ok, err := g.admit(ctx, n, false)
	if err != nil || !ok {
		return false, err
	}

	held := g.release
	g.release = func() {
		held()
		release()
	}
	g.bytes += n

	return true, nil
}

// regrant hands the grant back and waits for n, holding nothing while it queues.
func (g *mergeGrant) regrant(ctx context.Context, n int64) error {
	g.done()

	release, ok, err := g.admit(ctx, n, true)
	switch {
	case err != nil:
		return err
	case !ok:
		return errMergeRefusedWaiter
	}

	g.release, g.bytes = release, n

	return nil
}

// errMergeDeclined reports a merge that may not wait and needs more memory than is free right now.
var errMergeDeclined = errors.New("merge needs more memory than is free")

// errMergeRefusedWaiter is a broken admission callback: it declined a caller that said it would
// wait. The alternative to erroring is returning nil having compacted nothing, which is the silent
// no-op waiting exists to prevent.
var errMergeRefusedWaiter = errors.New("merge admission declined a merge that asked to wait")

// mergeGrantBytes is what a merge of src reserves: its share, or its need when that is more; 0 when
// the budget is unbounded.
func (e *Engine) mergeGrantBytes(ctx context.Context, src []*part, capBytes int64) int64 {
	share := e.mergeMemoryBudgetBytes()
	if share == math.MaxInt64 {
		return 0
	}

	if need, ok := e.mergeNeed(ctx, src, capBytes); ok {
		return max(share, need)
	}

	return share
}

// reportOverBudget accounts a merge that holds need against the whole process merge budget: past
// it, admission can only clamp the request to the budget, so the merge runs alone and holds the rest
// over it, which is counted and logged rather than left silent.
func (e *Engine) reportOverBudget(ctx context.Context, need int64) {
	budget := memlimit.MergeBudget(e.cfg.MergeMemoryBytes)
	if budget == math.MaxInt64 || need <= budget {
		return
	}

	e.cfg.Obs.Merge.OverBudget(ctx, e.cfg.Signal, need-budget)
	zctx.From(ctx).Warn("merge needs more than the whole merge memory budget; it runs alone and holds past it",
		zap.String("signal", e.cfg.Signal), zap.String("prefix", e.cfg.Prefix),
		zap.Int64("need", need), zap.Int64("budget", budget))
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

// admitMerge reserves the memory a merge of src intends to hold: its share, or what its sources,
// encoder and writers' floor need if that is more ([Engine.mergeNeed]), so a merge never holds more
// than it reserved. A source whose manifest cannot bound it leaves the share reserved, and the merge
// tops it up once it has opened the sources and can measure them. It is called once the merge knows
// it has work, so a no-op cycle never consults the budget at all.
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
func (e *Engine) admitMerge(ctx context.Context, src []*part, capBytes int64, background bool) (*mergeGrant, bool, error) {
	g := &mergeGrant{bytes: e.mergeGrantBytes(ctx, src, capBytes), wait: !background, admit: e.cfg.MergeAdmission}
	if e.cfg.MergeAdmission == nil {
		return g, true, nil
	}

	ask := g.bytes
	if ask == 0 {
		ask = e.mergeMemoryBudgetBytes()
	}

	release, ok, err := e.cfg.MergeAdmission(ctx, ask, !background)
	switch {
	case err != nil:
		return nil, false, err
	case ok:
		g.release = release

		return g, true, nil
	case background:
		return nil, false, nil
	}

	return nil, false, errMergeRefusedWaiter
}
