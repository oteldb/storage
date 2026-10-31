package engine

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
)

func (e *Engine) lockFlush() func() {
	if e.cfg.FlushAdmission == nil {
		e.flushMu.Lock()
		e.activeBuilds.Add(1)
		return func() { e.activeBuilds.Add(-1); e.flushMu.Unlock() }
	}
	e.flushRun.Lock()
	e.flushMu.RLock()
	e.activeBuilds.Add(1)
	return func() { e.activeBuilds.Add(-1); e.flushMu.RUnlock(); e.flushRun.Unlock() }
}

func (e *Engine) lockMerge() func() {
	if e.cfg.FlushAdmission == nil {
		e.flushMu.Lock()
		e.activeBuilds.Add(1)
		return func() { e.activeBuilds.Add(-1); e.flushMu.Unlock() }
	}
	e.mergeRun.Lock()
	e.flushMu.RLock()
	e.activeBuilds.Add(1)
	return func() { e.activeBuilds.Add(-1); e.flushMu.RUnlock(); e.mergeRun.Unlock() }
}

// mergeSourcesLive prevents a concurrent reload from making the merge resurrect removed sources.
func (e *Engine) mergeSourcesLive(selected []*part) bool {
	live := make(map[*part]struct{}, len(e.parts))
	for _, p := range e.parts {
		live[p] = struct{}{}
	}
	for _, p := range selected {
		if _, ok := live[p]; !ok {
			return false
		}
	}
	return true
}

func (e *Engine) pinMergeSourcesLocked(ctx context.Context) ([]*part, func(), error) {
	for _, p := range e.parts {
		if p.pending != nil {
			if err := e.updateIndexLocked(ctx); err != nil {
				return nil, nil, err
			}
			break
		}
	}
	src := make([]*part, 0, len(e.parts))
	for _, p := range e.parts {
		if p.pending == nil {
			p.acquire()
			src = append(src, p)
		}
	}
	return src, func() {
		for _, p := range src {
			p.release()
		}
		e.reclaimRetired(ctx)
	}, nil
}

func (e *Engine) publishMerge(
	ctx context.Context, selected, newParts, readable []*part, stamp uint64, plan rollupPlan,
) (bool, error) {
	removed := make(map[string]struct{}, len(selected))
	for _, p := range selected {
		removed[p.prefix] = struct{}{}
	}

	e.mu.Lock()
	if !e.mergeSourcesLive(selected) {
		e.retireLocked(newParts)
		e.mu.Unlock()
		e.reclaimRetired(ctx)
		return false, nil
	}
	next := replaceParts(e.parts, removed, newParts...)

	if err := e.commitIndexLocked(ctx, next, e.flushedEpoch, stamp, rollupGuard(readable, plan.marker, e)); err != nil {
		if !errors.Is(err, errRollupConflict) {
			e.mu.Unlock()

			return false, err
		}

		// Never committed and never readable, so nothing waits on these before they are deleted.
		e.retireLocked(newParts)
		e.mu.Unlock()
		e.reclaimRetired(ctx)

		zctx.From(ctx).Warn("merge output dropped: a rival writer committed a part recording a downsample "+
			"layout that does not nest with this merge's; the next merge replans with it",
			zap.String("prefix", e.cfg.Prefix))

		return false, nil
	}

	e.parts = next
	e.retireLocked(selected)
	e.trimRecentBelow(plan.rolledBefore())
	// Rows that did not survive the merge are retention's work: the samples are gone, so the
	// identities naming them may be dead too and an identity prune has something to find. Merging
	// without dropping rows (a plain compaction) leaves every identity backed, so it arms nothing.
	if partRows(newParts) < partRows(selected) {
		e.identityDirty = true
	}

	e.mu.Unlock()

	e.reclaimRetired(ctx)

	return true, nil
}

func (e *Engine) canceledFlush(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		if e.HeadBytes() == 0 {
			return true, nil
		}
		return true, err
	}
	return false, nil
}

func (e *Engine) admitFlushWork(ctx context.Context) (func(), error) {
	if canceled, err := e.canceledFlush(ctx); canceled {
		return nil, err
	}
	unlock := e.lockFlush()
	if canceled, err := e.canceledFlush(ctx); canceled {
		unlock()
		return nil, err
	}
	if err := e.admitFlush(ctx); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}
