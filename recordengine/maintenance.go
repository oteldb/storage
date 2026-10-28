package recordengine

import (
	"context"
	"maps"
	"math"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/signal"
)

func (e *Engine) lockFlush() func() {
	if e.cfg.FlushAdmission == nil || e.cfg.SideStore != nil {
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
	if e.cfg.FlushAdmission == nil || e.cfg.SideStore != nil {
		e.flushMu.Lock()
		e.activeBuilds.Add(1)
		return func() { e.activeBuilds.Add(-1); e.flushMu.Unlock() }
	}
	e.mergeRun.Lock()
	e.flushMu.RLock()
	e.activeBuilds.Add(1)
	return func() { e.activeBuilds.Add(-1); e.flushMu.RUnlock(); e.mergeRun.Unlock() }
}

func (e *Engine) reserveFlush(
	ctx context.Context, detached map[signal.SeriesID]*recordCols, values int64, side map[string][]byte,
) (func(), error) {
	if e.cfg.FlushAdmission == nil {
		return func() {}, nil
	}
	var rows, identities, sides, workspace, materialized int64
	first := true
	for _, buf := range detached {
		rows = buildbudget.Add(rows, int64(buf.len()))
	}
	e.captureIdentities(maps.Keys(detached), func(_ signal.SeriesID, view series.View, ref uint32) {
		identities = buildbudget.Add(identities, view.EncodingBytes(int(ref)))
		if first {
			workspace = view.EncodingWorkspace()
			first = false
		}
		materialized = buildbudget.Add(materialized, view.MaterializationBytes(int(ref)))
	})
	for _, data := range side {
		sides = buildbudget.Add(sides, 256+int64(len(data)))
	}
	columns := int64(1 + e.cfg.Schema.numInts() + e.cfg.Schema.numBytes())
	estimate := buildbudget.Flush(rows, values, identities, int64(len(detached)), columns, sides, buildbudget.Add(workspace, materialized))
	if e.cfg.SideStore != nil {
		// SideStore has no bound for Encode/Stored on the active accumulator; run this flush alone.
		estimate = math.MaxInt64
	}
	return e.cfg.FlushAdmission(ctx, estimate)
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

func (e *Engine) publishFlushLocked(ctx context.Context, newParts []*part, epoch uint64, checkpointSeq int) error {
	next := e.parts
	for _, p := range newParts {
		next = appendPart(next, p)
	}
	epoch = max(epoch, e.flushedEpoch)
	err := e.commitIndexLocked(ctx, next, epoch, e.term())
	// Keep completed flush outputs locally even when commit confirmation fails. Their WAL
	// remains intact, and the next commit carries these same outputs rather than rewriting rows.
	e.parts, e.flushedEpoch = next, epoch
	e.flushing = nil
	e.flushingSide = nil
	e.head.releaseDetached()
	if err == nil {
		err = e.checkpointLocked(checkpointSeq)
	}
	return err
}

func (e *Engine) releaseFlushScratch() {
	if e.cfg.FlushAdmission != nil {
		e.flushBuf, e.bloomBuf = nil, nil
	}
}
