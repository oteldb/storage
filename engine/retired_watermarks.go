package engine

import (
	"context"
	"time"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

// retiredWatermarks retains only the identity state needed by admission after a catalog commit.
// It owns no sample arena and the generation coordinator joins the refresh before publishing again.
type retiredWatermarks struct {
	index   *series.Index
	refs    []uint32
	slots   headSlots
	cache   *diskSeriesCache
	lineage uint64
	bytes   int64
	done    chan struct{}
}

func newRetiredWatermarks(cache *diskSeriesCache, frozen *head, lineage uint64) *retiredWatermarks {
	if cache == nil {
		return nil
	}
	return &retiredWatermarks{
		index: frozen.series, refs: frozen.stateRefs, slots: frozen.slots, cache: cache, lineage: lineage,
		bytes: frozen.series.SizeBytes() + memsize.Slice(frozen.stateRefs) + frozen.slots.sizeBytes(),
		done:  make(chan struct{}),
	}
}

func (h *head) historicalState(id signal.SeriesID) (diskHeadEntry, bool) {
	if r := h.retiring; r != nil && r.cache == h.disk.cache {
		if ref, ok := r.index.Ref(id); ok {
			if ts, seen := r.slots.get(r.refs[ref]); seen {
				return diskHeadEntry{newest: ts, descriptorLineage: r.lineage, exists: true}, true
			}
		}
	}
	return h.disk.cache.get(id)
}

func (e *Engine) refreshRetiredWatermarks(ctx context.Context, r *retiredWatermarks) {
	start := time.Now()
	view := r.index.View()
	for i, ref := range r.refs {
		if ts, seen := r.slots.get(ref); seen {
			r.cache.remember(view.ID(i), ts, r.lineage)
		}
		if i%256 == 255 && e.parallel.hooks != nil && e.parallel.hooks.afterFrozenCacheBatch != nil {
			e.parallel.hooks.afterFrozenCacheBatch()
		}
	}
	e.cfg.Obs.Flush.HeadPhase(ctx, "watermark_refresh", time.Since(start))
	e.mu.LockState()
	e.head.retiring = nil
	e.notifyPressureProgress()
	close(r.done)
	e.mu.UnlockState()
}
