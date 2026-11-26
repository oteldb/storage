package engine

import (
	"context"
	"slices"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func (h *head) freezeIdentity(contexts *series.Contexts, templates *series.AttributeTemplates) {
	frozen := newHeadWithContexts(contexts, templates)
	h.swapGenerationIdentity(frozen)
	h.frozen = frozen
}

func (h *head) swapGenerationIdentity(other *head) {
	h.sym, other.sym = other.sym, h.sym
	h.series, other.series = other.series, h.series
	h.post, other.post = other.post, h.post
	h.nonStringLabels, other.nonStringLabels = other.nonStringLabels, h.nonStringLabels
	h.valueScratch, other.valueScratch = other.valueScratch, h.valueScratch
	h.encodedRefs, other.encodedRefs = other.encodedRefs, h.encodedRefs
	h.maxEncodedDescriptor, other.maxEncodedDescriptor = other.maxEncodedDescriptor, h.maxEncodedDescriptor
	h.stateRefs, other.stateRefs = other.stateRefs, h.stateRefs
	h.slots, other.slots = other.slots, h.slots
}

func (h *head) restoreFrozenIdentity() {
	if h.frozen == nil {
		return
	}
	frozen := h.frozen
	view := h.series.View()
	var attrs signal.Attributes
	for i := range view.Len() {
		id := view.ID(i)
		ref, ok := frozen.series.Ref(id)
		if !ok {
			s := view.At(i, attrs)
			attrs = s.Attributes
			frozen.register(series.Entry{ID: id, Series: s})
			ref, _ = frozen.series.Ref(id)
		}
		if ts, seen := h.watermark(id); seen {
			prior, had := frozen.slots.get(frozen.stateRefs[ref])
			if !had || ts > prior {
				frozen.slots.set(frozen.stateRefs[ref], ts)
			}
		}
	}
	h.swapGenerationIdentity(frozen)
	h.frozen = nil
}

func (h *head) frozenState(id signal.SeriesID) (headSeriesState, bool) {
	if h.frozen == nil {
		return headSeriesState{}, false
	}
	ref, exists := h.frozen.series.Ref(id)
	if !exists {
		return h.frozen.frozenState(id)
	}
	ts, seen := h.frozen.slots.get(h.frozen.stateRefs[ref])
	return headSeriesState{newest: ts, seen: seen, historical: true}, true
}

// A historical lookup can finish after a newer accepted write evicted its cache entry.
func (h *head) rememberHistorical(id signal.SeriesID, newest int64) {
	if current, seen := h.watermark(id); seen {
		newest = max(newest, current)
	}
	h.disk.remember(id, newest)
}

func (h *head) getIdentity(id signal.SeriesID) (signal.Series, bool) {
	if s, ok := h.series.Get(id); ok {
		return s, true
	}
	if h.frozen != nil {
		return h.frozen.getIdentity(id)
	}
	return signal.Series{}, false
}

func (h *head) identityMaterializationBytes(id signal.SeriesID) int64 {
	if ref, ok := h.series.Ref(id); ok {
		return h.series.View().MaterializationBytes(int(ref))
	}
	if h.frozen != nil {
		return h.frozen.identityMaterializationBytes(id)
	}
	return 0
}

func (h *head) identityCount() int {
	n := h.series.Len()
	if h.frozen != nil {
		n += h.frozen.identityCount()
	}
	return n
}

func (h *head) identityIDs() []signal.SeriesID {
	ids := make([]signal.SeriesID, 0, h.identityCount())
	view := h.series.View()
	for i := range view.Len() {
		ids = append(ids, view.ID(i))
	}
	if h.frozen != nil {
		ids = append(ids, h.frozen.identityIDs()...)
	}
	slices.SortFunc(ids, signal.SeriesID.Compare)
	return slices.Compact(ids)
}

func (h *head) resolve(matchers []fetch.Matcher) []signal.SeriesID {
	ids := h.resolveLocal(matchers)
	if h.frozen != nil {
		ids = append(ids, h.frozen.resolve(matchers)...)
		slices.SortFunc(ids, signal.SeriesID.Compare)
		ids = slices.Compact(ids)
	}
	return ids
}

func (e *Engine) flushIdentityIndex() *series.Index {
	if e.head.frozen != nil {
		return e.head.frozen.series
	}
	return e.head.series
}

func (e *Engine) rememberFrozenWatermarks(ctx context.Context) error {
	e.mu.RLock()
	frozen := e.head.frozen
	e.mu.RUnlock()
	return e.rememberGenerationWatermarks(ctx, frozen, 0)
}

func (e *Engine) rememberGenerationWatermarks(ctx context.Context, frozen *head, descriptorEpoch uint64) error {
	e.mu.RLock()
	enabled := frozen != nil && e.head.disk.cache != nil
	e.mu.RUnlock()
	if !enabled {
		return nil
	}
	view := frozen.series.View()
	for off := 0; off < view.Len(); off += 256 {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.parallel != nil {
			e.mu.RLockWriter()
			e.mu.LockState()
		} else {
			e.mu.LockWriter()
		}
		for i := off; i < min(off+256, view.Len()); i++ {
			if ts, seen := frozen.slots.get(frozen.stateRefs[i]); seen {
				if descriptorEpoch != 0 {
					id := view.ID(i)
					if current, seen := e.head.watermark(id); seen {
						ts = max(ts, current)
					}
					e.head.disk.cache.remember(id, ts, descriptorEpoch)
				} else {
					e.head.rememberHistorical(view.ID(i), ts)
				}
			}
		}
		if e.parallel != nil {
			e.mu.UnlockState()
			e.mu.RUnlockWriter()
		} else {
			e.mu.UnlockWriter()
		}
		if e.parallel != nil && e.parallel.hooks != nil && e.parallel.hooks.afterFrozenCacheBatch != nil {
			e.parallel.hooks.afterFrozenCacheBatch()
		}
	}
	return ctx.Err()
}
