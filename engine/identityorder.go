package engine

import (
	"container/heap"
	"context"
	"slices"
	"sync/atomic"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/identitysnapshot"
	"github.com/oteldb/storage/signal"
)

const identityOrderBatch = 64 << 10

type identityOrder struct {
	index *series.Index
	known int
	runs  [][]uint32
	bytes atomic.Int64
}

func (o *identityOrder) reset() {
	o.index, o.known, o.runs = nil, 0, nil
	o.bytes.Store(0)
}

func (o *identityOrder) update(ctx context.Context, index *series.Index, view series.View, reserve func(int64) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.index != index || o.known > view.Len() {
		o.reset()
		o.index = index
	}
	if err := o.mergeTail(ctx, view, reserve); err != nil {
		return err
	}
	for o.known < view.Len() {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(identityOrderBatch, view.Len()-o.known)
		if err := reserve(int64(n) * 4); err != nil {
			return err
		}
		refs := make([]uint32, n)
		for i := range refs {
			refs[i] = uint32(o.known + i)
		}
		sortIdentityRefs(view, refs)
		o.runs = append(o.runs, refs)
		o.known += n
		o.bytes.Store(int64(o.known) * 4)
		if err := o.mergeTail(ctx, view, reserve); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (o *identityOrder) mergeTail(ctx context.Context, view series.View, reserve func(int64) error) error {
	for len(o.runs) > 1 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := len(o.runs)
		a, b := o.runs[n-2], o.runs[n-1]
		if len(a) > 2*len(b) {
			break
		}
		if err := reserve(int64(len(a)+len(b)) * 4); err != nil {
			return err
		}
		merged, err := mergeIdentityRefs(ctx, view, a, b)
		if err != nil {
			return err
		}
		o.runs[n-2], o.runs[n-1] = merged, nil
		o.runs = o.runs[:n-1]
	}
	return nil
}

func sortIdentityRefs(view series.View, refs []uint32) {
	slices.SortFunc(refs, func(a, b uint32) int { return view.ID(int(a)).Compare(view.ID(int(b))) })
}

func mergeIdentityRefs(ctx context.Context, view series.View, a, b []uint32) ([]uint32, error) {
	out := make([]uint32, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if len(out)%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if view.ID(int(a[0])).Compare(view.ID(int(b[0]))) < 0 {
			out = append(out, a[0])
			a = a[1:]
		} else {
			out = append(out, b[0])
			b = b[1:]
		}
	}
	out = append(out, a...)
	out = append(out, b...)
	return out, ctx.Err()
}

type identityOrderCursor struct {
	refs []uint32
	pos  int
	id   signal.SeriesID
}

type identityOrderHeap []*identityOrderCursor

func (h *identityOrderHeap) Len() int           { return len(*h) }
func (h *identityOrderHeap) Less(i, j int) bool { return (*h)[i].id.Compare((*h)[j].id) < 0 }
func (h *identityOrderHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *identityOrderHeap) Push(v any)         { *h = append(*h, v.(*identityOrderCursor)) }
func (h *identityOrderHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return last
}

func walkIdentityOrder(ctx context.Context, view series.View, runs [][]uint32, emit func(signal.SeriesID, uint32) error) error {
	if len(runs) == 1 {
		for i, ref := range runs[0] {
			if i%4096 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if err := emit(view.ID(int(ref)), ref); err != nil {
				return err
			}
		}
		return nil
	}
	cursors := make([]identityOrderCursor, len(runs))
	h := make(identityOrderHeap, 0, len(runs))
	for i, refs := range runs {
		if len(refs) == 0 {
			continue
		}
		cursors[i] = identityOrderCursor{refs: refs, id: view.ID(int(refs[0]))}
		h = append(h, &cursors[i])
	}
	heap.Init(&h)
	for n := 0; len(h) > 0; n++ {
		if n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		cursor := h[0]
		if err := emit(cursor.id, cursor.refs[cursor.pos]); err != nil {
			return err
		}
		cursor.pos++
		if cursor.pos == len(cursor.refs) {
			heap.Pop(&h)
		} else {
			cursor.id = view.ID(int(cursor.refs[cursor.pos]))
			heap.Fix(&h, 0)
		}
	}
	return nil
}

type detachedIdentityOrder struct {
	view      series.View
	runs      [][]uint32
	workspace int64
}

func (e *Engine) prepareDetachedOrder(ctx context.Context, selected map[signal.SeriesID]*sampleBuf,
	reserve func(int64) error,
) (detachedIdentityOrder, error) {
	if err := ctx.Err(); err != nil {
		return detachedIdentityOrder{}, err
	}
	e.mu.RLock()
	index := e.head.series
	view := index.View()
	e.mu.RUnlock()
	if len(selected) < view.Len()/4+min(view.Len()%4, 1) {
		return e.sparseDetachedOrder(ctx, view, selected, reserve)
	}
	if err := e.identityOrder.update(ctx, index, view, reserve); err != nil {
		return detachedIdentityOrder{}, err
	}
	return detachedIdentityOrder{view: view, runs: e.identityOrder.runs}, ctx.Err()
}

func (e *Engine) sparseDetachedOrder(ctx context.Context, view series.View,
	selected map[signal.SeriesID]*sampleBuf, reserve func(int64) error,
) (detachedIdentityOrder, error) {
	if err := reserve(int64(len(selected)) * 4); err != nil {
		return detachedIdentityOrder{}, err
	}
	refs := make([]uint32, 0, len(selected))
	ids := func(yield func(signal.SeriesID) bool) {
		for id := range selected {
			if ctx.Err() != nil || !yield(id) {
				return
			}
		}
	}
	identitysnapshot.Capture(e.mu.RLocker(), func() *series.Index { return e.head.series }, ids,
		func(_ signal.SeriesID, _ series.View, ref uint32) { refs = append(refs, ref) })
	out := detachedIdentityOrder{view: view, workspace: int64(cap(refs)) * 4}
	for len(refs) > 0 {
		if err := ctx.Err(); err != nil {
			return detachedIdentityOrder{}, err
		}
		n := min(len(refs), identityOrderBatch)
		run := refs[:n:n]
		sortIdentityRefs(view, run)
		out.runs = append(out.runs, run)
		refs = refs[n:]
	}
	return out, ctx.Err()
}
