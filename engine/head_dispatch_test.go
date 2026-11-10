package engine

import (
	"context"
	"runtime/pprof"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

const dispatchRequests = 8

type headDispatchHooks struct {
	beforeReceive func(int)
	beforeWrite   func(int, int)
	queued        func(int)
	completed     func(int)
}

type headRequest struct {
	ctx         context.Context //nolint:containedctx // Queued request retains caller labels until completion.
	ids         []signal.SeriesID
	ts          []int64
	values, sf  []float64
	materialize func(int) signal.Series
	done        chan struct{}
	result      AppendResult
	err         error
}

type headDispatcher struct {
	mu       sync.RWMutex
	closed   bool
	stop     sync.Once
	wg       sync.WaitGroup
	slots    chan struct{}
	queues   []chan *headRequest
	prepared bool
	hooks    *headDispatchHooks
}

func newHeadDispatcher(ctx context.Context, heads []*Engine, prepared bool, hooks *headDispatchHooks) *headDispatcher {
	d := &headDispatcher{slots: make(chan struct{}, dispatchRequests), queues: make([]chan *headRequest, len(heads)), prepared: prepared, hooks: hooks}
	for lane, h := range heads {
		d.queues[lane] = make(chan *headRequest, dispatchRequests)
		d.wg.Go(func() { d.run(ctx, lane, h) })
	}
	return d
}

func (d *headDispatcher) begin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case d.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		<-d.slots
		return errors.New("prototype dispatcher is closed")
	}
	return nil
}

func (d *headDispatcher) end() { d.mu.RUnlock(); <-d.slots }

func (d *headDispatcher) close() {
	d.stop.Do(func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.closed = true
		for _, q := range d.queues {
			close(q)
		}
		d.wg.Wait()
	})
}

func (b *headRouteBatch) dispatchBatch(ctx context.Context, d *headDispatcher, materialize func(int) signal.Series) (AppendResult, error) {
	b.requests = resize(b.requests, len(d.queues))
	for lane := range d.queues {
		lo, hi := b.offsets[lane], b.offsets[lane+1]
		if lo == hi {
			continue
		}
		r := &b.requests[lane]
		if r.done == nil {
			r.done = make(chan struct{}, 1)
		}
		r.ctx = ctx //nolint:fatcontext // Independent request envelope, not nested derived contexts.
		r.ids, r.ts, r.values = b.ids[lo:hi], b.ts[lo:hi], b.values[lo:hi]
		r.sf = nil
		if b.weighted {
			r.sf = b.sf[lo:hi]
		}
		r.materialize = func(i int) signal.Series {
			b.materializeMu.Lock()
			defer b.materializeMu.Unlock()
			return materialize(b.refs[lo+i])
		}
		r.result, r.err = AppendResult{}, nil
		d.queues[lane] <- r
		if d.hooks != nil && d.hooks.queued != nil {
			d.hooks.queued(lane)
		}
	}
	var result AppendResult
	var failure error
	for lane := range d.queues {
		if b.offsets[lane] == b.offsets[lane+1] {
			continue
		}
		r := &b.requests[lane]
		<-r.done
		result.Accepted += r.result.Accepted
		result.RejectedOOO += r.result.RejectedOOO
		failure = errors.Join(failure, r.err)
		r.ids, r.ts, r.values, r.sf, r.materialize, r.ctx = nil, nil, nil, nil, nil, nil
	}
	return result, failure
}

func (d *headDispatcher) run(ctx context.Context, lane int, h *Engine) {
	q := d.queues[lane]
	var carry *headRequest
	var scratch headCombine
	var group [dispatchRequests]*headRequest
	for {
		if d.hooks != nil && d.hooks.beforeReceive != nil {
			d.hooks.beforeReceive(lane)
		}
		first := carry
		carry = nil
		if first == nil {
			var ok bool
			first, ok = <-q
			if !ok {
				return
			}
		}
		group[0] = first
		n, points := 1, len(first.ids)
	collect:
		for n < len(group) {
			select {
			case next, ok := <-q:
				if !ok {
					break collect
				}
				if points+len(next.ids) > maxPreparedBatchSamples {
					carry = next
					break collect
				}
				group[n] = next
				n++
				points += len(next.ids)
			default:
				break collect
			}
		}
		if d.hooks != nil && d.hooks.beforeWrite != nil {
			d.hooks.beforeWrite(lane, n)
		}
		pprof.SetGoroutineLabels(first.ctx) //nolint:contextcheck // Profiling follows the queued caller, not worker ownership.
		if n == 1 {
			first.result, first.err = appendHeadLane(h, d.prepared, first.ids, first.ts, first.values, first.sf, first.materialize)
		} else {
			d.writeGroup(h, group[:n], &scratch)
		}
		pprof.SetGoroutineLabels(ctx)
		if d.hooks != nil && d.hooks.completed != nil {
			d.hooks.completed(lane)
		}
		for i, r := range group[:n] {
			r.done <- struct{}{}
			group[i] = nil
		}
	}
}

type headCombine struct {
	ids []signal.SeriesID
}

func (d *headDispatcher) writeGroup(h *Engine, group []*headRequest, s *headCombine) {
	if d.prepared && residentDispatchHistory(h) {
		for _, r := range group {
			r.result, r.err = appendHeadLane(h, true, r.ids, r.ts, r.values, r.sf, r.materialize)
		}
		return
	}
	metricBatchPreparations.active <- struct{}{}
	var p *preparedBatch
	select {
	case p = <-metricBatchPreparations.free:
	default:
		p = &preparedBatch{batch: newWALBatch()}
	}
	defer metricBatchPreparations.release(p)
	s.ids = s.ids[:0]
	for _, r := range group {
		s.ids = append(s.ids, r.ids...)
	}
	p.prepare(s.ids)
	err := d.applyGroup(h, group, p, s.ids)
	if err != nil {
		for _, r := range group {
			r.result, r.err = AppendResult{}, err
		}
	}
}

func residentDispatchHistory(h *Engine) bool {
	if h.parallel == nil {
		return false
	}
	h.mu.RLockWriter()
	defer h.mu.RUnlockWriter()
	return len(h.parts)+len(h.foreignParts) > 0 && h.head.disk != nil && h.head.disk.complete &&
		int64(h.head.series.Len()) == h.head.cardinality()
}

func (d *headDispatcher) applyGroup(h *Engine, group []*headRequest, p *preparedBatch, ids []signal.SeriesID) error {
	if err := h.refuseWrite(); err != nil {
		return err
	}
	h.mu.LockWriter()
	defer h.mu.UnlockWriter()
	if err := h.flushResidentHeadLocked(); err != nil {
		return err
	}
	if err := h.preloadPreparedDiskSeriesLocked(ids, &p.lookup); err != nil {
		return err
	}
	defer h.clearDiskPendingLocked()
	err := h.mu.WithReaders(func() error {
		positions := p.positions
		defer func() { p.positions = positions }()
		var offset int
		for _, r := range group {
			hi := offset + len(r.ids)
			p.positions = positions[offset:hi]
			r.result = h.stagePreparedRows(r.ids, r.ts, r.values, r.sf, r.materialize, AppendLimits{}, p)
			offset = hi
		}
		if p.batch.empty() {
			return nil
		}
		return h.encodeWriteBatch(p.batch, false)
	})
	if err != nil {
		return err
	}
	p.batch.apply(h.head)
	return nil
}

func appendHeadLane(h *Engine, prepared bool, ids []signal.SeriesID, ts []int64, values, sf []float64,
	materialize func(int) signal.Series) (AppendResult, error) {
	if prepared {
		if p := metricBatchPreparations.acquire(); p != nil {
			defer metricBatchPreparations.release(p)
			p.prepare(ids)
			return h.appendPrepared(ids, ts, values, sf, materialize, AppendLimits{}, p)
		}
	}
	return h.AppendBatch(ids, ts, values, sf, materialize, AppendLimits{})
}
