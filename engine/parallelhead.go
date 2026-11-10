package engine

import (
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/signal"
)

const maxParallelHeadWork = 32 << 20

type parallelHeadHooks struct {
	owned         func()
	beforeWAL     func()
	beforePublish func()
	queued        func()
	fallback      func()
}

type parallelHead struct {
	reservations publicationReservations
	commits      commitQueue
	walMu        sync.Mutex
	pending      atomic.Int64
	hooks        *parallelHeadHooks
}

func (e *Engine) appendKnownPrepared(
	ids []signal.SeriesID, ts []int64, values, sf []float64, materialize func(int) signal.Series,
	limits AppendLimits, p *preparedBatch,
) (AppendResult, bool, error) {
	e.mu.RLockWriter()
	defer e.mu.RUnlockWriter()
	p.snapshots = resize(p.snapshots, len(p.groups))
	clear(p.snapshots)
	p.refs = p.refs[:0]
	for i, id := range ids {
		group := p.positions[i]
		if p.snapshots[group].known {
			continue
		}
		ref, known := e.head.series.Ref(id)
		if !known {
			return AppendResult{}, false, nil
		}
		p.snapshots[group] = headSeriesState{ref: ref, known: true}
		p.refs = append(p.refs, ref)
	}
	p.prepareReservation()
	ph := e.parallel
	if !ph.reservations.acquire(p) {
		if ph.hooks != nil && ph.hooks.fallback != nil {
			ph.hooks.fallback()
		}
		return AppendResult{}, false, nil
	}
	defer ph.reservations.release(p, len(p.masks))
	if ph.hooks != nil && ph.hooks.owned != nil {
		ph.hooks.owned()
	}
	if err := e.refuseWrite(); err != nil {
		return AppendResult{}, true, err
	}
	admissionErr := func() error {
		e.mu.RLock()
		defer e.mu.RUnlock()
		if err := e.checkDiskAdmissionLocked(); err != nil {
			return err
		}
		for i := range p.snapshots {
			s := &p.snapshots[i]
			slot := e.head.stateRefs[s.ref]
			s.buffer = e.head.buffers.get(slot)
			s.newest, s.seen = e.head.slots.get(slot)
		}
		return nil
	}()
	if admissionErr != nil {
		return AppendResult{}, true, admissionErr
	}
	p.warm = true
	stageLimits := limits
	stageLimits.MaxInFlightBytes = 0
	res := e.stagePreparedRows(ids, ts, values, sf, materialize, stageLimits, p)
	if p.batch.empty() {
		return res, true, nil
	}
	view := e.head.series.View()
	if !p.parallelWorkFits(view) {
		return AppendResult{}, false, nil
	}
	bytes := int64(p.batch.samples) * SampleBytes
	reserved := e.reserveParallelBytes(bytes, limits.MaxInFlightBytes)
	if !reserved {
		if ph.hooks != nil && ph.hooks.fallback != nil {
			ph.hooks.fallback()
		}
		return AppendResult{}, false, nil
	}
	defer func() {
		if reserved {
			ph.pending.Add(-bytes)
		}
	}()

	p.prepareTails()
	if err := p.batch.encode(p.batch.encodingView(e.head, false), false); err != nil {
		return AppendResult{}, true, err
	}
	if err := e.commitPrepared(p); err != nil {
		return AppendResult{}, true, err
	}
	reserved = false
	return res, true, nil
}

func (p *preparedBatch) parallelWorkFits(view series.View) bool {
	work := int64(p.batch.samples) * 32
	for i := range p.batch.order {
		a := &p.batch.accs[i]
		old := sampleBuf{}
		if a.buffer != nil {
			old = *a.buffer
		}
		needed := len(old.ts) + len(a.ts)
		if needed > cap(old.ts) {
			work = buildbudget.Add(work, buildbudget.Mul(int64(needed), 16))
		}
		if needed > cap(old.values) {
			work = buildbudget.Add(work, buildbudget.Mul(int64(needed), 16))
		}
		if (old.sf != nil || a.sampled) && needed > cap(old.sf) {
			work = buildbudget.Add(work, buildbudget.Mul(int64(needed), 16))
		}
		if a.isNew {
			work = buildbudget.Add(work, view.IdentityWorkspace(int(a.identityRef)))
			work = buildbudget.Add(work, view.MaterializationBytes(int(a.identityRef)))
			work = buildbudget.Add(work, view.EncodingBytes(int(a.identityRef)))
		}
		if work > maxParallelHeadWork {
			return false
		}
	}
	return true
}

func (p *preparedBatch) prepareTails() {
	n := len(p.batch.order)
	p.next = resize(p.next, n)
	p.created = resize(p.created, n)
	clear(p.created)
	for i := range p.batch.order {
		a := &p.batch.accs[i]
		next := sampleBuf{}
		if a.buffer != nil {
			next = *a.buffer
		}
		prior := len(next.ts)
		p.memory.initColumns(&next, prior+len(a.ts))
		next.ts = append(next.ts, a.ts...)
		next.values = append(next.values, a.values...)
		if next.sf != nil {
			next.sf = append(next.sf, a.sf...)
		} else if a.sampled {
			next.sf = make([]float64, prior+len(a.ts))
			for j := range prior {
				next.sf[j] = 1
			}
			copy(next.sf[prior:], a.sf)
		}
		p.next[i] = next
		if a.buffer == nil {
			p.created[i] = p.memory.buffer()
			*p.created[i] = next
		}
	}
}

func (p *preparedBatch) publishTails(h *head) {
	h.memory.adopt(&p.memory)
	minTS := int64(math.MaxInt64)
	for i, id := range p.batch.order {
		a := &p.batch.accs[i]
		if a.buffer == nil {
			h.setBuffer(id, a.identityRef, p.created[i])
		} else {
			*a.buffer = p.next[i]
		}
		if !a.hadNewest || a.newest > a.priorNewest {
			h.slots.set(h.stateRefs[a.identityRef], a.newest)
		}
		h.newest = max(h.newest, a.newest)
		minTS = min(minTS, slices.Min(a.ts))
	}
	h.noteBufferedTS(minTS)
	h.grow(int64(p.batch.samples) * SampleBytes)
	if h.since.IsZero() || p.batch.first.Before(h.since) {
		h.since = p.batch.first
	}
}

func (e *Engine) reserveParallelBytes(bytes, limit int64) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	used := e.head.inFlightBytes()
	for {
		pending := e.parallel.pending.Load()
		if limit > 0 && (used >= limit || pending > limit-used || bytes > limit-used-pending) {
			return false
		}
		if e.parallel.pending.CompareAndSwap(pending, pending+bytes) {
			return true
		}
	}
}
