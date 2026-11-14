package engine

import (
	"encoding/binary"
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
	capturing     func()
	ordered       func([]signal.SeriesID)
	owned         func()
	prepared      func()
	beforeWAL     func()
	beforePublish func()
	queued        func()
	fallback      func()
}

type parallelHead struct {
	ownership     seriesOwnership
	admission     sync.Mutex
	pendingSeries atomic.Int64
	commits       commitQueue
	walMu         sync.Mutex
	pending       atomic.Int64
	preparations  *batchPreparations
	hooks         *parallelHeadHooks
}

func (e *Engine) appendKnownPrepared(
	ids []signal.SeriesID, ts []int64, values, sf []float64, materialize func(int) signal.Series,
	limits AppendLimits, p *preparedBatch,
) (AppendResult, bool, error) {
	lookupErr := e.captureParallelDiskSeries(ids, p)
	e.mu.RLockWriter()
	defer e.mu.RUnlockWriter()
	ph := e.parallel
	p.prepareOwnership()
	var conflict func()
	if ph.hooks != nil {
		conflict = ph.hooks.fallback
	}
	ph.ownership.acquire(p, conflict)
	defer ph.ownership.release(p)
	if ph.hooks != nil && ph.hooks.owned != nil {
		ph.hooks.owned()
	}
	if err := e.refuseWrite(); err != nil {
		return AppendResult{}, true, err
	}

	view, valid, err := e.parallelSnapshots(p, lookupErr)
	if !valid || err != nil {
		return AppendResult{}, valid, err
	}
	ph.admission.Lock()
	e.mu.RLock()
	if e.cfg.DiskSeries && e.head.bytes > 0 &&
		e.head.residentIdentityBytes()+e.head.inFlightBytes()+ph.pending.Load() >= e.cfg.ResidentHeadBytes {
		e.mu.RUnlock()
		ph.admission.Unlock()
		return AppendResult{}, false, nil
	}
	p.otherPendingBytes, p.otherPendingSeries = ph.pending.Load(), ph.pendingSeries.Load()
	p.warm = true
	res := e.stagePreparedRows(ids, ts, values, sf, func(i int) signal.Series {
		p.materializeAt[p.positions[i]] = i
		return signal.Series{}
	}, limits, p)
	bytes := int64(p.batch.samples) * SampleBytes
	registered := int64(p.batch.registered)
	ph.pending.Add(bytes)
	ph.pendingSeries.Add(registered)
	e.mu.RUnlock()
	ph.admission.Unlock()
	if res.RejectedBytes > 0 && ph.hooks != nil && ph.hooks.fallback != nil {
		ph.hooks.fallback()
	}
	if p.batch.empty() {
		return res, true, nil
	}
	reserved := true
	defer func() {
		if reserved {
			ph.pending.Add(-bytes)
			ph.pendingSeries.Add(-registered)
		}
	}()
	for group, i := range p.materializeAt {
		if i >= 0 {
			p.batch.accs[p.groups[group].staged].series = materialize(i)
		}
	}
	if !p.parallelWorkFits(view) {
		return AppendResult{}, false, nil
	}
	p.prepareTails()
	if err := p.batch.encode(view, false); err != nil {
		return AppendResult{}, true, err
	}
	if ph.hooks != nil && ph.hooks.prepared != nil {
		ph.hooks.prepared()
	}
	if err := e.commitPrepared(p); err != nil {
		return AppendResult{}, true, err
	}
	reserved = false
	return res, true, nil
}

func (e *Engine) captureParallelDiskSeries(ids []signal.SeriesID, p *preparedBatch) error {
	if !e.cfg.DiskSeries {
		return nil
	}
	e.mu.RLockWriter()
	if hooks := e.parallel.hooks; hooks != nil && hooks.capturing != nil {
		hooks.capturing()
	}
	e.mu.RLock()
	if err := e.checkDiskAdmissionLocked(); err != nil {
		e.mu.RUnlock()
		e.mu.RUnlockWriter()
		return err
	}
	p.lookup.capture(e, ids)
	e.mu.RUnlock()
	e.mu.RUnlockWriter()
	if len(p.lookup.historical) == 0 {
		return nil
	}
	defer func() {
		for _, part := range p.lookup.parts {
			part.release()
		}
	}()
	slices.SortFunc(p.lookup.historical, signal.SeriesID.Compare)
	return lookupDiskSeries(p.lookup.parts, p.lookup.historical, p.lookup.pending)
}

func (e *Engine) parallelSnapshots(p *preparedBatch, lookupErr error) (series.View, bool, error) {
	e.mu.RLock()
	valid := !e.cfg.DiskSeries || p.lookup.valid(e)
	e.mu.RUnlock()
	if !valid {
		return series.View{}, false, nil
	}
	if lookupErr != nil {
		return series.View{}, true, lookupErr
	}
	if len(p.lookup.historical) > 0 {
		// A successful lookup remains useful even if this batch's WAL later fails.
		e.mu.LockState()
		for _, id := range p.lookup.historical {
			if entry := p.lookup.pending[id]; entry.exists {
				e.head.disk.remember(id, entry.newest)
			}
		}
		e.mu.UnlockState()
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkDiskAdmissionLocked(); err != nil {
		return series.View{}, true, err
	}
	p.snapshots = resize(p.snapshots, len(p.groups))
	for i, id := range p.ownedIDs {
		ref, known := e.head.series.Ref(id)
		state := headSeriesState{ref: ref, known: known}
		if known {
			slot := e.head.stateRefs[ref]
			state.buffer = e.head.buffers.get(slot)
			state.newest, state.seen = e.head.slots.get(slot)
		} else if e.cfg.DiskSeries {
			entry := p.lookup.pending[id]
			state.historical, state.seen, state.newest = entry.exists, entry.exists, entry.newest
		}
		p.snapshots[i] = state
	}
	return e.head.series.View(), true, nil
}

func (p *preparedBatch) parallelWorkFits(view series.View) bool {
	work := buildbudget.Add(1024, int64(p.batch.samples)*192)
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
		if a.register {
			n := buildbudget.Attributes(a.series.Attributes)
			n = buildbudget.Add(n, buildbudget.Attributes(a.series.Resource.Attributes))
			n = buildbudget.Add(n, buildbudget.Attributes(a.series.Scope.Attributes))
			n = buildbudget.Add(n, int64(len(a.series.Resource.SchemaURL)+len(a.series.Scope.SchemaURL)))
			n = buildbudget.Add(n, int64(len(a.series.Scope.Name)+len(a.series.Scope.Version)))
			work = buildbudget.Add(work, n)
			work = buildbudget.Add(work, walIdentityWork(buildbudget.SeriesEncoding(a.series)))
		} else if a.isNew {
			work = buildbudget.Add(work, view.MaterializationBytes(int(a.identityRef)))
			resource, scope := view.Context(int(a.identityRef))
			encoded := buildbudget.SeriesEncoding(signal.Series{Resource: resource, Scope: scope})
			view.EncodedAttributes(int(a.identityRef), func(key, value []byte) {
				encoded = buildbudget.Add(encoded, binary.MaxVarintLen64+int64(len(key)))
				encoded = buildbudget.Add(encoded, int64(len(value)))
			})
			work = buildbudget.Add(work, walIdentityWork(encoded))
		}
		if work > maxParallelHeadWork {
			return false
		}
	}
	return true
}

func walIdentityWork(encoded int64) int64 {
	// Payload, frame and accumulated frames can each retain up to twice their encoded size.
	return buildbudget.Mul(buildbudget.Add(encoded, 32), 6)
}

func (p *preparedBatch) prepareTails() {
	n := len(p.batch.order)
	p.next = resize(p.next, n)
	p.created = resize(p.created, n)
	clear(p.created)
	var freshHeaders, freshSamples int
	for i := range p.batch.order {
		a := &p.batch.accs[i]
		if a.buffer == nil {
			freshHeaders++
		}
		if a.buffer == nil || cap(a.buffer.ts) == 0 && cap(a.buffer.values) == 0 {
			freshSamples += len(a.ts)
		}
	}
	var compactHeaders []sampleBuf
	if freshHeaders < sampleHeaderPageSize {
		compactHeaders = make([]sampleBuf, freshHeaders)
	}
	var compactTS []int64
	var compactValues []float64
	if freshSamples < sampleNumericPageSize {
		compactTS, compactValues = make([]int64, freshSamples), make([]float64, freshSamples)
	}
	for i := range p.batch.order {
		a := &p.batch.accs[i]
		next := sampleBuf{}
		if a.buffer != nil {
			next = *a.buffer
		}
		prior := len(next.ts)
		if cap(next.ts) == 0 && cap(next.values) == 0 && compactTS != nil {
			count := len(a.ts)
			next.ts, next.values = compactTS[:0:count], compactValues[:0:count]
			compactTS, compactValues = compactTS[count:], compactValues[count:]
		} else {
			p.memory.initColumns(&next, prior+len(a.ts))
		}
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
			if len(compactHeaders) > 0 {
				p.created[i], compactHeaders = &compactHeaders[0], compactHeaders[1:]
			} else {
				p.created[i] = p.memory.buffer()
			}
			*p.created[i] = next
		}
	}
}

func (p *preparedBatch) publishTails(h *head) {
	if h.disk != nil {
		prior := h.disk.pending
		h.disk.pending = p.lookup.pending
		defer func() { h.disk.pending = prior }()
	}
	h.memory.adopt(&p.memory)
	minTS := int64(math.MaxInt64)
	for i, id := range p.batch.order {
		a := &p.batch.accs[i]
		if a.register && !h.series.Has(id) {
			h.register(series.Entry{ID: id, Series: a.series})
		}
		if !a.knownRef {
			a.identityRef, a.knownRef = h.series.Ref(id)
		}
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
