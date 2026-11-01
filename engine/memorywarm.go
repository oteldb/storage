package engine

import (
	"github.com/oteldb/storage/signal"
)

type memoryWarmHooks struct {
	reserved  func([]signal.SeriesID)
	published func([]signal.SeriesID)
}

func (e *Engine) memoryPublicationEnabled() bool {
	e.viewMu.Lock()
	enabled := e.readView != nil
	e.viewMu.Unlock()
	return enabled
}

func (e *Engine) appendMemoryWarm(
	ids []signal.SeriesID, ts []int64, values, sf []float64,
	materialize func(int) signal.Series, limits AppendLimits,
) (AppendResult, bool, error) {
	if e.cfg.WAL == nil || len(ids) == 0 || len(ids) > maxPreparedBatchSamples ||
		limits.MaxInFlightBytes > 0 || limits.Overflow != nil && limits.MaxSeriesSoft > 0 {
		return AppendResult{}, false, nil
	}
	preparations := e.warmPreparations
	if preparations == nil {
		preparations = metricBatchPreparations
	}
	var p *preparedBatch
	if e.memoryLife != nil {
		p = preparations.acquireUntil(e.memoryLife.ctx.Done())
	} else {
		p = preparations.acquire()
	}
	if p == nil {
		if e.memoryLife != nil {
			return AppendResult{}, true, errMemoryClosed
		}
		return AppendResult{}, false, nil
	}
	defer preparations.release(p)
	if err := e.awaitMemoryCapacity(); err != nil {
		return AppendResult{}, true, err
	}
	p.prepare(ids)
	e.mu.RLockWriter()
	defer e.mu.RUnlockWriter()
	for i, id := range ids {
		group := &p.groups[p.positions[i]]
		if group.known {
			continue
		}
		ref, known := e.head.series.Ref(id)
		if !known {
			return AppendResult{}, false, nil
		}
		group.ref, group.known = ref, true
	}
	if p.reservation == nil {
		p.reservation = newSeriesReservation(ids)
	} else {
		p.reservation.prepare(ids)
	}
	reservation := p.reservation
	var done <-chan struct{}
	var cancellation func() error
	if e.memoryLife != nil {
		done, cancellation = e.memoryLife.ctx.Done(), e.memoryLife.ctx.Err
	}
	if err := e.reservations.acquireUntil(reservation, done, cancellation); err != nil {
		return AppendResult{}, true, err
	}
	defer e.reservations.release(reservation)
	if hooks := e.warmHooks; hooks != nil && hooks.reserved != nil {
		hooks.reserved(ids)
	}
	if err := e.refuseWrite(); err != nil {
		return AppendResult{}, true, err
	}
	p.batch.warm = true
	var result AppendResult
	for i, id := range ids {
		weight := float64(1)
		if sf != nil {
			weight = sf[i]
		}
		p.current = p.positions[i]
		result = result.with(p.admit(e.head, id, ts[i], values[i], weight, e.cfg.OOOWindow, limits,
			func() signal.Series { return materialize(i) }))
	}
	if p.batch.empty() {
		return result, true, nil
	}
	p.sortedAccs = resize(p.sortedAccs, len(reservation.ids))[:0]
	for _, id := range reservation.ids {
		if index := p.groups[p.batch.pos[id]].staged; index >= 0 {
			p.sortedAccs = append(p.sortedAccs, index)
		}
	}
	p.batch.sortedAccs, p.batch.prepared = p.sortedAccs, p
	if err := e.encodeWriteBatch(p.batch, false); err != nil {
		return AppendResult{}, true, err
	}
	e.applyLoggedBatch(p.batch)
	return result, true, nil
}
