package engine

import (
	"bytes"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

const (
	minPreparedBatchSamples = 1024
	maxPreparedBatchSamples = 4096
	maxPreparedBatches      = 8
	maxPreparedFrameBytes   = 4 << 20
)

var metricBatchPreparations = newBatchPreparations()

type batchPreparations struct {
	active chan struct{}
	free   chan *preparedBatch
}

func newBatchPreparations() *batchPreparations {
	return &batchPreparations{
		active: make(chan struct{}, maxPreparedBatches),
		free:   make(chan *preparedBatch, maxPreparedBatches),
	}
}

func (p *batchPreparations) acquire() *preparedBatch {
	select {
	case p.active <- struct{}{}:
	default:
		return nil
	}

	select {
	case b := <-p.free:
		return b
	default:
		b := &preparedBatch{batch: newWALBatch()}
		return b
	}
}

func (p *batchPreparations) release(b *preparedBatch) {
	b.memory.release()
	b.batch.reset()
	// An identity can be arbitrarily large; retain only bounded numeric/frame scratch.
	if b.batch.frames.Cap() > maxPreparedFrameBytes {
		b.batch.frames = bytes.Buffer{}
		b.batch.fw = wal.NewWriter(&b.batch.frames)
	}
	b.batch.frames.Reset()
	clear(b.batch.accs[:cap(b.batch.accs)])
	clear(b.snapshots[:cap(b.snapshots)])
	clear(b.next[:cap(b.next)])
	clear(b.created[:cap(b.created)])
	b.warm = false
	b.commit = preparedCommit{}
	p.free <- b
	<-p.active
}

type preparedGroup struct {
	offset int
	count  int
	staged int
}

type preparedBatch struct {
	memory    sampleArena
	batch     *walBatch
	positions []int
	groups    []preparedGroup
	ts        []int64
	values    []float64
	sf        []float64
	current   int
	warm      bool
	snapshots []headSeriesState
	next      []sampleBuf
	created   []*sampleBuf
	refs      []uint32
	masks     []reservationMask
	maskOrder []uint32
	commit    preparedCommit
}

func (p *preparedBatch) prepare(ids []signal.SeriesID) {
	p.positions = resize(p.positions, len(ids))
	p.groups = p.groups[:0]
	for i, id := range ids {
		k, ok := p.batch.pos[id]
		if !ok {
			k = len(p.groups)
			p.batch.pos[id] = k
			p.groups = append(p.groups, preparedGroup{staged: -1})
		}
		p.positions[i] = k
		p.groups[k].count++
	}
	var offset int
	for i := range p.groups {
		p.groups[i].offset = offset
		offset += p.groups[i].count
	}
	p.ts = resize(p.ts, len(ids))
	p.values = resize(p.values, len(ids))
	p.sf = resize(p.sf, len(ids))
	p.batch.order = resize(p.batch.order, len(p.groups))[:0]
	p.batch.accs = resize(p.batch.accs, len(p.groups))
	p.batch.frames.Grow(len(ids)*32 + len(p.groups)*64)
}

func resize[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}
	return s[:n]
}

func (e *Engine) appendPrepared(
	ids []signal.SeriesID, ts []int64, values, sf []float64, materialize func(i int) signal.Series, limits AppendLimits,
	p *preparedBatch,
) (AppendResult, error) {
	if e.parallel != nil {
		if res, handled, err := e.appendKnownPrepared(ids, ts, values, sf, materialize, limits, p); handled {
			return res, err
		}
		p.batch.reset()
		p.warm = false
		clear(p.next)
		clear(p.created)
		p.prepare(ids)
	}
	e.mu.LockWriter()
	defer e.mu.UnlockWriter()
	return e.appendPreparedLocked(ids, ts, values, sf, materialize, limits, p)
}

func (e *Engine) appendPreparedLocked(
	ids []signal.SeriesID, ts []int64, values, sf []float64, materialize func(i int) signal.Series, limits AppendLimits,
	p *preparedBatch,
) (AppendResult, error) {
	if err := e.flushResidentHeadLocked(); err != nil {
		return AppendResult{}, err
	}
	if err := e.preloadDiskSeriesLocked(ids); err != nil {
		return AppendResult{}, err
	}
	defer e.clearDiskPendingLocked()
	var res AppendResult
	err := e.mu.WithReaders(func() error {
		var err error
		res, err = e.stagePreparedBatch(ids, ts, values, sf, materialize, limits, p)
		return err
	})
	if err != nil {
		return AppendResult{}, err
	}
	p.batch.apply(e.head)
	return res, nil
}

func (e *Engine) stagePreparedBatch(
	ids []signal.SeriesID, ts []int64, values, sf []float64, materialize func(i int) signal.Series, limits AppendLimits,
	p *preparedBatch,
) (AppendResult, error) {
	res := e.stagePreparedRows(ids, ts, values, sf, materialize, limits, p)
	if p.batch.empty() {
		return res, nil
	}
	if err := e.encodeWriteBatch(p.batch, false); err != nil {
		return AppendResult{}, err
	}
	return res, nil
}

func (e *Engine) stagePreparedRows(
	ids []signal.SeriesID, ts []int64, values, sf []float64, materialize func(i int) signal.Series,
	limits AppendLimits, p *preparedBatch,
) AppendResult {
	var bi int
	mat := func() signal.Series { return materialize(bi) }
	var res AppendResult
	for i, id := range ids {
		bi = i
		w := float64(1)
		if sf != nil {
			w = sf[i]
		}
		p.current = p.positions[i]
		res = res.with(p.admit(e.head, id, ts[i], values[i], w, e.cfg.OOOWindow, limits, mat))
	}
	return res
}

func (p *preparedBatch) admit(
	h *head, id signal.SeriesID, ts int64, value, sf float64, oooWindow int64, limits AppendLimits,
	materialize func() signal.Series,
) admitOutcome {
	if limits.EnforceMinTimestamp && ts < limits.MinTimestamp {
		return rejectRetention
	}
	g := &p.groups[p.current]
	b := p.batch
	k := g.staged
	var state headSeriesState
	if k < 0 && oooWindow > 0 {
		state = p.seriesState(h, id)
	}
	var newest int64
	var seen bool
	if oooWindow > 0 {
		if k >= 0 {
			newest, seen = b.accs[k].admissionNewest(), true
		} else {
			newest, seen = state.newest, state.seen
		}
		if seen && ts < newest-oooWindow {
			return rejectOOO
		}
	}
	if limits.MaxInFlightBytes > 0 && h.inFlightBytes()+int64(b.samples)*SampleBytes >= limits.MaxInFlightBytes {
		return rejectBytes
	}
	if k >= 0 {
		b.add(k, ts, value, sf)
		return admitted
	}
	if oooWindow <= 0 {
		state = p.seriesState(h, id)
		newest, seen = state.newest, state.seen
	}
	acc := walSeriesAcc{buffer: state.buffer, priorNewest: newest, hadNewest: seen,
		identityRef: state.ref, knownRef: state.known, historical: state.historical}
	if acc.buffer == nil {
		acc.isNew = true
		if !acc.knownRef {
			if !state.historical && limits.MaxSeries > 0 && h.cardinality()+int64(b.registered) >= limits.MaxSeries {
				return rejectCardinality
			}
			acc.register, acc.series = true, materialize()
		}
	}
	k = len(b.order)
	b.order = append(b.order, id)
	g.staged = k
	end := g.offset + g.count
	acc.ts = p.ts[g.offset:g.offset:end]
	acc.values = p.values[g.offset:g.offset:end]
	acc.sf = p.sf[g.offset:g.offset:end]
	b.accs[k] = acc
	if acc.register && !acc.historical {
		b.registered++
	}
	b.relog = b.relog || (acc.isNew && !acc.register)
	b.add(k, ts, value, sf)
	return admitted
}

func (p *preparedBatch) seriesState(h *head, id signal.SeriesID) headSeriesState {
	if p.warm {
		return p.snapshots[p.current]
	}
	return h.seriesState(id)
}
