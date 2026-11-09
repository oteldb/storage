package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

const defaultResidentHeadBytes = 128 << 20
const diskHeadCacheEntries = 8192
const diskHeadBloomWords = 1 << 17

type diskHeadEntry struct {
	newest int64
	exists bool
}

type diskHeadState struct {
	count   int64
	bloom   []uint64
	cache   map[signal.SeriesID]diskHeadEntry
	order   []signal.SeriesID
	next    int
	pending map[signal.SeriesID]diskHeadEntry
}

func newDiskHeadState() *diskHeadState {
	return &diskHeadState{bloom: make([]uint64, diskHeadBloomWords), cache: make(map[signal.SeriesID]diskHeadEntry)}
}

func (s *diskHeadState) note(id signal.SeriesID) {
	for _, x := range []uint64{id.Lo, id.Hi, id.Lo ^ (id.Hi >> 17)} {
		bit := x % uint64(len(s.bloom)*64)
		s.bloom[bit/64] |= uint64(1) << (bit % 64)
	}
}

func (s *diskHeadState) mayContain(id signal.SeriesID) bool {
	for _, x := range []uint64{id.Lo, id.Hi, id.Lo ^ (id.Hi >> 17)} {
		bit := x % uint64(len(s.bloom)*64)
		if s.bloom[bit/64]&(uint64(1)<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

func (s *diskHeadState) remember(id signal.SeriesID, newest int64) {
	if _, ok := s.cache[id]; !ok {
		if len(s.order) < diskHeadCacheEntries {
			s.order = append(s.order, id)
		} else {
			delete(s.cache, s.order[s.next])
			s.order[s.next] = id
			s.next = (s.next + 1) % diskHeadCacheEntries
		}
	}
	s.cache[id] = diskHeadEntry{newest: newest, exists: true}
}

func (h *head) cardinality() int64 {
	if h.disk != nil {
		return h.disk.count
	}
	return int64(h.series.Len())
}

func (e *Engine) diskSeriesCountLocked() int64 { return e.head.cardinality() }

// rebuildDiskStateLocked recounts the streaming union after a catalog lifecycle change.
// The caller excludes writers and owns state; disk reads admit readers.
func (e *Engine) rebuildDiskStateLocked(ctx context.Context) error {
	if !e.cfg.DiskSeries {
		return nil
	}
	next := newDiskHeadState()
	headIDs := make([]signal.SeriesID, e.head.series.Len())
	view := e.head.series.View()
	for i := range headIDs {
		headIDs[i] = view.ID(i)
	}
	slices.SortFunc(headIDs, signal.SeriesID.Compare)
	parts := e.readablePartsLocked()
	if err := e.mu.WithReaders(func() error {
		its := []postings.Postings{postings.FromSlice(headIDs)}
		for _, p := range parts {
			it, err := p.diskSeriesIDs(ctx)
			if err != nil {
				return err
			}
			its = append(its, it)
		}
		it := postings.Merge(its...)
		for it.Next() {
			next.count++
			next.note(it.At())
		}
		return it.Err()
	}); err != nil {
		return err
	}
	e.head.disk = next
	return nil
}

// preloadDiskSeriesLocked resolves historical membership before staged admission.
func (e *Engine) preloadDiskSeriesLocked(ids []signal.SeriesID) error {
	if !e.cfg.DiskSeries {
		return nil
	}
	e.head.disk.pending = nil
	return e.preloadDiskSeriesMoreLocked(ids)
}

func (e *Engine) preloadOneDiskSeriesLocked(id signal.SeriesID) error {
	return e.preloadDiskSeriesMoreLocked([]signal.SeriesID{id})
}

func (e *Engine) preloadDiskSeriesMoreLocked(ids []signal.SeriesID) error {
	if !e.cfg.DiskSeries {
		return nil
	}
	return e.mu.WithReaders(func() error { return e.preloadDiskSeriesOffState(ids) })
}

//nolint:contextcheck,nolintlint // The context analyzer consumes this intentional API boundary before reporting.
func (e *Engine) preloadDiskSeriesOffState(ids []signal.SeriesID) error {
	s := e.head.disk
	if s.pending == nil {
		s.pending = make(map[signal.SeriesID]diskHeadEntry)
	}
	parts := e.readablePartsLocked()
	for _, id := range ids {
		if e.head.series.Has(id) {
			continue
		}
		if _, ok := s.pending[id]; ok {
			continue
		}
		if hit, ok := s.cache[id]; ok {
			s.pending[id] = hit
			continue
		}
		var entry diskHeadEntry
		if s.mayContain(id) {
			for _, p := range parts {
				meta, ok, err := p.diskSeriesMetadata(context.Background(), id)
				if err != nil {
					return errors.Wrap(err, "lookup historical series")
				}
				if ok {
					if !entry.exists || meta.Covered > entry.newest {
						entry.newest = meta.Covered
					}
					entry.exists = true
				}
			}
		}
		s.pending[id] = entry
	}
	return nil
}

func (e *Engine) clearDiskPendingLocked() {
	if e.head.disk != nil {
		e.head.disk.pending = nil
	}
}

// compactDiskHeadLocked drops metadata once its samples have immutable part ownership.
// The caller owns writer/state; disk-mode merges retain no head references.
func (e *Engine) compactDiskHeadLocked() {
	h := e.head
	if h.disk == nil || e.flushing != nil || len(h.samples) == h.series.Len() {
		return
	}
	next := newHead()
	view := h.series.View()
	var attrs signal.Attributes
	for i := range view.Len() {
		id := view.ID(i)
		newest, seen := h.watermark(id)
		if _, ok := h.samples[id]; !ok {
			if seen {
				h.disk.remember(id, newest)
			}
			continue
		}
		ident := view.At(i, attrs)
		attrs = ident.Attributes
		next.register(id, ident)
		if seen {
			ref, _ := next.series.Ref(id)
			next.slots.set(next.stateRefs[ref], newest)
		}
	}
	next.disk = h.disk
	h.sym, h.series, h.post = next.sym, next.series, next.post
	h.nonStringLabels, h.stateRefs, h.slots = next.nonStringLabels, next.stateRefs, next.slots
	h.resetBuffers()
	e.identityOrder.reset()
}

//nolint:contextcheck,nolintlint // The context analyzer consumes this intentional API boundary before reporting.
func (e *Engine) flushResidentHead() error {
	if !e.cfg.DiskSeries {
		return nil
	}
	e.mu.RLock()
	over := e.head.bytes > 0 && e.head.identityBytes()+e.head.inFlightBytes() >= e.cfg.ResidentHeadBytes
	e.mu.RUnlock()
	if over {
		return e.Flush(context.Background())
	}
	return nil
}

// flushResidentHeadLocked applies backpressure while preserving writer lock ownership.
//
//nolint:contextcheck,nolintlint // The context analyzer consumes this intentional API boundary before reporting.
func (e *Engine) flushResidentHeadLocked() error {
	if !e.cfg.DiskSeries {
		return nil
	}
	for e.head.bytes > 0 && e.head.identityBytes()+e.head.inFlightBytes() >= e.cfg.ResidentHeadBytes {
		e.mu.UnlockWriter()
		err := e.Flush(context.Background())
		e.mu.LockWriter()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) replayResidentGuard(samples int) error {
	if e.cfg.DiskSeries && e.head.identityBytes()+e.head.inFlightBytes()+int64(samples)*SampleBytes > e.cfg.ResidentHeadBytes {
		return errors.New("WAL replay exceeds resident head budget; journal preserved; increase ResidentHeadBytes to recover")
	}
	return nil
}

func (e *Engine) applyDiskReplicatedLocked(data []byte) error {
	defer e.walB.reset()
	byID := make(map[signal.SeriesID]signal.Series)
	admit := func(id signal.SeriesID, ts []int64, values, sf []float64) {
		if _, ok := byID[id]; !ok && !e.head.series.Has(id) {
			return
		}
		for i := range ts {
			weight := 1.0
			if sf != nil {
				weight = sf[i]
			}
			e.walB.admit(e.head, id, ts[i], values[i], weight, 0, AppendLimits{}, func() signal.Series { return byID[id] })
		}
	}
	if err := wal.Replay(data, wal.Handlers{
		OnSeries: func(id signal.SeriesID, s signal.Series) error {
			if err := e.preloadOneDiskSeriesLocked(id); err != nil {
				return err
			}
			byID[id] = s
			return nil
		},
		OnSamples: func(id signal.SeriesID, ts []int64, values []float64) error { admit(id, ts, values, nil); return nil },
		OnSamplesSF: func(id signal.SeriesID, ts []int64, values, sf []float64) error {
			admit(id, ts, values, sf)
			return nil
		},
	}); err != nil {
		return err
	}
	e.walB.apply(e.head)
	return nil
}

func (e *Engine) diskOverflowLimits(limits AppendLimits, b *walBatch, stateHeld bool) AppendLimits {
	if !e.cfg.DiskSeries || limits.Overflow == nil {
		return limits
	}
	overflow := limits.Overflow
	limits.Overflow = func(s signal.Series) signal.Series {
		ov := overflow(s)
		var err error
		if stateHeld {
			err = e.preloadOneDiskSeriesLocked(ov.Hash())
		} else {
			err = e.preloadDiskSeriesOffState([]signal.SeriesID{ov.Hash()})
		}
		if err != nil {
			b.lookupErr = err
		}
		return ov
	}
	return limits
}
