package engine

import (
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// walBatch groups an [Engine.AppendBatch]'s accepted samples by series so the engine logs one
// WriteSamples frame per series (and one WriteSeries per newly-seen series) instead of one frame — a
// write plus, under WALSyncAlways, an fsync — per sample, all under the engine lock. It is reused
// across batches (guarded by the engine lock) and keeps its buffers' capacity, so a steady-state
// durable append allocates nothing here.
type walBatch struct {
	order []signal.SeriesID       // series in first-seen order; order[k] ↔ accs[k]
	pos   map[signal.SeriesID]int // id → its index in order/accs (for this batch only)
	accs  []walSeriesAcc
}

type walSeriesAcc struct {
	isNew   bool
	series  signal.Series // the materialized identity, set only when isNew
	ts      []int64
	values  []float64
	sf      []float64 // per-sample scale factors (aligned with ts)
	sampled bool      // true once any sf != 1 — selects the sf-carrying WAL frame on flush
}

func newWALBatch() *walBatch { return &walBatch{pos: make(map[signal.SeriesID]int)} }

// add records one accepted sample for series id with its lossy-sampling weight sf (1 when unsampled).
// isNew/s are taken from the head append and used only on the series' first sight in this batch (to
// log its identity).
func (b *walBatch) add(id signal.SeriesID, ts int64, value, sf float64, isNew bool, s signal.Series) {
	k, ok := b.pos[id]
	if !ok {
		k = len(b.order)
		b.order = append(b.order, id)
		b.pos[id] = k

		if k < len(b.accs) { // reuse a prior batch's accumulator (and its slice capacity)
			b.accs[k] = walSeriesAcc{isNew: isNew, series: s, ts: b.accs[k].ts[:0], values: b.accs[k].values[:0], sf: b.accs[k].sf[:0]}
		} else {
			b.accs = append(b.accs, walSeriesAcc{isNew: isNew, series: s})
		}
	}

	b.accs[k].ts = append(b.accs[k].ts, ts)
	b.accs[k].values = append(b.accs[k].values, value)
	b.accs[k].sf = append(b.accs[k].sf, sf)

	if sf != 1 {
		b.accs[k].sampled = true
	}
}

// empty reports whether the batch buffered no samples (so the engine can skip the WAL write).
func (b *walBatch) empty() bool { return len(b.order) == 0 }

// flush writes the grouped frames — one WriteSeries per new series, one WriteSamples per series — then
// resets the batch for reuse. The caller holds the engine lock.
func (b *walBatch) flush(w *wal.SegmentWriter) error {
	defer b.reset()

	for k, id := range b.order {
		a := &b.accs[k]

		if a.isNew {
			if err := w.WriteSeries(id, a.series); err != nil {
				return err
			}
		}

		// Only spend the per-sample sf bytes when sampling actually weighted this series; the common
		// unsampled path stays on the original (no-sf) samples frame.
		if a.sampled {
			if err := w.WriteSamplesSF(id, a.ts, a.values, a.sf); err != nil {
				return err
			}

			continue
		}

		if err := w.WriteSamples(id, a.ts, a.values); err != nil {
			return err
		}
	}

	return nil
}

func (b *walBatch) reset() {
	b.order = b.order[:0]
	clear(b.pos)
}
