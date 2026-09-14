package engine

import (
	"bytes"
	"time"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

// walBatch is a logged write staged before it touches the head: its admitted samples grouped by the
// series they land under, with what admitting them would have done to the head. A durable write is
// decided into it ([walBatch.admit]), logged from it ([walBatch.flush]), and only then applied
// ([walBatch.apply]), so a write whose log fails leaves the head as it found it.
//
// Applying first — what [head.appendByID] does for an engine with nothing to log — would leave a
// failed log write's samples in the head and, for a series seen for the first time, its buffer: the
// retry would then see a buffered series, skip the identity record, and log samples replay drops.
//
// It groups samples so the engine logs one samples frame per series (and one series record per series
// starting a buffer) rather than one frame per sample, and it is reused across writes under the engine
// lock, keeping its buffers' capacity, so a steady-state durable append allocates nothing here.
type walBatch struct {
	order []signal.SeriesID       // series in first-admitted order; order[k] ↔ accs[k]
	pos   map[signal.SeriesID]int // id → its index in order/accs (for this write only)
	accs  []walSeriesAcc

	// samples is the admitted sample count and registered the new identities the write registers; both
	// are what [head.appendByID] would have added to the head's in-flight bytes and series count.
	samples    int
	registered int

	// first is when the write staged its first sample: the head's age starts there if the write is the
	// first to fill it, as it would had the sample been applied on admission, not once the log returns.
	first time.Time

	frames bytes.Buffer
	fw     *wal.Writer
}

type walSeriesAcc struct {
	isNew    bool          // the series starts a buffer, so its identity record is logged
	register bool          // the identity is not in the head's index yet
	series   signal.Series // the identity, set when isNew
	newest   int64         // the newest admitted timestamp
	ts       []int64
	values   []float64
	sf       []float64 // per-sample scale factors (aligned with ts)
	sampled  bool      // true once any sf != 1 — selects the sf-carrying WAL frame on flush
}

func newWALBatch() *walBatch {
	b := &walBatch{pos: make(map[signal.SeriesID]int)}
	b.fw = wal.NewWriter(&b.frames)

	return b
}

// admit decides one sample exactly as [head.appendByID] would, as if every sample admitted into b so
// far had already been applied, and stages it when admitted. The head is not touched.
func (b *walBatch) admit(
	h *head, id signal.SeriesID, ts int64, value, sf float64, oooWindow int64, limits AppendLimits,
	materialize func() signal.Series,
) admitOutcome {
	if oooWindow > 0 {
		newest, seen := h.seriesNewest[id]
		if k, ok := b.pos[id]; ok && (!seen || b.accs[k].newest > newest) {
			newest, seen = b.accs[k].newest, true
		}

		if seen && ts < newest-oooWindow {
			return rejectOOO
		}
	}

	if limits.MaxInFlightBytes > 0 && h.bytes+int64(b.samples)*SampleBytes >= limits.MaxInFlightBytes {
		return rejectBytes
	}

	if b.buffered(h, id) {
		b.add(id, ts, value, sf, false, false, signal.Series{})

		return admitted
	}

	if s, ok := h.series.Get(id); ok {
		b.add(id, ts, value, sf, true, false, s)

		return admitted
	}

	cardinality := int64(h.series.Len() + b.registered)
	if limits.MaxSeries > 0 && cardinality >= limits.MaxSeries {
		return rejectCardinality
	}

	if limits.Overflow != nil && limits.MaxSeriesSoft > 0 && cardinality >= limits.MaxSeriesSoft {
		ov := limits.Overflow(materialize())
		oid := ov.Hash()

		switch {
		case b.buffered(h, oid):
			b.add(oid, ts, value, sf, false, false, signal.Series{})
		case h.series.Has(oid):
			b.add(oid, ts, value, sf, true, false, ov)
		default:
			b.add(oid, ts, value, sf, true, true, ov)
		}

		return admittedOverflow
	}

	b.add(id, ts, value, sf, true, true, materialize())

	return admitted
}

// buffered reports whether series id has a sample buffer once b is applied.
func (b *walBatch) buffered(h *head, id signal.SeriesID) bool {
	if h.samples[id] != nil {
		return true
	}

	_, ok := b.pos[id]

	return ok
}

// add stages one admitted sample for series id with its lossy-sampling weight sf (1 when unsampled).
// isNew, register and s describe the series' first sample in b and are ignored on later ones.
func (b *walBatch) add(id signal.SeriesID, ts int64, value, sf float64, isNew, register bool, s signal.Series) {
	k, ok := b.pos[id]
	if !ok {
		k = len(b.order)
		b.order = append(b.order, id)
		b.pos[id] = k

		acc := walSeriesAcc{isNew: isNew, register: register, series: s, newest: ts}
		if k < len(b.accs) { // reuse a prior write's accumulator (and its slice capacity)
			acc.ts, acc.values, acc.sf = b.accs[k].ts[:0], b.accs[k].values[:0], b.accs[k].sf[:0]
			b.accs[k] = acc
		} else {
			b.accs = append(b.accs, acc)
		}

		if register {
			b.registered++
		}
	}

	if b.samples == 0 {
		b.first = time.Now()
	}

	a := &b.accs[k]
	a.ts = append(a.ts, ts)
	a.values = append(a.values, value)
	a.sf = append(a.sf, sf)
	a.newest = max(a.newest, ts)

	if sf != 1 {
		a.sampled = true
	}

	b.samples++
}

// empty reports whether the batch staged no samples (so the engine can skip the WAL write).
func (b *walBatch) empty() bool { return len(b.order) == 0 }

// flush logs the staged write — a series record per series starting a buffer, a samples frame per
// series — as a single WAL write, so a write that fails logs none of it. It does not reset b.
func (b *walBatch) flush(h *head, w *wal.SegmentWriter) error {
	if err := b.encode(h, false); err != nil {
		return err
	}

	return w.WriteFrames(b.frames.Bytes())
}

// encode frames the staged write into b.frames, one samples frame per series, each preceded by its
// series record when the series starts a buffer — or always, with everySeries, for frames read by a
// replica whose head may not hold the identity the primary's does.
func (b *walBatch) encode(h *head, everySeries bool) error {
	b.frames.Reset()

	for k, id := range b.order {
		a := &b.accs[k]

		switch {
		case a.isNew:
			if err := b.fw.WriteSeries(id, a.series); err != nil {
				return err
			}
		case everySeries:
			s, _ := h.series.Get(id) // a buffered series is registered
			if err := b.fw.WriteSeries(id, s); err != nil {
				return err
			}
		}

		// Only spend the per-sample sf bytes when sampling actually weighted this series; the common
		// unsampled path stays on the original (no-sf) samples frame.
		if a.sampled {
			if err := b.fw.WriteSamplesSF(id, a.ts, a.values, a.sf); err != nil {
				return err
			}

			continue
		}

		if err := b.fw.WriteSamples(id, a.ts, a.values); err != nil {
			return err
		}
	}

	return nil
}

// apply puts the staged samples into the head, leaving it exactly as applying each one through
// [head.appendByID] in admission order would have.
func (b *walBatch) apply(h *head) {
	wasEmpty := h.bytes == 0

	for k, id := range b.order {
		a := &b.accs[k]

		if a.register && !h.series.Has(id) {
			h.register(id, a.series)
		}

		buf := h.bufFor(id)
		for i := range a.ts {
			buf.appendSample(a.ts[i], a.values[i], a.sf[i])
		}

		h.grow(int64(len(a.ts)) * SampleBytes)
		h.noteTS(id, a.newest)
	}

	if wasEmpty && h.bytes > 0 {
		h.since = b.first
	}
}

func (b *walBatch) reset() {
	b.order = b.order[:0]
	b.samples, b.registered = 0, 0
	clear(b.pos)
}
