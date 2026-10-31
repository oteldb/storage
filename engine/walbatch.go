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
	isNew       bool          // the series starts a buffer, so its identity record is logged
	register    bool          // the identity is not in the head's index yet
	series      signal.Series // the identity, set when isNew
	attrs       signal.Attributes
	newest      int64 // the newest admitted timestamp
	buffer      *sampleBuf
	priorNewest int64
	hadNewest   bool
	ts          []int64
	values      []float64
	sf          []float64 // per-sample scale factors (aligned with ts)
	sampled     bool      // true once any sf != 1 — selects the sf-carrying WAL frame on flush
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
	k, staged := b.pos[id]
	var newest int64
	var seen bool
	if oooWindow > 0 {
		if staged {
			newest, seen = b.accs[k].admissionNewest(), true
		} else {
			newest, seen = h.seriesNewest[id]
		}

		if seen && ts < newest-oooWindow {
			return rejectOOO
		}
	}

	if limits.MaxInFlightBytes > 0 && h.inFlightBytes()+int64(b.samples)*SampleBytes >= limits.MaxInFlightBytes {
		return rejectBytes
	}

	if staged {
		b.add(k, ts, value, sf)
		return admitted
	}

	if oooWindow <= 0 {
		newest, seen = h.seriesNewest[id]
	}
	acc := walSeriesAcc{buffer: h.samples[id], priorNewest: newest, hadNewest: seen}
	if acc.buffer != nil {
		b.add(b.start(id, acc), ts, value, sf)
		return admitted
	}
	acc.isNew = true
	if h.series.Has(id) {
		b.add(b.start(id, acc), ts, value, sf)
		return admitted
	}

	cardinality := int64(h.series.Len() + b.registered)
	if limits.MaxSeries > 0 && cardinality >= limits.MaxSeries {
		return rejectCardinality
	}

	if limits.Overflow != nil && limits.MaxSeriesSoft > 0 && cardinality >= limits.MaxSeriesSoft {
		ov := limits.Overflow(materialize())
		oid := ov.Hash()
		if k, ok := b.pos[oid]; ok {
			b.add(k, ts, value, sf)
		} else {
			previous, exists := h.seriesNewest[oid]
			a := walSeriesAcc{buffer: h.samples[oid], priorNewest: previous, hadNewest: exists}
			a.isNew = a.buffer == nil
			if a.isNew && !h.series.Has(oid) {
				a.register, a.series = true, ov
			}
			b.add(b.start(oid, a), ts, value, sf)
		}

		return admittedOverflow
	}

	acc.register, acc.series = true, materialize()
	b.add(b.start(id, acc), ts, value, sf)

	return admitted
}

func (a *walSeriesAcc) admissionNewest() int64 {
	if a.hadNewest {
		return max(a.newest, a.priorNewest)
	}
	return a.newest
}

func (b *walBatch) start(id signal.SeriesID, acc walSeriesAcc) int {
	k := len(b.order)
	b.order = append(b.order, id)
	b.pos[id] = k
	if k < len(b.accs) {
		acc.ts, acc.values, acc.sf = b.accs[k].ts[:0], b.accs[k].values[:0], b.accs[k].sf[:0]
		acc.attrs = b.accs[k].attrs
		b.accs[k] = acc
	} else {
		b.accs = append(b.accs, acc)
	}
	if acc.register {
		b.registered++
	}
	return k
}

func (b *walBatch) add(k int, ts int64, value, sf float64) {
	if b.samples == 0 {
		b.first = time.Now()
	}

	a := &b.accs[k]
	if len(a.ts) == 0 {
		a.newest = ts
	} else {
		a.newest = max(a.newest, ts)
	}
	a.ts = append(a.ts, ts)
	a.values = append(a.values, value)
	a.sf = append(a.sf, sf)

	if sf != 1 {
		a.sampled = true
	}

	b.samples++
}

// empty reports whether the batch staged no samples (so the engine can skip the WAL write).
func (b *walBatch) empty() bool { return len(b.order) == 0 }

// flushWALLocked encodes under the state lock, then logs with readers admitted.
func (e *Engine) flushWALLocked() error {
	if err := e.walB.encode(e.head, false); err != nil {
		return err
	}
	return e.mu.WithReaders(func() error { return e.cfg.WAL.WriteFrames(e.walB.frames.Bytes()) })
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
			s := a.series
			if !a.register {
				s, _ = h.series.GetInto(id, a.attrs)
				a.attrs = s.Attributes
			}
			if err := b.fw.WriteSeries(id, s); err != nil {
				return err
			}
		case everySeries:
			s, _ := h.series.GetInto(id, a.attrs)
			a.attrs = s.Attributes
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

		buf := a.buffer
		if buf == nil {
			buf = h.bufFor(id)
		}
		for i := range a.ts {
			buf.appendSample(a.ts[i], a.values[i], a.sf[i])
		}

		h.grow(int64(len(a.ts)) * SampleBytes)
		if !a.hadNewest || a.newest > a.priorNewest {
			h.seriesNewest[id] = a.newest
		}
		h.newest = max(h.newest, a.newest)
	}

	if wasEmpty && h.bytes > 0 {
		h.since = b.first
	}
}

func (b *walBatch) reset() {
	for i := range b.order {
		b.accs[i].series = signal.Series{}
		b.accs[i].buffer = nil
	}
	b.order = b.order[:0]
	b.samples, b.registered = 0, 0
	clear(b.pos)
}
