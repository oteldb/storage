package engine

import (
	"bytes"
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

type knownSampleHead struct {
	snapshot          *seriesSnapshot
	marks             map[signal.SeriesID]int64
	sampleBytes, rows int64
}

func (h *knownSampleHead) newest(id signal.SeriesID) (int64, bool) {
	if h == nil {
		return 0, false
	}
	n, ok := h.marks[id]
	return n, ok
}
func (h *knownSampleHead) has(id signal.SeriesID) bool { _, ok := h.newest(id); return ok }
func (h *knownSampleHead) len() int {
	if h == nil {
		return 0
	}
	return len(h.marks)
}
func (h *knownSampleHead) sizeBytes() int64 {
	if h == nil {
		return 0
	}
	// Both the watermark map and its entries in the head's sample maps remain resident.
	return memsize.Of[knownSampleHead]() + memsize.Map(h.marks) +
		2*int64(len(h.marks))*memsize.MapEntry[signal.SeriesID, *sampleBuf]() + memsize.Slice(h.snapshot.roots)
}

// prepareKnownSamples verifies canonical bytes before WAL acknowledgement, without retaining labels.
func (e *Engine) prepareKnownSamples(b *walBatch) error {
	if !e.cfg.KnownSeriesHead {
		return nil
	}
	snapshot := e.seriesTable
	if e.head.known != nil {
		snapshot = e.head.known.snapshot
	}
	if snapshot == nil {
		return nil
	}
	positions := make([]int, 0, len(b.order))
	for i := range b.order {
		if a := &b.accs[i]; a.register && a.historical {
			a.compact = false
			positions = append(positions, i)
		}
	}
	slices.SortFunc(positions, func(i, j int) int { return b.order[i].Compare(b.order[j]) })
	var canonical []byte
	remaining := len(positions)
	for _, root := range snapshot.roots {
		if remaining == 0 {
			break
		}
		if err := verifyKnownRoot(root, b, positions, &canonical); err != nil {
			return err
		}

		next := positions[:0]
		for _, i := range positions {
			if b.accs[i].compact {
				remaining--
				if b.knownSnapshot == nil {
					snapshot.acquire()
					b.knownSnapshot = snapshot
				}
			} else {
				next = append(next, i)
			}
		}
		positions = next
	}
	for _, i := range positions {
		if e.head.known.has(b.order[i]) {
			return errors.New("engine: known head identity missing from pinned metadata")
		}
	}

	return nil
}

func verifyKnownRoot(root *seriesRootHandle, b *walBatch, positions []int, canonical *[]byte) (err error) {
	r, err := root.run.NewReader(context.Background())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	r.StreamPages()
	for _, i := range positions {
		raw, found, err := r.LookupRaw(b.order[i])
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		a := &b.accs[i]
		*canonical = a.series.AppendHashInput((*canonical)[:0])
		if !bytes.Equal(raw, *canonical) {
			return errors.New("engine: shared series canonical identity conflict")
		}
		a.compact = true
	}
	return nil
}

func (h *head) applyKnownSamples(id signal.SeriesID, a *walSeriesAcc, snapshot *seriesSnapshot) {
	if h.known == nil {
		snapshot.acquire()
		h.known = &knownSampleHead{snapshot: snapshot, marks: make(map[signal.SeriesID]int64)}
	}
	buf := h.samples[id]
	if buf == nil {
		buf = &sampleBuf{}
		h.samples[id] = buf
		h.known.sampleBytes += memsize.Of[sampleBuf]()
	}
	before := knownBufferBytes(buf)
	for i, ts := range a.ts {
		weight := 1.0
		if a.sf != nil {
			weight = a.sf[i]
		}
		buf.appendSample(ts, a.values[i], weight)
		h.noteBufferedTS(ts)
	}
	h.known.sampleBytes += knownBufferBytes(buf) - before
	h.known.rows += int64(len(a.ts))
	h.known.marks[id] = a.admissionNewest()
	h.newest = max(h.newest, a.newest)
	h.grow(int64(len(a.ts)) * SampleBytes)
}

func knownBufferBytes(buf *sampleBuf) int64 {
	return int64(cap(buf.ts)+cap(buf.values)+cap(buf.sf)) * 8
}

func (e *Engine) residentSampleHeadBytes() int64 {
	h := e.head
	n := h.residentIdentityBytes() + h.inFlightBytes()
	if h.known != nil {
		n += max(h.known.sampleBytes-h.known.rows*SampleBytes, 0)
	}
	return n
}

func (h *head) compactKnownSamples() {
	if h.known == nil {
		return
	}
	live := 0
	for id, newest := range h.known.marks {
		if h.samples[id] != nil {
			live++
		} else {
			h.disk.remember(id, newest)
		}
	}
	switch live {
	case 0:
		h.closeKnownSamples()
		return
	case len(h.known.marks):
		h.recountKnownSamples(nil)
		return
	}
	next := make(map[signal.SeriesID]int64, live)
	for id, newest := range h.known.marks {
		if h.samples[id] != nil {
			next[id] = newest
		}
	}
	h.known.marks = next
	h.recountKnownSamples(nil)
}

func (h *head) recountKnownSamples(frozen map[signal.SeriesID]*sampleBuf) {
	if h.known == nil {
		return
	}
	h.known.sampleBytes, h.known.rows = 0, 0
	for id := range h.known.marks {
		for _, buf := range [2]*sampleBuf{h.samples[id], frozen[id]} {
			if buf != nil {
				h.known.sampleBytes += memsize.Of[sampleBuf]() + knownBufferBytes(buf)
				h.known.rows += int64(len(buf.ts))
			}
		}
	}
}
func (h *head) closeKnownSamples() {
	if h.known != nil {
		h.known.snapshot.release()
		h.known = nil
	}
}
