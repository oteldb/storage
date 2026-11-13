package engine

import "github.com/oteldb/storage/signal"

func (e *Engine) replayKnownIdentity(id signal.SeriesID, s signal.Series) (bool, error) {
	if !e.cfg.KnownSeriesHead || e.head.series.Has(id) {
		return false, nil
	}
	state := e.head.seriesState(id)
	b := walBatch{order: []signal.SeriesID{id},
		accs: []walSeriesAcc{{compact: state.compact, register: true, historical: state.historical, series: s}}}
	defer b.reset()
	if err := e.prepareKnownSamples(&b); err != nil {
		return false, err
	}
	if !b.accs[0].compact {
		return false, nil
	}
	if e.head.known == nil {
		b.knownSnapshot.acquire()
		e.head.known = &knownSampleHead{snapshot: b.knownSnapshot, marks: make(map[signal.SeriesID]int64)}
	}
	e.head.known.marks[id] = state.newest
	return true, nil
}

func (h *head) replayKnownSamples(id signal.SeriesID, ts []int64, values, sf []float64) bool {
	newest, ok := h.known.newest(id)
	if !ok {
		return false
	}
	a := walSeriesAcc{ts: ts, values: values, sf: sf, hadNewest: true, priorNewest: newest, newest: newest}

	for _, t := range ts {
		a.newest = max(a.newest, t)
	}
	h.applyKnownSamples(id, &a, h.known.snapshot)
	return true
}
