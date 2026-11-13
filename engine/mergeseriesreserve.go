package engine

// mergeSeriesReservation spends a bounded, operation-local budget across output writers.
// Input cardinality is a hint; disjoint series and rollups retain the normal growth path.
type mergeSeriesReservation struct {
	series    int
	remaining int64
}

func newMergeSeriesReservation(src []*part, allowance, held int64) mergeSeriesReservation {
	limit := int64(32 << 20)
	if allowance > 0 {
		limit = min(limit, max(allowance-held, 0)/8)
	}
	r := mergeSeriesReservation{remaining: limit}
	for _, p := range src {
		r.series = max(r.series, p.index.seriesCount())
	}
	return r
}

func (r *mergeSeriesReservation) reserve(w *partStreamWriter) error {
	bytesPerSeries := int64(24 + 8)
	if w.withStats {
		bytesPerSeries += 40
	}
	// Leave room for allocator size classes and slice capacity rounding.
	n := min(r.series, int(r.remaining/(2*bytesPerSeries)))
	if n == 0 {
		return nil
	}
	before := w.residentBytes()
	if err := w.reserveSeries(n); err != nil {
		w.abort()
		return err
	}
	r.remaining -= w.residentBytes() - before
	return nil
}
