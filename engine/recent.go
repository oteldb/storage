package engine

import (
	"github.com/oteldb/storage/signal"
)

// The recent tier is an in-memory, read-side mirror of the most recent flush window, kept across
// flushes so a query whose time range falls inside it answers from RAM without touching or decoding
// file parts (issue #25, item 4). It is distinct from the head (the unflushed write tail): the head
// is drained on every flush, but the recent tier persists, holding already-flushed samples for the
// window so first-touch "recent range" queries skip the decode path the decode cache only helps on
// repeats.
//
// The tier is populated at flush publish from the detached buffers (the samples just written to a
// part), retaining those within [newest-RecentWindow, newest]. Because the same samples now live in
// both the part and the tier, planFetch's freshest-wins timestamp merge (sampleMerge) dedups the
// overlap — there is no double count. A query with Start ≥ recentMin is served wholly by the tier ∪
// the mid-flush buffers ∪ the head (which together cover [recentMin, now]); every part is skipped.

// populateRecent merges the just-flushed detached buffers into the recent tier (retaining samples
// within the window) and trims everything older, recomputing the tier's min timestamp. Caller holds
// e.mu.
func (e *Engine) populateRecent(detached map[signal.SeriesID]*sampleBuf) {
	cutoff := e.head.newest - e.cfg.RecentWindow
	if e.head.newest == 0 { // no samples seen yet ⇒ keep everything this flush
		cutoff = 0
	}

	// Append this flush's in-window samples into the tier.
	for id, buf := range detached {
		if buf == nil {
			continue
		}

		rbuf := e.recent[id]
		if rbuf == nil {
			rbuf = &sampleBuf{}
			e.recent[id] = rbuf
		}

		for i := range buf.ts {
			if buf.ts[i] < cutoff {
				continue
			}

			var sf float64
			if buf.sf != nil {
				sf = buf.sf[i]
			}

			rbuf.appendSample(buf.ts[i], buf.values[i], sf)
		}
	}

	// Trim every tier buffer to the window, dropping series that no longer hold any in-window sample,
	// and track the tier's min timestamp for the planFetch short-circuit.
	e.recentMin = maxInt64

	for id, rbuf := range e.recent {
		ts, vals, sf := trimBufBelow(rbuf, cutoff)
		if len(ts) == 0 {
			delete(e.recent, id)

			continue
		}

		rbuf.ts, rbuf.values, rbuf.sf = ts, vals, sf

		if ts[0] < e.recentMin {
			e.recentMin = ts[0]
		}
	}

	// recentMin stays maxInt64 when the tier is empty, which disables the short-circuit (no Start ≥
	// maxInt64 is ever true).
}

// trimBufBelow returns rbuf's samples with ts ≥ cutoff, compacted into fresh backing arrays when any
// were trimmed (so the dropped prefix is freed), or rbuf's own slices unchanged when nothing was
// trimmed. Samples are arrival-ordered, not sorted; the filter preserves order.
func trimBufBelow(rbuf *sampleBuf, cutoff int64) (ts []int64, vals, sf []float64) {
	keep := 0

	for _, t := range rbuf.ts {
		if t >= cutoff {
			keep++
		}
	}

	switch keep {
	case 0:
		return nil, nil, nil
	case len(rbuf.ts):
		return rbuf.ts, rbuf.values, rbuf.sf
	}

	ts = make([]int64, 0, keep)
	vals = make([]float64, 0, keep)

	hasSF := rbuf.sf != nil
	if hasSF {
		sf = make([]float64, 0, keep)
	}

	for i := range rbuf.ts {
		if rbuf.ts[i] < cutoff {
			continue
		}

		ts = append(ts, rbuf.ts[i])
		vals = append(vals, rbuf.values[i])

		if hasSF {
			sf = append(sf, rbuf.sf[i])
		}
	}

	return ts, vals, sf
}

// recentEnabled reports whether the recent tier is configured on.
func (e *Engine) recentEnabled() bool { return e.recent != nil }
