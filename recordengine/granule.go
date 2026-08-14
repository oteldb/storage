package recordengine

import (
	"context"
	"slices"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/signal"
)

// Granule time pruning. A part's columns are block-framed, so a reader can decode one granule at a
// time, and the marks sidecar records each granule's [minTime, maxTime]. A query with a time window
// therefore decodes only the granules that window can intersect, instead of the whole column.
//
// This is what part span stops being the only time filter records have. Without it a 15-minute query
// against a part spanning a day decodes the day: measured on the real log corpus, a 15-minute window
// reads 286x the rows it needs.
//
// The row ranges matter as much as the bounds. Rows are sorted by (stream, ts), so a granule's
// bounds are not monotonic across the part — but each requested stream occupies one contiguous run,
// and within that run timestamps ascend. Selecting from the *requested streams'* ranges rather than
// from the whole part is what makes a service-filtered query touch a handful of granules.

// granuleTimes returns the part's per-granule record-time bounds, lazily loading the marks sidecar.
// nil ⇒ the index is absent, corrupt, or does not line up with the part's framing, which leaves
// every granule a candidate.
func (p *part) granuleTimes(ctx context.Context) []block.Granule {
	p.marksOnce.Do(func() {
		if p.reader == nil {
			return
		}

		man := p.reader.Manifest()
		if man.GranuleSize <= 0 {
			return
		}

		m, err := p.reader.Marks(ctx)
		if err != nil || m.GranuleSize != man.GranuleSize {
			return
		}

		if len(m.Granules) != (man.RowCount+man.GranuleSize-1)/man.GranuleSize {
			return
		}

		p.granules = m.Granules
	})

	return p.granules
}

// granuleInWindow reports whether granule g can hold a record in [start, end]. A granule outside the
// index is always a candidate, so pruning can only ever remove granules it can prove empty.
func granuleInWindow(gran []block.Granule, g int, start, end int64) bool {
	if g < 0 || g >= len(gran) {
		return true
	}

	return gran[g].MaxKey >= start && gran[g].MinKey <= end
}

// windowGranules returns the granules a fetch must decode: those the requested streams' row ranges
// span whose bounds can hold a record in [start, end].
//
// nil means "decode everything" — either pruning is unavailable, or every granule survived, in which
// case naming them all would only cost the caller a list to walk.
func (p *part) windowGranules(ctx context.Context, ids []signal.SeriesID, start, end int64) []int {
	gran := p.granuleTimes(ctx)
	if gran == nil || p.reader == nil {
		return nil
	}

	size := p.reader.Manifest().GranuleSize
	if size <= 0 {
		return nil
	}

	var out []int

	for _, id := range ids {
		rng, ok := p.ranges[id]
		if !ok || rng.start >= rng.end {
			continue
		}

		first := rng.start / size
		last := min((rng.end-1)/size, len(gran)-1)

		for g := first; g <= last; g++ {
			if granuleInWindow(gran, g, start, end) {
				out = append(out, g)
			}
		}
	}

	if len(out) == 0 {
		return nil
	}

	// Streams are visited in the caller's id order, which need not be row order, and neighboring
	// streams share the granule straddling their boundary.
	slices.Sort(out)
	out = slices.Compact(out)

	if len(out) == len(gran) {
		return nil // nothing pruned
	}

	return out
}
