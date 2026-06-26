package engine

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// flushColumns is the head's buffered samples laid out as the flat part columns, one row per
// sample, sorted by (series, ts). sf carries the lossy-sampling weights and is nil when every
// weight is 1 (the unsampled common case), in which case no sf column is written — the part keeps
// its original three-column layout.
type flushColumns struct {
	series []chunk.U128
	ts     []int64
	value  []float64
	sf     []float64
}

// appendRow appends one (series, ts, value, sf) row, materializing the sf column lazily the first
// time a non-unit weight appears (backfilling 1 for the rows already collected).
func (c *flushColumns) appendRow(u chunk.U128, ts int64, value, sf float64) {
	c.series = append(c.series, u)
	c.ts = append(c.ts, ts)
	c.value = append(c.value, value)

	switch {
	case c.sf != nil:
		c.sf = append(c.sf, sf)
	case sf != 1:
		c.sf = make([]float64, len(c.ts)-1, len(c.ts))
		for i := range c.sf {
			c.sf[i] = 1
		}

		c.sf = append(c.sf, sf)
	}
}

// drainHead snapshots every buffered sample into part columns sorted by (series, ts) and
// clears the head's sample buffers (the series index is retained — identities outlive a
// flush). It returns nil if no series has buffered samples.
func (h *head) drainHead() *flushColumns {
	ids := make([]signal.SeriesID, 0, len(h.samples))
	for id, buf := range h.samples {
		if len(buf.ts) > 0 {
			ids = append(ids, id)
		}
	}

	if len(ids) == 0 {
		return nil
	}

	slices.SortFunc(ids, func(a, b signal.SeriesID) int { return a.Compare(b) })

	cols := &flushColumns{}

	for _, id := range ids {
		buf := h.samples[id]

		ts, values, sf := sortedWindow(buf, minInt64, maxInt64)
		u := idToU128(id)

		for i := range ts {
			w := float64(1)
			if sf != nil {
				w = sf[i]
			}

			cols.appendRow(u, ts[i], values[i], w)
		}
	}

	h.samples = make(map[signal.SeriesID]*sampleBuf)
	h.bytes = 0

	return cols
}

const (
	minInt64 = int64(-1 << 63)
	maxInt64 = int64(1<<63 - 1)
)

// writePart writes cols as a metric part under prefix via [block.PartWriter].
func writePart(ctx context.Context, b backend.Backend, prefix string, cols *flushColumns) error {
	w := block.NewPartWriter(block.WithSortKey(colTs))
	if err := w.AddColumn(block.Column{Name: colSeries, Kind: block.KindInt128, Int128: cols.series}); err != nil {
		return err
	}

	if err := w.AddColumn(block.Column{Name: colTs, Kind: block.KindInt64, Codec: chunk.CodecDoD, Int64: cols.ts}); err != nil {
		return err
	}

	if err := w.AddColumn(block.Column{Name: colValue, Kind: block.KindFloat64, Float64: cols.value}); err != nil {
		return err
	}

	// The scale-factor column is additive: it is written only when sampling actually occurred, so
	// an unsampled part keeps its original three-column layout (and a reader defaults a missing sf
	// column to weight 1). A constant column (e.g. a whole part sampled at one factor) collapses to
	// a single manifest value with no data object.
	if cols.sf != nil {
		if err := w.AddColumn(block.Column{Name: colSF, Kind: block.KindFloat64, Float64: cols.sf}); err != nil {
			return err
		}
	}

	if err := block.WritePart(ctx, b, prefix, w); err != nil {
		return errors.Wrapf(err, "write part %q", prefix)
	}

	return nil
}

// partPrefix is the backend key prefix of the seq-th part of this engine.
func (e *Engine) partPrefix(seq int) string {
	return fmt.Sprintf("%s/%010d", e.cfg.Prefix, seq)
}
