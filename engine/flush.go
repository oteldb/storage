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

// partRowBytes is the approximate uncompressed bytes a metric part row occupies (series int128 +
// ts int64 + value float64). It bounds a part's row count against [Config.MaxPartBytes]; it ignores
// compression (so the cap is conservative — real parts are smaller) and the optional sf column.
const partRowBytes = 32

// maxRowsPerPart converts a byte cap into a row cap (0 ⇒ unlimited). At least one row per part, so a
// cap smaller than a single row still makes progress.
func maxRowsPerPart(maxBytes int64) int {
	if maxBytes <= 0 {
		return 0
	}

	if r := int(maxBytes / partRowBytes); r >= 1 {
		return r
	}

	return 1
}

// chunkRanges splits n rows into [lo, hi) ranges of at most maxRows each (maxRows ≤ 0 ⇒ a single
// full-width range). Splitting at arbitrary row boundaries is safe: parts are independent and a
// series spanning two parts is merged back by the read seam.
func chunkRanges(n, maxRows int) [][2]int {
	if maxRows <= 0 || n <= maxRows {
		return [][2]int{{0, n}}
	}

	out := make([][2]int, 0, (n+maxRows-1)/maxRows)
	for lo := 0; lo < n; lo += maxRows {
		out = append(out, [2]int{lo, min(lo+maxRows, n)})
	}

	return out
}

// reset truncates the columns to empty while keeping their backing arrays, so a streaming merge can
// refill the same buffer for the next output part without reallocating.
func (c *flushColumns) reset() {
	c.series = c.series[:0]
	c.ts = c.ts[:0]
	c.value = c.value[:0]
	c.sf = nil // the next part re-materializes sf lazily only if it samples
}

// slice returns a view of rows [a, b) of the columns (sharing the backing arrays; read-only use).
func (c *flushColumns) slice(a, b int) *flushColumns {
	out := &flushColumns{series: c.series[a:b], ts: c.ts[a:b], value: c.value[a:b]}
	if c.sf != nil {
		out.sf = c.sf[a:b]
	}

	return out
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

// buildFlushColumns lays the detached sample buffers out as part columns sorted by (series, ts),
// returning nil if no series has a sample. It reads the (now immutable) detached buffers off the
// engine lock.
func buildFlushColumns(samples map[signal.SeriesID]*sampleBuf) *flushColumns {
	ids := make([]signal.SeriesID, 0, len(samples))
	for id, buf := range samples {
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
		buf := samples[id]

		ts, values, sf := sortedWindow(buf, minInt64, maxInt64, nil)
		u := idToU128(id)

		for i := range ts {
			w := float64(1)
			if sf != nil {
				w = sf[i]
			}

			cols.appendRow(u, ts[i], values[i], w)
		}
	}

	return cols
}

const (
	minInt64 = int64(-1 << 63)
	maxInt64 = int64(1<<63 - 1)
)

// writePart writes cols as a metric part under prefix via [block.PartWriter]. A non-nil comp
// rewrites the part with a higher-ratio compression profile (recompression of cold data); nil
// keeps the default codec-only framing. precisionBits in 1..63 encodes the value column lossily
// (age-tiered precision, set by the merge engine for cold data); 0 keeps it lossless.
func writePart(
	ctx context.Context, b backend.Backend, prefix string, cols *flushColumns,
	comp *RecompressSpec, precisionBits uint8, writeStats bool, blockRows int,
) error {
	if blockRows <= 0 {
		blockRows = DefaultMetricBlockRows
	}

	// Block the ts/value/sf columns at blockRows so the engine can decode only the blocks a query
	// touches; the block size also drives the marks granules (WithGranuleSize). The series id column
	// (RLE) is not blocked — it is read whole when the part is opened to build the row-range index.
	opts := []block.PartOption{block.WithSortKey(colTs), block.WithGranuleSize(blockRows)}
	if comp != nil {
		opts = append(opts, block.WithCompression(comp.Algorithm), block.WithCompressionLevel(comp.Level))
	}

	w := block.NewPartWriter(opts...)
	if err := w.AddColumn(block.Column{Name: colSeries, Kind: block.KindInt128, Int128: cols.series}); err != nil {
		return err
	}

	if err := w.AddColumn(block.Column{Name: colTs, Kind: block.KindInt64, Codec: chunk.CodecDoD, Int64: cols.ts, Block: true}); err != nil {
		return err
	}

	// AutoCodec lets the part writer pick the denser float codec per part: an integer-valued or
	// low-precision value column (e.g. a counter) takes the scaled-decimal path, a high-entropy
	// one keeps Gorilla. precisionBits > 0 additionally allows a lossy (age-tiered) decimal
	// encoding for cold data, traded against lossless Gorilla so it is never worse.
	if err := w.AddColumn(block.Column{
		Name: colValue, Kind: block.KindFloat64, Float64: cols.value,
		AutoCodec: true, FloatPrecisionBits: precisionBits, Block: true,
	}); err != nil {
		return err
	}

	// The scale-factor column is additive: it is written only when sampling actually occurred, so
	// an unsampled part keeps its original three-column layout (and a reader defaults a missing sf
	// column to weight 1). A constant column (e.g. a whole part sampled at one factor) collapses to
	// a single manifest value with no data object.
	if cols.sf != nil {
		if err := w.AddColumn(block.Column{Name: colSF, Kind: block.KindFloat64, Float64: cols.sf, Block: true}); err != nil {
			return err
		}
	}

	if err := block.WritePart(ctx, b, prefix, w); err != nil {
		return errors.Wrapf(err, "write part %q", prefix)
	}

	// Aggregate-pushdown sidecar: per-series count/sum/min/max over the value column, so a query
	// whose range fully covers this part answers from it without decoding the column. Opt-in (it
	// costs a little storage per series) and written only for an unsampled part (raw values); a
	// sampled part falls back to the weighted decode path.
	if writeStats && cols.sf == nil {
		ids, stats := computeSeriesStats(cols)
		if err := b.Write(ctx, statsKey(prefix), encodeSeriesStats(ids, stats)); err != nil {
			return errors.Wrapf(err, "write stats sidecar %q", prefix)
		}
	}

	return nil
}

// statsKey is the backend key of a part's aggregate-pushdown sidecar (deleted with the part, since
// deletePart lists and removes everything under the prefix).
func statsKey(prefix string) string { return prefix + "/stats" }

// partPrefix is the backend key prefix of the seq-th part of this engine.
func (e *Engine) partPrefix(seq int) string {
	return fmt.Sprintf("%s/%010d", e.cfg.Prefix, seq)
}
