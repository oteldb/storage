package recordengine

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/signal"
)

// flushColumns is the head's buffered records laid out as flat part columns: the int128 stream
// sort grouping plus the full per-record column set, one row per record, sorted by (stream, ts).
type flushColumns struct {
	stream []chunk.U128
	cols   *recordCols // full column set (every schema column)
}

func (f *flushColumns) len() int { return len(f.stream) }

// reset re-arms the buffer for another flush at the given shape, keeping the backing arrays. A part
// is written and read back before the next flush starts (the engine has a single flusher), so the
// buffer that fed one part is free to feed the next — and after the first flush its arrays are
// already the right size, so a steady ingest rate stops allocating and re-zeroing them entirely.
func (f *flushColumns) reset(schema *Schema, rows int, blob []int) {
	if cap(f.stream) >= rows {
		f.stream = f.stream[:0]
	} else {
		f.stream = make([]chunk.U128, 0, max(rows, 2*cap(f.stream)))
	}

	f.cols.prepare(schema, rows, fullSel(schema))

	for k := range f.cols.bytes {
		f.cols.bytes[k].ensureBytes(rows, blob[k])
	}
}

const (
	minInt64 = int64(-1 << 63)
	maxInt64 = int64(1<<63 - 1)
)

// detach moves the head's record buffers aside for a flush and installs fresh empty buffers, so new
// appends are unaffected, returning the detached buffers (nil if no stream holds a record). The stream
// index is retained — identities outlive a flush. The caller (the engine) keeps the detached buffers
// readable until the flushed part is published, so a concurrent fetch never loses sight of the records
// mid-flush.
func (h *head) detach() map[signal.SeriesID]*recordCols {
	hasRows := false
	for _, buf := range h.records {
		if buf.len() > 0 {
			hasRows = true

			break
		}
	}

	if !hasRows {
		return nil
	}

	detached := h.records
	h.records = make(map[signal.SeriesID]*recordCols)
	h.bytes = 0

	return detached
}

// buildFlushColumns lays the detached record buffers out as part columns sorted by (stream, ts). It
// reads the (now immutable) detached buffers off the engine lock.
func buildFlushColumns(schema *Schema, records map[signal.SeriesID]*recordCols, reuse *flushColumns) *flushColumns {
	ids := make([]signal.SeriesID, 0, len(records))
	for id, buf := range records {
		if buf.len() > 0 {
			ids = append(ids, id)
		}
	}

	slices.SortFunc(ids, func(a, b signal.SeriesID) int { return a.Compare(b) })

	f := reuse
	if f == nil {
		f = &flushColumns{cols: newRecordCols(schema, 0, fullSel(schema))}
	}

	rows, blob := flushShape(schema, records, ids)
	f.reset(schema, rows, blob)

	for _, id := range ids {
		buf := records[id]
		buf.sortByTs() // order each stream's records by ts so the part is (stream, ts)-sorted

		u := idToU128(id)
		for i := range buf.ts {
			f.stream = append(f.stream, u)
			f.cols.appendRow(buf, i)
		}
	}

	return f
}

// flushShape measures the detached head: its total row count and, per byte column, its total blob
// bytes. Both are already known — the head holds the buffers — and sizing the flush buffer from them
// keeps it from growing each column out of nothing, re-copying every blob ~log₂(size) times and
// ending up with as much as 2× the capacity it needs, on the path whose whole job is to hand memory
// back.
func flushShape(schema *Schema, records map[signal.SeriesID]*recordCols, ids []signal.SeriesID) (rows int, blob []int) {
	blob = make([]int, schema.numBytes())

	for _, id := range ids {
		buf := records[id]
		rows += buf.len()

		for k := range buf.bytes {
			blob[k] += int(buf.bytes[k].byteSize())
		}
	}

	return rows, blob
}

// writePart writes f as a part under prefix via [block.PartWriter]: the stream id column, the
// timestamp sort key, then every schema column with its codec. comp block-compresses every column on
// top of its chunk codec; [compress.AlgorithmNone] writes the columns codec-only (the flush path,
// kept cheap), while the cold merge passes ZSTD to entropy-code the long-lived compacted data.
func writePart(
	ctx context.Context, b backend.Backend, schema *Schema, prefix string, f *flushColumns,
	comp compress.Algorithm, level compress.Level,
) error {
	opts := []block.PartOption{block.WithSortKey(colTs)}
	if comp != compress.AlgorithmNone {
		opts = append(opts, block.WithCompression(comp), block.WithCompressionLevel(level))
	}

	w := block.NewPartWriter(opts...)

	if err := w.AddColumn(block.Column{Name: colStream, Kind: block.KindInt128, Int128: f.stream}); err != nil {
		return err
	}

	if err := w.AddColumn(block.Column{Name: colTs, Kind: block.KindInt64, Codec: chunk.CodecDoD, Int64: f.cols.ts}); err != nil {
		return err
	}

	for k := range schema.intCols {
		col := schema.intColumn(k)
		if err := w.AddColumn(block.Column{Name: col.Name, Kind: block.KindInt64, Codec: col.Codec, Int64: f.cols.ints[k]}); err != nil {
			return err
		}
	}

	for k := range schema.byteCols {
		// Blob+offsets pass-through: the head buffer's byte-column layout feeds the encoder
		// directly, so a flush materializes no per-row [][]byte view.
		col := schema.byteColumn(k)
		bc := &f.cols.bytes[k]
		if err := w.AddColumn(block.Column{
			Name: col.Name, Kind: block.KindBytes, Codec: col.Codec,
			BytesBlob: bc.data, BytesOffsets: bc.offsets,
		}); err != nil {
			return err
		}
	}

	if err := block.WritePart(ctx, b, prefix, w); err != nil {
		return errors.Wrapf(err, "write part %q", prefix)
	}

	if err := writeBlooms(ctx, b, schema, prefix, f.cols); err != nil {
		return err
	}

	return writeRecordKeys(ctx, b, schema, prefix, f.cols)
}

// partPrefix is the backend key prefix of the seq-th part of this engine.
func (e *Engine) partPrefix(seq int) string {
	return fmt.Sprintf("%s/%010d", e.cfg.Prefix, seq)
}

// colsTimeRange returns the inclusive min/max timestamp across f (≥ 1 record when a part is written).
func colsTimeRange(f *flushColumns) (minTime, maxTime int64) {
	minTime, maxTime = maxInt64, minInt64
	for _, t := range f.cols.ts {
		if t < minTime {
			minTime = t
		}

		if t > maxTime {
			maxTime = t
		}
	}

	return minTime, maxTime
}
