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
	// sortIdx is the reusable row-order buffer shared by every stream's ts gather — see
	// [recordCols.tsOrder]. It rides the flush buffer because it has the same lifetime.
	sortIdx []int
}

func (f *flushColumns) len() int { return len(f.stream) }

// byteSize is the buffer's resident footprint: the columns plus the stream ids they are keyed by.
// It is what a merge seals an output part on, so a variable-width record is measured, not modeled.
func (f *flushColumns) byteSize() int64 {
	return f.cols.byteSize() + int64(len(f.stream))*streamIDBytes
}

// rowBytes is row i's footprint, its stream id included.
func (f *flushColumns) rowBytes(i int) int64 { return f.cols.rowBytes(i) + streamIDBytes }

// streamIDBytes is one row's share of the stream id column (a 128-bit id per row).
const streamIDBytes = 16

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

// byteRanges splits f into [lo, hi) row ranges each holding at most capBytes of decoded record
// bytes (capBytes ≤ 0 ⇒ a single full-width range). Splitting at arbitrary row boundaries is safe:
// parts are independent and a stream spanning two is concatenated by the read seam.
//
// Records are variable-width, so a row count cannot stand in for a byte budget — the same cap holds
// ten 1 MiB rows or ten thousand 1 KiB ones. A range always holds at least one row, so a record
// larger than the whole cap still makes progress.
func byteRanges(f *flushColumns, capBytes int64) [][2]int {
	n := f.len()
	if capBytes <= 0 || n == 0 {
		return [][2]int{{0, n}}
	}

	var (
		out [][2]int
		lo  int
		acc int64
	)

	for i := range n {
		b := f.rowBytes(i)

		// Closed before the row that would exceed the cap, not after: the cap bounds a buffer the
		// process must hold, so overshooting it by a whole record is the one thing it must not do.
		if acc > 0 && acc+b > capBytes {
			out = append(out, [2]int{lo, i})
			lo, acc = i, 0
		}

		acc += b
	}

	return append(out, [2]int{lo, n})
}

// slice returns a read-only view of rows [lo, hi) of f, sharing every backing array (no copy). The
// byte columns keep the whole blob and reslice their offset index instead of rebasing it — cell i of
// the view is data[offsets[i]:offsets[i+1]] either way, so an offset index that does not start at 0
// is a valid column everywhere it is read or encoded (see [byteCol]).
func (f *flushColumns) slice(lo, hi int) *flushColumns {
	src := f.cols
	cols := &recordCols{
		schema: src.schema,
		sel:    src.sel,
		ts:     src.ts[lo:hi],
		ints:   make([][]int64, len(src.ints)),
		bytes:  make([]byteCol, len(src.bytes)),
		tsMin:  maxInt64,
		tsMax:  minInt64,
	}

	for k, col := range src.ints {
		if col != nil {
			cols.ints[k] = col[lo:hi]
		}
	}

	for k := range src.bytes {
		bc := &src.bytes[k]
		if bc.rows() == 0 {
			continue
		}

		cols.bytes[k] = byteCol{data: bc.data, offsets: bc.offsets[lo : hi+1]}
	}

	return &flushColumns{stream: f.stream[lo:hi], cols: cols}
}

// detach moves the head's record buffers aside for a flush and installs fresh empty buffers, so new
// appends are unaffected, returning the detached buffers (nil if no stream holds a record) and the
// buffered byte count they carried. The stream index is retained — identities outlive a flush. The
// caller (the engine) keeps the detached buffers readable until the flushed part is published, so a
// concurrent fetch never loses sight of the records mid-flush; on a failed flush it hands them back
// via [head.reattach].
func (h *head) detach() (map[signal.SeriesID]*recordCols, int64) {
	hasRows := false
	for _, buf := range h.records {
		if buf.len() > 0 {
			hasRows = true

			break
		}
	}

	if !hasRows {
		return nil, 0
	}

	detached, bytes := h.records, h.bytes
	h.records = make(map[signal.SeriesID]*recordCols)
	h.bytes = 0
	// The detached buffers are still resident: keep their bytes in the in-flight measure until the
	// part is published ([head.releaseDetached]) or they are folded back in ([head.reattach]).
	h.detachedBytes = bytes

	return detached, bytes
}

// releaseDetached drops the detached buffers' bytes from the in-flight measure. Called when the
// flushed part is published — the point at which the engine lets go of the buffers.
func (h *head) releaseDetached() { h.detachedBytes = 0 }

// reattach folds buffers detached by a failed flush back into the head, so the records are retried by
// the next flush instead of being dropped: the part was never published, so nothing else holds them.
// Rows appended while the flush was in flight are concatenated after the detached ones (a flush sorts
// by (stream, ts) anyway, so arrival order across the two carries no meaning). bytes is the count
// [head.detach] took away.
func (h *head) reattach(detached map[signal.SeriesID]*recordCols, bytes int64) {
	for id, buf := range detached {
		if buf.len() == 0 {
			continue
		}

		if live := h.records[id]; live != nil && live.len() > 0 {
			buf.appendRange(live, 0, live.len())
		}

		h.records[id] = buf
	}

	// The bytes move back from the detached side of the measure to the live one; counting them in
	// both would permanently inflate the in-flight total.
	h.bytes += bytes
	h.detachedBytes = 0
}

// buildFlushColumns lays the detached record buffers out as part columns sorted by (stream, ts). It
// runs off the engine lock and must therefore only *read* the detached buffers: the engine keeps
// them fetchable until the part is published, so a concurrent fetch is reading the very same
// buffers. Ordering is applied at copy time — rows are gathered into the flush buffer through each
// stream's ts permutation ([recordCols.tsOrder]) instead of the source being sorted in place.
func buildFlushColumns(schema *Schema, records map[signal.SeriesID]*recordCols, reuse *flushColumns) *flushColumns {
	ids := make([]signal.SeriesID, 0, len(records))
	for id, buf := range records {
		if buf.len() > 0 {
			ids = append(ids, id)
		}
	}

	slices.SortFunc(ids, signal.SeriesID.Compare)

	f := reuse
	if f == nil {
		f = &flushColumns{cols: newRecordCols(schema, 0, fullSel(schema))}
	}

	rows, blob := flushShape(schema, records, ids)
	f.reset(schema, rows, blob)

	for _, id := range ids {
		buf := records[id]
		u := idToU128(id)

		// Gather each stream's records in ts order so the part is (stream, ts)-sorted. A nil order
		// means the buffer is already ascending, the common case.
		order := buf.tsOrder(f.sortIdx[:0])
		if order != nil {
			f.sortIdx = order
		}

		for i := range buf.ts {
			row := i
			if order != nil {
				row = order[i]
			}

			f.stream = append(f.stream, u)
			f.cols.appendRow(buf, row)
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
	idents identitySet, comp compress.Algorithm, level compress.Level, bb *bloomBuilder,
) error {
	opts := []block.PartOption{block.WithSortKey(colTs)}
	if comp != compress.AlgorithmNone {
		opts = append(opts, block.WithCompression(comp), block.WithCompressionLevel(level))
	}

	w := block.NewPartWriter(opts...)

	if err := w.AddColumn(block.Column{Name: colStream, Kind: block.KindInt128, Int128: f.stream}); err != nil {
		return err
	}

	// Block-framed from here down, so a windowed fetch decodes only the granules its rows occupy
	// rather than the whole column (see granule.go). The stream id column stays unframed: a fetch
	// resolves streams through the part's row-range index and never decodes it.
	if err := w.AddColumn(block.Column{
		Name: colTs, Kind: block.KindInt64, Codec: chunk.CodecDoD, Int64: f.cols.ts, Block: true,
	}); err != nil {
		return err
	}

	for k := range schema.intCols {
		col := schema.intColumn(k)
		if err := w.AddColumn(block.Column{
			Name: col.Name, Kind: block.KindInt64, Codec: col.Codec, Int64: f.cols.ints[k], Block: true,
		}); err != nil {
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
			BytesBlob: bc.data, BytesOffsets: bc.offsets, Block: true,
		}); err != nil {
			return err
		}
	}

	if err := block.WritePart(ctx, b, prefix, w); err != nil {
		return errors.Wrapf(err, "write part %q", prefix)
	}

	// Identity object: the identities of this part's streams, so the part carries what names its
	// own rows (see partidentity.go). Written before the part is committed to the bucket index,
	// which is what makes it visible — a readable part always has its identities.
	if err := writeIdentity(ctx, b, prefix, idents.entriesFor(f.stream)); err != nil {
		return err
	}

	if err := writeBlooms(ctx, b, schema, prefix, f.cols, bb); err != nil {
		return err
	}

	return writeRecordKeys(ctx, b, schema, prefix, f.cols)
}

// partPrefix is the backend key prefix of the seq-th part of this engine.
func (e *Engine) partPrefix(seq int) string {
	return fmt.Sprintf("%s/%010d", e.cfg.Prefix, seq)
}

// reserveSeq allocates the next part sequence and advances the counter immediately, so part prefixes
// are append-only: an attempt that fails after writing some of its objects burns its sequence instead
// of leaving it for the retry. Reuse would be unsound — the retry overwrites only the objects it
// itself produces, and two of a part's objects are conditional (the record-key footer is skipped when
// the rows carry no attributes, the side-store sidecars when the flush has no side data), so a
// reusing part silently adopts the failed attempt's keys.bin / symbol tables. The leftovers are swept
// at open by [Engine.LoadParts]. Safe for concurrent use.
func (e *Engine) reserveSeq() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	seq := e.nextSeq
	e.nextSeq++

	return seq
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
