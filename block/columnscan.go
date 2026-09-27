package block

import (
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// The sequential counterpart of the ranged read path. A query touches a handful of granules out of
// thousands and wants each frame fetched alone; a merge touches every granule exactly once in order
// and wants as many frames per request as it can hold. Same directory, same decode — the difference
// is read-ahead ([frameSource.window]) and, for bytes, decoding one granule at a time against the
// cached frame rather than merging a set of them.

// ColumnScan returns a decoder over the named column tuned for a forward walk: instead of one ranged
// read per compression frame it fetches whole frames up to window bytes at a time.
//
// It is [PartReader.ColumnBlocks]' sequential counterpart, and the distinction is not a tuning knob.
// A query touches a handful of frames out of thousands, scattered, so reading ahead would fetch
// bytes it never decodes. A merge touches every frame exactly once in order, and a frame is
// [defaultCompressBlockBytes] *uncompressed* — so without coalescing a multi-source merge issues
// hundreds of thousands of serialized ranged reads that nothing caches.
//
// window is the read side's memory budget for this column: the decoder holds one buffer that large.
// A column whose frames all fit inside it is fetched in a single request, which is [PartReader.Column]
// minus the cache write. A window at or below zero disables read-ahead.
//
// A column the ranged path cannot serve is read whole, once: the legacy unframed layout, and any
// column object [backend.RangesNatively] denies, where every ranged read is itself a whole-object
// read and a windowed walk would repeat it per window.
func (r *PartReader) ColumnScan(ctx context.Context, name string, window int64) (*Decoder, error) {
	if i, ok := r.byName[name]; ok {
		desc := r.manifest.Columns[i]
		if desc.Blocked && !desc.Const &&
			(!desc.Framed || !backend.RangesNatively(ctx, r.b, columnKey(r.prefix, i))) {
			col, err := r.Column(ctx, name)
			if err != nil {
				return nil, err
			}

			return col.BlockDecoder()
		}
	}

	return r.openDecoder(ctx, name, max(window, 0))
}

// ScanBound bounds, from the manifest alone, what [Decoder.ResidentBytes] reports for
// ColumnScan(ctx, name, window), so a caller can reserve memory before opening the column. ok is
// false for a column the manifest cannot size: one written without [WithSizingStats], an unframed
// or leading-dictionary layout, a constant or unblocked column, or none by that name.
func (r *PartReader) ScanBound(ctx context.Context, name string, window int64) (n int64, ok bool) {
	i, ok := r.byName[name]
	if !ok {
		return 0, false
	}

	desc := r.manifest.Columns[i]
	if desc.Const || !desc.Blocked || !desc.Framed || !desc.HasSizing || (desc.SharedDict && !desc.TrailerDict) {
		return 0, false
	}

	sz := desc.Sizing

	n = 4 * (3*sz.NumFrames + 1 + 3*sz.NumGranules)
	n += desc.DictRaw + (binary.MaxVarintLen32+24)*desc.DictEntries

	switch {
	case !backend.RangesNatively(ctx, r.b, columnKey(r.prefix, i)):
		n += desc.Bytes
	case window > 0:
		// The window never reaches past the frames, which the object holds beside the dictionary
		// region and the directory.
		n += min(max(window, sz.MaxFrameBytes), max(desc.Bytes-desc.DictLen-sz.DirLen, sz.MaxFrameBytes))
	default:
		n += sz.MaxFrameBytes
	}

	n += 2 * (sz.MaxFrameRaw + int64(compress.OutputSlack(desc.Compress)))

	// The granule count is ceil(rows / granule rows), so a granule holds fewer than rows / (count-1).
	rows, granules := int64(r.manifest.RowCount), sz.NumGranules
	if granules > 1 {
		rows = rows/(granules-1) + 1
	}

	return n + rows*desc.Kind.decodedRowBytes(), true
}

// ResidentBytes bounds what a forward walk over the decoder holds at any point: the directory and
// shared dictionary, the read-ahead window (or the object, when the column was read whole), the
// decompressed frame buffer twice over while a larger frame replaces it, and one decoded granule. It
// is fixed when the decoder opens, so a caller may charge it before the walk touches a frame.
func (d *Decoder) ResidentBytes() int64 {
	dir := d.streams.dir
	n := dir.residentBytes() + d.shared.residentBytes()

	var maxFrame, maxRaw int64

	for f := 0; f+1 < len(dir.frameOff); f++ {
		maxFrame = max(maxFrame, int64(dir.frameOff[f+1]-dir.frameOff[f]))

		if dir.frameRaw != nil {
			maxRaw = max(maxRaw, int64(dir.frameRaw[f]))
		}
	}

	if dir.frameRaw == nil {
		maxRaw = max(dir.legacyMax, 0)
	}

	switch {
	case dir.data != nil:
		n += int64(len(dir.data))
	case dir.src != nil && dir.src.window > 0 && len(dir.frameOff) > 0:
		n += min(max(dir.src.window, maxFrame), int64(dir.frameOff[len(dir.frameOff)-1]))
	default:
		n += maxFrame
	}

	if d.streams.comp != nil {
		maxRaw += int64(d.streams.comp.OutputSlack())
	}

	return n + 2*maxRaw + int64(dir.blockRows)*d.kind.decodedRowBytes()
}

// decodedRowBytes is what one decoded row of the kind costs: its value, or for bytes an id and the
// view a granule's table holds per row at most.
func (k Kind) decodedRowBytes() int64 {
	switch k {
	case KindInt128:
		return 16
	case KindBytes:
		return 4 + 24
	default:
		return 8
	}
}

// TsCursor returns a forward cursor over an int64 timestamp column, walking its granules in order.
// It is [ColumnReader.TsCursor] over this decoder's frames, so under [PartReader.ColumnScan] a merge
// holds one read-ahead window of the column rather than its object.
func (d *Decoder) TsCursor() (chunk.TsCursor, error) {
	if d.kind != KindInt64 {
		return nil, errors.Errorf("block: column is %s, not int64", d.kind)
	}

	if d.codec != chunk.CodecDoD && d.codec != chunk.CodecDoDScaled {
		return nil, errors.Errorf("block: codec %s not a streamable timestamp codec", d.codec)
	}

	return newBlockedTsCursor(d.streams.dir, d.streams.comp, d.codec, d.rows), nil
}

// FloatCursor is the float64 analog of [Decoder.TsCursor].
func (d *Decoder) FloatCursor() (chunk.FloatDecoder, error) {
	if d.kind != KindFloat64 {
		return nil, errors.Errorf("block: column is %s, not float64", d.kind)
	}

	return newBlockedFloatCursor(d.streams.dir, d.streams.comp, d.codec, d.rows), nil
}

// readAhead serves frame f from the buffered run, refilling it when the frame is not covered. A
// forward walk therefore pays one request per window; a caller that seeks backwards or skips past
// the window refills, which is why this is not the query path's reader.
func (s *frameSource) readAhead(f int, off, n int64) ([]byte, error) {
	if off < s.lo || off+n > s.hi {
		if err := s.fill(f, off, n); err != nil {
			return nil, err
		}
	}

	return s.ahead[off-s.lo : off-s.lo+n], nil
}

// fill reads the longest run of whole frames starting at f that fits the window, always at least
// frame f — a frame larger than the window is still served, in one request of its own.
func (s *frameSource) fill(f int, off, n int64) error {
	hi := off + n

	for j := f + 1; j < len(s.frameOff)-1; j++ {
		end := int64(s.frameOff[j+1])
		if end-off > s.window {
			break
		}

		hi = end
	}

	// Dropped before the read, so the walk holds one window rather than two while it refills.
	s.ahead, s.lo, s.hi = nil, 0, 0

	buf, err := s.read(off, hi-off)
	if err != nil {
		return err
	}

	s.ahead, s.lo, s.hi = buf, off, hi

	return nil
}

// DecodeBytesBlock decodes one granule of a bytes column against the decoder's cached frame, which
// is what a forward walk wants: [Decoder.DecodeBytes] takes a block set because it merges their
// dictionaries, and merging is exactly what a caller consuming one granule at a time does not need.
//
// The result **aliases the decoder's frame buffer** and is valid only until the next decode. That is
// the point — a merge reads a granule's ids and appends them, so copying every value to hand it back
// would be the per-row cost this path exists to avoid. A caller that retains the column past its
// next call must copy it.
//
// For a granule on the column's shared dictionary the result carries that dictionary and the
// granule's ids unchanged, so its entries are the column's, not the granule's, and the token is the
// one [Decoder.SharedEntries] returns. Any other granule gets a token that dies at the next decode.
// The granule itself, whose ids alias the frame in either case, dies at the next decode too.
func (d *Decoder) DecodeBytesBlock(blk int) (DecodedGranule, error) {
	if d.kind != KindBytes {
		return DecodedGranule{}, errors.Errorf("block: column is %s, not bytes", d.kind)
	}

	dir := d.streams.dir

	if blk < 0 || blk >= dir.nBlocks() {
		return DecodedGranule{}, errors.Errorf("block: block %d out of range [0,%d)", blk, dir.nBlocks())
	}

	lo := blk * dir.blockRows
	if lo >= d.rows {
		return DecodedGranule{}, errors.Wrapf(ErrCorrupt, "block %d start %d past rows %d", blk, lo, d.rows)
	}

	n := min(lo+dir.blockRows, d.rows) - lo

	d.granules.retire()

	stream, err := d.streams.granule(blk)
	if err != nil {
		return DecodedGranule{}, err
	}

	if d.shared.on {
		ids, width, self, err := d.shared.granule(stream, n)
		if err != nil {
			return DecodedGranule{}, errors.Wrapf(err, "decode block %d", blk)
		}

		if !self {
			return d.granule(&chunk.DictColumn{Entries: d.shared.entries, IDs: ids, IDWidth: width}, d.sharedGen.token()), nil
		}

		stream = ids
	}

	var dc chunk.DictColumn

	if _, err := dc.DecodeBytes(stream); err != nil {
		return DecodedGranule{}, errors.Wrapf(err, "decode block %d", blk)
	}

	if dc.Len() != n {
		return DecodedGranule{}, errors.Wrapf(ErrCorrupt, "block %d decoded %d rows, want %d", blk, dc.Len(), n)
	}

	return d.granule(&dc, d.granules.token()), nil
}

func (d *Decoder) granule(dc *chunk.DictColumn, table DictGen) DecodedGranule {
	return DecodedGranule{dc: dc, table: table, lease: lease{g: d.granules.token(), dc: dc}}
}
