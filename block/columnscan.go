package block

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
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
// The result **aliases the decoder's frame buffer** and is valid only until the next decode that
// crosses a frame boundary. That is the point — a merge reads a granule's ids and appends them, so
// copying every value to hand it back would be the per-row cost this path exists to avoid. A caller
// that retains the column past its next call must copy it.
//
// For a granule on the column's shared dictionary the result carries that dictionary and the
// granule's ids unchanged, so its entries are the column's, not the granule's, and the token is the
// one [Decoder.SharedEntries] returns. Any other granule gets a fresh token.
func (d *Decoder) DecodeBytesBlock(blk int) (*chunk.DictColumn, DictGen, error) {
	if d.kind != KindBytes {
		return nil, DictGen{}, errors.Errorf("block: column is %s, not bytes", d.kind)
	}

	dir := d.streams.dir

	if blk < 0 || blk >= dir.nBlocks() {
		return nil, DictGen{}, errors.Errorf("block: block %d out of range [0,%d)", blk, dir.nBlocks())
	}

	lo := blk * dir.blockRows
	if lo >= d.rows {
		return nil, DictGen{}, errors.Wrapf(ErrCorrupt, "block %d start %d past rows %d", blk, lo, d.rows)
	}

	n := min(lo+dir.blockRows, d.rows) - lo

	stream, err := d.streams.granule(blk)
	if err != nil {
		return nil, DictGen{}, err
	}

	if d.shared.on {
		ids, width, self, err := d.shared.granule(stream, n)
		if err != nil {
			return nil, DictGen{}, errors.Wrapf(err, "decode block %d", blk)
		}

		if !self {
			return &chunk.DictColumn{Entries: d.shared.entries, IDs: ids, IDWidth: width}, d.sharedGen, nil
		}

		stream = ids
	}

	var dc chunk.DictColumn

	if _, err := dc.DecodeBytes(stream); err != nil {
		return nil, DictGen{}, errors.Wrapf(err, "decode block %d", blk)
	}

	if dc.Len() != n {
		return nil, DictGen{}, errors.Wrapf(ErrCorrupt, "block %d decoded %d rows, want %d", blk, dc.Len(), n)
	}

	return &dc, NewDictGen(), nil
}
