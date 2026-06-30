package engine

import (
	"context"

	"github.com/oteldb/storage/block"
)

// assembleFromBlocks fills dp's columns for the matched series' row ranges from the cross-fetch block
// cache, decoding (and caching) only the blocks not already resident. dp's arrays are sized to the
// part's full row count, with the touched blocks' row spans populated; the caller reads only the
// matched series' rows, which lie in those blocks. A legacy unblocked part (no granule-aligned blocks)
// falls back to a whole-column decode.
func (e *Engine) assembleFromBlocks(ctx context.Context, dp *decodedPart, p *part, need colNeed, ranges []rowRange) error {
	blockRows := p.reader.Manifest().GranuleSize

	tsDesc, ok := p.reader.ColumnDescByName(colTs)
	if !ok || blockRows <= 0 || !tsDesc.Blocked {
		_, err := p.decodeRangesInto(ctx, dp, need, ranges)

		return err
	}

	rows := p.rows()
	blocks := neededBlocks(ranges, blockRows, rows)

	ts := growLen(dp.ts, rows)
	if err := e.fillIntBlocks(ctx, ts, p, colTs, colTsID, blocks, blockRows); err != nil {
		return err
	}

	dp.ts, dp.haveValues = ts, need.values

	if !need.values {
		return nil
	}

	vals, err := e.fillFloatColumn(ctx, dp.vals, p, colValue, colValID, blocks, blockRows, rows)
	if err != nil {
		return err
	}

	dp.vals = vals

	if !p.hasSF {
		dp.sf = nil

		return nil
	}

	sf, err := e.fillFloatColumn(ctx, dp.sf, p, colSF, colSFID, blocks, blockRows, rows)
	if err != nil {
		return err
	}

	dp.sf = sf

	return nil
}

// fillIntBlocks copies each needed block of an int64 column into its row span of dst, taking it from
// the block cache or decoding and caching it on a miss. The column object is read (and a BlockDecoder
// built) lazily — only when a block misses — so a full cache hit reads nothing from the backend.
func (e *Engine) fillIntBlocks(ctx context.Context, dst []int64, p *part, name string, cid colID, blocks []int, blockRows int) error {
	var bd *block.Decoder

	for _, blk := range blocks {
		key := blockKey{prefix: p.prefix, col: cid, blk: blk}
		if ent, ok := e.blockCache.get(key); ok {
			copy(dst[blk*blockRows:], ent.i64)

			continue
		}

		if bd == nil {
			d, err := e.partBlockDecoder(ctx, p, name)
			if err != nil {
				return err
			}

			bd = d
		}

		b, err := bd.DecodeInt64(blk)
		if err != nil {
			return err
		}

		e.blockCache.put(&blockEntry{key: key, i64: b, bytes: int64(len(b)) * 8})
		copy(dst[blk*blockRows:], b)
	}

	return nil
}

// fillFloatColumn returns a full-length slice for a float64 column with the needed blocks populated. A
// constant column is synthesized from the manifest with no I/O; a legacy unblocked column decodes
// whole; a blocked column is assembled from the block cache like [Engine.fillIntBlocks].
func (e *Engine) fillFloatColumn(
	ctx context.Context, dst []float64, p *part, name string, cid colID, blocks []int, blockRows, rows int,
) ([]float64, error) {
	desc, ok := p.reader.ColumnDescByName(name)
	if !ok {
		return growLen(dst, rows), nil
	}

	if desc.Const {
		out := growLen(dst, rows)
		for i := range out {
			out[i] = desc.ConstFloat64
		}

		return out, nil
	}

	if !desc.Blocked {
		col, err := p.reader.Column(ctx, name)
		if err != nil {
			return nil, err
		}

		return col.Float64(dst[:0])
	}

	out := growLen(dst, rows)

	var bd *block.Decoder

	for _, blk := range blocks {
		key := blockKey{prefix: p.prefix, col: cid, blk: blk}
		if ent, ok := e.blockCache.get(key); ok {
			copy(out[blk*blockRows:], ent.f64)

			continue
		}

		if bd == nil {
			d, err := e.partBlockDecoder(ctx, p, name)
			if err != nil {
				return nil, err
			}

			bd = d
		}

		b, err := bd.DecodeFloat64(blk)
		if err != nil {
			return nil, err
		}

		e.blockCache.put(&blockEntry{key: key, f64: b, bytes: int64(len(b)) * 8})
		copy(out[blk*blockRows:], b)
	}

	return out, nil
}

// partBlockDecoder reads the named column's object and returns a per-block decoder over it.
func (e *Engine) partBlockDecoder(ctx context.Context, p *part, name string) (*block.Decoder, error) {
	col, err := p.reader.Column(ctx, name)
	if err != nil {
		return nil, err
	}

	return col.BlockDecoder()
}
