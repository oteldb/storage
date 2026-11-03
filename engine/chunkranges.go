package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
)

func (p *part) decodeChunkRanges(ctx context.Context, reuse *decodedPart, need colNeed, ranges []rowRange) (*decodedPart, error) {
	tc, err := p.reader.Column(ctx, colTs)
	if err != nil {
		return nil, err
	}
	if !tc.Blocked() {
		return p.decodeInto(ctx, reuse, need)
	}
	geometry, err := tc.BlockDecoder()
	if err != nil {
		return nil, err
	}
	var blocks []int
	for _, rng := range ranges {
		if rng.start >= rng.end {
			continue
		}
		first, err := geometry.BlockForRow(rng.start)
		if err != nil {
			return nil, err
		}
		last, err := geometry.BlockForRow(rng.end - 1)
		if err != nil {
			return nil, err
		}
		for b := first; b <= last; b++ {
			blocks = append(blocks, b)
		}
	}
	slices.Sort(blocks)
	blocks = slices.Compact(blocks)
	if reuse == nil {
		reuse = &decodedPart{}
	}
	reuse.ts, err = tc.DecodeBlocksInt64(reuse.ts, blocks)
	if err != nil {
		return nil, err
	}
	if need.values {
		reuse.vals, err = p.decodeChunkFloat(ctx, colValue, reuse.vals, blocks, geometry)
		if err != nil {
			return nil, err
		}
		if p.hasSF {
			reuse.sf, err = p.decodeChunkFloat(ctx, colSF, reuse.sf, blocks, geometry)
			if err != nil {
				return nil, err
			}
		}
	}
	reuse.haveValues = need.values
	return reuse, nil
}

func (p *part) decodeChunkFloat(ctx context.Context, name string, dst []float64, blocks []int, geometry *block.Decoder) ([]float64, error) {
	col, err := p.reader.Column(ctx, name)
	if err != nil {
		return nil, err
	}
	desc, _ := p.reader.ColumnDescByName(name)
	if desc.Const {
		return col.Float64(dst)
	}
	other, err := col.BlockDecoder()
	if err != nil {
		return nil, err
	}
	if other.NumBlocks() != geometry.NumBlocks() {
		return nil, errors.Wrap(block.ErrCorrupt, "numeric chunk count differs")
	}
	for _, b := range blocks {
		lo, hi := geometry.BlockSpan(b)
		a, z := other.BlockSpan(b)
		if lo != a || hi != z {
			return nil, errors.Wrap(block.ErrCorrupt, "numeric chunk rows differ")
		}
	}
	return col.DecodeBlocksFloat64(dst, blocks)
}
