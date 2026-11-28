package recordengine

import (
	"bytes"
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// projectSparseHits compacts projected columns to hit order before the usual gather.
// Condition columns already belong to this fetch and stay live through the gather.
func (p *fetchPlan) projectSparseHits(
	ctx context.Context, part *part, lz *lazyCols, blocks []int, reserve func(int64) error,
) (bool, error) {
	hits := p.hits
	if len(hits) == 0 || len(hits) > part.reader.RowCount()/8 || !slices.IsSorted(hits) {
		return false, nil
	}
	// Dense matches keep the bulk decoder. Legacy columns may still use it individually.
	width := 8*int64(len(lz.ints)+1) + viewBytes*int64(len(lz.bytes))
	if err := reserve(fetchAdd(fetchMul(int64(len(hits)), width), 4096)); err != nil {
		return false, err
	}
	lz.ts = compactInts(lz.ts, hits)
	for k, selected := range p.sel.ints {
		if !selected {
			continue
		}
		if p.condSel.ints[k] {
			lz.ints[k] = compactInts(lz.ints[k], hits)
			continue
		}
		name := part.schema.intColumn(k).Name
		col, err := p.projectHitInts(ctx, part, name, blocks, reserve)
		if err != nil {
			return false, err
		}
		lz.ints[k] = col
	}
	for k, selected := range p.sel.bytes {
		if !selected {
			continue
		}
		cells := make([][]byte, len(hits))
		if p.condSel.bytes[k] {
			for i, row := range hits {
				cells[i] = lz.cell(k, row)
			}
		} else {
			name := part.schema.byteColumn(k).Name
			if err := p.projectHitBytes(ctx, part, name, blocks, cells, reserve); err != nil {
				return false, err
			}
		}
		lz.bytes[k] = &chunk.DictColumn{Entries: cells}
		lz.rawBlob[k] = rawBytesCol{}
	}
	// Each hit's ordinal is now its row in lz. hitRuns already describes slices of that list.
	for i := range hits {
		hits[i] = i
	}
	return true, nil
}

func compactInts(values []int64, hits []int) []int64 {
	out := make([]int64, len(hits))
	for i, row := range hits {
		out[i] = values[row]
	}
	return out
}

func (p *fetchPlan) projectHitInts(
	ctx context.Context, part *part, name string, blocks []int, reserve func(int64) error,
) ([]int64, error) {
	desc, _ := part.reader.ColumnDescByName(name)
	if desc.Const {
		out := make([]int64, len(p.hits))
		for i := range out {
			out[i] = desc.ConstInt64
		}
		return out, nil
	}
	d, release, err := openProjectionDecoder(ctx, part, p.memory, desc)
	if err != nil {
		return nil, err
	}
	if d == nil {
		if err := reserveBulkProjection(part, name, reserve); err != nil {
			return nil, err
		}
		whole, err := part.readInt64(ctx, name, nil, blocks)
		if err != nil {
			return nil, err
		}
		return compactInts(whole, p.hits), nil
	}
	defer release()
	out := make([]int64, len(p.hits))
	var scratch []int64
	err = visitProjectionBlocks(ctx, d, p.hits, func(blk, base, at int, rows []int) error {
		var err error
		scratch, err = d.DecodeInt64Into(blk, scratch[:0])
		if err != nil {
			return err
		}
		for i, row := range rows {
			out[at+i] = scratch[row-base]
		}
		return nil
	})
	return out, err
}

func (p *fetchPlan) projectHitBytes(
	ctx context.Context, part *part, name string, blocks []int, cells [][]byte, reserve func(int64) error,
) error {
	desc, _ := part.reader.ColumnDescByName(name)
	if desc.Const {
		for i := range cells {
			cells[i] = desc.ConstBytes
		}
		return nil
	}
	d, release, err := openProjectionDecoder(ctx, part, p.memory, desc)
	if err != nil {
		return err
	}
	if d == nil {
		if err := reserveBulkProjection(part, name, reserve); err != nil {
			return err
		}
		col, err := part.readDict(ctx, name, blocks)
		if err != nil {
			return err
		}
		for i, row := range p.hits {
			cells[i] = col.At(row)
		}
		return nil
	}
	defer release()
	return visitProjectionBlocks(ctx, d, p.hits, func(blk, base, at int, rows []int) error {
		granule, err := d.DecodeBytesBlock(blk)
		if err != nil {
			return err
		}
		col := granule.Column()
		for i, row := range rows {
			value := col.At(row - base)
			if err := reserve(fetchMul(2, int64(len(value)))); err != nil {
				return err
			}
			// Frame views expire on the next decode. Keep just the matching cells.
			cells[at+i] = bytes.Clone(value)
		}
		return nil
	})
}

func openProjectionDecoder(
	ctx context.Context, part *part, memory *fetchMemory, desc block.ColumnDesc,
) (*block.Decoder, func(), error) {
	// DecodeBytesBlock addresses fixed-row granules. Variable layouts and old columns use bulk decode.
	if desc.MaxBlockRows > 0 {
		return nil, nil, nil
	}
	steady, open, ok := part.reader.ScanBound(ctx, desc.Name, 0)
	if !ok {
		return nil, nil, nil
	}
	n := fetchAdd(steady, open)
	if desc.Compress != compress.AlgorithmNone {
		n = fetchAdd(n, compress.DecodeWorkspace)
	}
	if err := memory.reserve(n); err != nil {
		return nil, nil, err
	}
	d, err := part.reader.ColumnScan(ctx, desc.Name, 0)
	if err != nil {
		memory.release(n)
		return nil, nil, err
	}
	return d, func() {
		_ = d.Close()
		memory.release(n)
	}, nil
}

func reserveBulkProjection(part *part, name string, reserve func(int64) error) error {
	var size fetchDecodeSize
	if err := size.addColumn(part, name, false); err != nil {
		return err
	}
	return reserve(fetchAdd(size.resident, fetchAdd(size.sourceWide, size.workspace)))
}

func visitProjectionBlocks(
	ctx context.Context, d *block.Decoder, hits []int, visit func(blk, base, at int, rows []int) error,
) error {
	for at := 0; at < len(hits); {
		if err := ctx.Err(); err != nil {
			return err
		}
		blk, err := d.BlockForRow(hits[at])
		if err != nil {
			return err
		}
		lo, hi := d.BlockSpan(blk)
		if hits[at] < lo || hits[at] >= hi {
			return errors.Wrap(block.ErrCorrupt, "projection block does not contain matched row")
		}
		end := at + 1
		for end < len(hits) && hits[end] < hi {
			end++
		}
		if err := visit(blk, lo, at, hits[at:end]); err != nil {
			return err
		}
		at = end
	}
	return nil
}
