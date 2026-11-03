package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/internal/memsize"
)

func (r *seriesBlockReader) ensureGeometry(ctx context.Context) error {
	if !r.part.variable || r.geometry != nil {
		return nil
	}
	for _, c := range []struct {
		name    string
		decoder **block.Decoder
	}{{colTs, &r.tsDec}, {colValue, &r.valDec}, {colSF, &r.sfDec}} {
		desc, ok := r.part.reader.ColumnDescByName(c.name)
		if !ok || desc.Const {
			continue
		}
		if desc.MaxBlockRows == 0 {
			return errors.Wrap(block.ErrCorrupt, "mixed numeric chunk geometry")
		}
		if *c.decoder == nil {
			d, err := r.partDecoder(ctx, c.name)
			if err != nil {
				return err
			}
			*c.decoder = d
		}
		r.geometry = *c.decoder
		return nil
	}
	return errors.Wrap(block.ErrCorrupt, "missing metric chunk geometry")
}

func (r *seriesBlockReader) rangeBlocks(ctx context.Context, rng rowRange) (int, int, error) {
	if !r.part.variable {
		return rng.start / r.blockRows, (rng.end - 1) / r.blockRows, nil
	}
	if err := r.ensureGeometry(ctx); err != nil {
		return 0, 0, err
	}
	first, err := r.geometry.BlockForRow(rng.start)
	if err != nil {
		return 0, 0, err
	}
	last, err := r.geometry.BlockForRow(rng.end - 1)
	if err != nil {
		return 0, 0, err
	}
	lo, _ := r.geometry.BlockSpan(first)
	_, hi := r.geometry.BlockSpan(last)
	if lo != rng.start || hi != rng.end {
		return 0, 0, errors.Wrap(block.ErrCorrupt, "chunk crosses series boundary")
	}
	return first, last, nil
}

func (r *seriesBlockReader) blockSpan(b int) (int, int) {
	if r.part.variable {
		return r.geometry.BlockSpan(b)
	}
	lo := b * r.blockRows
	return lo, min(lo+r.blockRows, r.part.rows())
}

func (r *seriesBlockReader) chunkInWindow(ctx context.Context, b int, start, end int64) bool {
	if !r.part.variable {
		return blockInWindow(r.granules(ctx), b, start, end)
	}
	if r.geometry == nil {
		return true
	}
	lo, hi, ok := r.geometry.BlockTimeBounds(b)
	return !ok || hi >= start && lo <= end
}

func (r *seriesBlockReader) checkColumnGeometry(ctx context.Context, decoder **block.Decoder, name string, b int) error {
	if err := r.ensureGeometry(ctx); err != nil {
		return err
	}
	desc, ok := r.part.reader.ColumnDescByName(name)
	if !ok {
		return errors.Wrap(block.ErrCorrupt, "missing numeric chunk column")
	}
	if desc.Const {
		return nil
	}
	if desc.MaxBlockRows == 0 {
		return errors.Wrap(block.ErrCorrupt, "missing variable numeric geometry")
	}
	if *decoder == nil {
		d, err := r.partDecoder(ctx, name)
		if err != nil {
			return err
		}
		*decoder = d
	}
	lo, hi := r.geometry.BlockSpan(b)
	otherLo, otherHi := (*decoder).BlockSpan(b)
	if (*decoder).NumBlocks() != r.geometry.NumBlocks() || lo != otherLo || hi != otherHi {
		return errors.Wrap(block.ErrCorrupt, "numeric chunk geometry differs")
	}
	return nil
}

func (r *seriesBlockReader) variableWindowBlocks(ctx context.Context, ranges []rowRange, start, end int64) ([]int, error) {
	if err := r.ensureGeometry(ctx); err != nil {
		return nil, err
	}
	var out []int
	for _, rng := range ranges {
		if rng.start >= rng.end {
			continue
		}
		first, last, err := r.rangeBlocks(ctx, rng)
		if err != nil {
			return nil, err
		}
		for b := first; b <= last; b++ {
			if r.chunkInWindow(ctx, b, start, end) {
				out = append(out, b)
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

func (p *enginePlan) variableDecodeEstimate(ctx context.Context, pt *part, ranges []rowRange, need colNeed) (int64, error) {
	total := int64(cap(ranges))*16 + memsize.Of[seriesBlockReader]()
	var selected int64
	for _, r := range ranges {
		selected += int64(r.end - r.start)
	}
	pinned := selected
	columns := []string{colTs}
	if need.values {
		columns = append(columns, colValue)
		if pt.hasSF {
			columns = append(columns, colSF)
		}
	}
	for _, name := range columns {
		desc, ok := pt.reader.ColumnDescByName(name)
		if !ok {
			return 0, errors.Wrap(block.ErrCorrupt, "missing chunk column")
		}
		if !desc.Const {
			retained, open, ok := pt.reader.ScanBound(ctx, name, 0)
			if !ok {
				return 0, errors.New("cannot bound variable chunk metadata")
			}
			total += retained + open + (4 << 10)
		}
	}
	// One reservation covers metadata discovery and the largest possible touched-block set.
	// No directory is opened while waiting for another reservation.
	chunks := pinned
	descriptors := pt.reader.Manifest().Columns
	for i := range descriptors {
		c := &descriptors[i]
		if c.MaxBlockRows > 0 {
			chunks = min(chunks, c.Sizing.NumGranules)
			break
		}
	}
	total += chunks * (8 + int64(len(columns))*(8+variableChunkEntryOverhead))
	total += pinned * 8 * int64(len(columns))
	if !p.countOnly {
		total += selected * 8 * int64(len(columns))
	}
	return total, nil
}

func variableMetricSchema(r *block.PartReader) (bool, error) {
	variable := false
	descriptors := r.Manifest().Columns
	for i := range descriptors {
		variable = variable || descriptors[i].MaxBlockRows > 0
	}
	if !variable {
		return false, nil
	}
	var chunks int64 = -1
	for _, name := range []string{colTs, colValue, colSF} {
		d, ok := r.ColumnDescByName(name)
		if !ok {
			if name == colSF {
				continue
			}
			return false, errors.Wrap(block.ErrCorrupt, "missing variable metric column")
		}
		if d.Const {
			continue
		}
		if d.MaxBlockRows == 0 || d.MaxBlockRows != r.Manifest().GranuleSize {
			return false, errors.Wrap(block.ErrCorrupt, "inconsistent variable metric schema")
		}
		if chunks >= 0 && chunks != d.Sizing.NumGranules {
			return false, errors.Wrap(block.ErrCorrupt, "numeric chunk counts differ")
		}
		chunks = d.Sizing.NumGranules
	}
	return true, nil
}

func (p *enginePlan) fallbackDecodeEstimate(ctx context.Context, pt *part, need colNeed, cols int64) (int64, error) {
	if !pt.variable {
		return int64(pt.rows()) * 8 * cols, nil
	}
	return p.variableDecodeEstimate(ctx, pt, []rowRange{{start: 0, end: pt.rows()}}, need)
}

func (r *seriesBlockReader) readTimestampChunk(ctx context.Context, b int) ([]int64, error) {
	desc, _ := r.part.reader.ColumnDescByName(colTs)
	if desc.Const {
		n, err := r.chunkRows(b)
		if err != nil {
			return nil, err
		}
		out := r.chunkI64Buffer(n)
		out = out[:n]
		for i := range out {
			out[i] = desc.ConstInt64
		}
		return out, nil
	}
	if r.tsDec == nil {
		d, err := r.partDecoder(ctx, colTs)
		if err != nil {
			return nil, err
		}
		r.tsDec = d
	}
	n, err := r.chunkRows(b)
	if err != nil {
		return nil, err
	}
	return r.tsDec.DecodeInt64Into(b, r.chunkI64Buffer(n))
}

func (r *seriesBlockReader) readFloatChunk(ctx context.Context, decoder **block.Decoder, name string, b int) ([]float64, error) {
	desc, _ := r.part.reader.ColumnDescByName(name)
	if desc.Const {
		n, err := r.chunkRows(b)
		if err != nil {
			return nil, err
		}
		out := r.chunkF64Buffer(n)
		out = out[:n]
		for i := range out {
			out[i] = desc.ConstFloat64
		}
		return out, nil
	}
	if *decoder == nil {
		d, err := r.partDecoder(ctx, name)
		if err != nil {
			return nil, err
		}
		*decoder = d
	}
	n, err := r.chunkRows(b)
	if err != nil {
		return nil, err
	}
	return (*decoder).DecodeFloat64Into(b, r.chunkF64Buffer(n))
}

// Map and LRU capacity is amortized alongside the entry and its exact-sized numeric buffer.
var variableChunkEntryOverhead = memsize.Of[blockEntry]() + 256

func (r *seriesBlockReader) chunkCacheBytes(capacity int) int64 {
	n := int64(capacity) * 8
	if r.part.variable {
		n += variableChunkEntryOverhead
	}
	return n
}

func (r *seriesBlockReader) chunkI64Buffer(n int) []int64 {
	if r.part.variable {
		return r.engine.blockCache.i64Free.getCapacity(n, true)
	}
	return r.engine.blockCache.getI64Buf(n)
}

func (r *seriesBlockReader) chunkF64Buffer(n int) []float64 {
	if r.part.variable {
		return r.engine.blockCache.f64Free.getCapacity(n, true)
	}
	return r.engine.blockCache.getF64Buf(n)
}

func (r *seriesBlockReader) chunkRows(b int) (int, error) {
	if r.part.variable {
		if r.geometry == nil || b < 0 || b >= r.geometry.NumBlocks() {
			return 0, errors.Wrap(block.ErrCorrupt, "chunk index outside directory")
		}
	} else {
		rows := r.part.rows()
		if rows <= 0 || r.blockRows <= 0 || b < 0 || b > (rows-1)/r.blockRows {
			return 0, errors.Wrap(block.ErrCorrupt, "block index outside part")
		}
	}
	lo, hi := r.blockSpan(b)
	if lo < 0 || hi <= lo || hi > r.part.rows() {
		return 0, errors.Wrap(block.ErrCorrupt, "invalid chunk span")
	}
	return hi - lo, nil
}
