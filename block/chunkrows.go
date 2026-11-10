package block

import (
	"encoding/binary"
	"sort"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/bitstream"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func (d blockDir) span(blk, rows int) (int, int) {
	if blk < 0 {
		return rows, rows
	}
	if d.rowStarts != nil {
		if blk+1 >= len(d.rowStarts) {
			return rows, rows
		}
		return int(d.rowStarts[blk]), int(d.rowStarts[blk+1])
	}
	lo := blk * d.blockRows
	return lo, min(lo+d.blockRows, rows)
}

func (d blockDir) blockAt(row int) int {
	if d.rowStarts == nil {
		return row / d.blockRows
	}
	return sort.Search(len(d.rowStarts), func(i int) bool { return d.rowStarts[i] > int64(row) }) - 1
}

func readChunkRows(c *dirCursor, d *blockDir, chk dirCheck) error {
	if !chk.checked {
		return errors.Wrap(ErrCorrupt, "variable chunks require checksums")
	}
	if len(d.frameRaw) != d.granules {
		return errors.Wrap(ErrCorrupt, "chunks must use independent frames")
	}
	for i, f := range d.gFrame {
		if int(f) != i {
			return errors.Wrap(ErrCorrupt, "chunks share a compression frame")
		}
	}
	d.rowStarts = make([]int64, d.granules+1)
	if chk.timeBounds {
		d.minKeys = make([]int64, d.granules)
		d.maxKeys = make([]int64, d.granules)
	}
	var sum, previous int64
	for i := range d.granules {
		n, err := c.uvarint("chunk rows")
		if err != nil {
			return err
		}
		if n == 0 || n > uint64(chk.maxBlockRows) || uint64(sum)+n > maxPartRows {
			return errors.Wrap(ErrCorrupt, "invalid chunk row count")
		}
		sum += int64(n)
		d.rowStarts[i+1] = sum
		if chk.timeBounds {
			delta, err := c.varint("chunk minimum")
			if err != nil {
				return err
			}
			span, err := c.varint("chunk maximum")
			if err != nil {
				return err
			}
			lo := previous + delta
			hi := lo + span
			if lo > hi {
				return errors.Wrap(ErrCorrupt, "invalid chunk time bounds")
			}
			d.minKeys[i], d.maxKeys[i], previous = lo, hi, lo
		}
	}
	if chk.rows > 0 && sum != int64(chk.rows) {
		return errors.Wrap(ErrCorrupt, "chunk rows differ from manifest")
	}
	return nil
}

// BlockForRow returns the chunk containing row.
func (d *Decoder) BlockForRow(row int) (int, error) {
	if row < 0 || row >= d.rows {
		return 0, errors.New("block: row outside column")
	}
	return d.streams.dir.blockAt(row), nil
}

// EndGroup completes one streaming row group without sharing its chunks with another group.
func (w *StreamWriter) EndGroup() error {
	if err := w.flushEncodedRun(); err != nil {
		return err
	}
	if !w.independentGroups {
		return errors.New("block: independent groups are disabled")
	}
	rows := w.Rows()
	for _, c := range w.cols {
		if c.rows != rows {
			return errors.New("block: group columns have different row counts")
		}
	}
	for _, c := range w.cols {
		if len(c.stageI64) > 0 {
			if err := c.flushGranuleInt64(c.stageI64); err != nil {
				return err
			}
			c.stageI64 = c.stageI64[:0]
		}
		if len(c.stageF64) > 0 {
			if err := c.flushGranuleFloat64(c.stageF64, 0); err != nil {
				return err
			}
			c.stageF64 = c.stageF64[:0]
			c.groupStarts = c.groupStarts[:0]
		}
	}
	return nil
}

func (c *streamColumn) sealChunk(rows int) error {
	if !c.variable {
		return nil
	}
	c.blk.chunkRows = append(c.blk.chunkRows, rows)
	if err := c.blk.seal(); err != nil {
		return err
	}
	if c.alt != nil && c.altOK {
		c.alt.chunkRows = append(c.alt.chunkRows, rows)
		return c.alt.seal()
	}
	return nil
}

func (c *ColumnDesc) decodeVariableRows(r *bitstream.Reader) error {
	n, err := r.ReadUvarint()
	if err != nil || n == 0 || n > chunk.MaxFloatGroupRows {
		return errors.Wrap(ErrCorrupt, "invalid variable row cap")
	}
	if !c.Blocked || !c.Framed || c.SharedDict || !c.HasSizing || c.Const {
		return errors.Wrap(ErrCorrupt, "invalid variable chunk flags")
	}
	if c.Kind != KindInt64 && c.Kind != KindFloat64 {
		return errors.Wrap(ErrCorrupt, "invalid variable chunk kind")
	}
	c.MaxBlockRows = int(n)
	return nil
}

func (w *StreamWriter) checkIndependentColumn(c Column) error {
	if !w.independentGroups || c.Kind == KindInt128 {
		return nil
	}
	if !c.Block || (c.Kind != KindInt64 && c.Kind != KindFloat64) {
		return errors.New("block: independent groups require blocked numeric columns")
	}
	if w.granuleSize <= 0 || w.granuleSize > chunk.MaxFloatGroupRows {
		return errors.New("block: independent groups require bounded granules")
	}
	return nil
}

func (c *streamColumn) setIndependentGroups(w *StreamWriter) {
	if !c.variable || c.blk == nil {
		return
	}
	c.blk.dirComp = w.compressorFor(compress.AlgorithmLZ4)
	c.blk.variable = true
	c.blk.timeBounds = c.timeBounds
	if c.alt != nil {
		c.alt.variable = true
		c.alt.dirComp = c.blk.dirComp
	}
}

func (c *streamColumn) setChunkLayout(desc *ColumnDesc, rows int) {
	desc.Blocked, desc.Framed = true, true
	if c.variable {
		desc.MaxBlockRows = rows
		desc.TimeBounds = c.timeBounds
	}
}

func (c *dirCursor) varint(field string) (int64, error) {
	v, n := binary.Varint(c.object[c.pos:])
	if n <= 0 {
		return 0, errors.Wrap(ErrCorrupt, field)
	}
	c.pos += n
	return v, nil
}

// BlockTimeBounds returns independently checked int64 chunk bounds when recorded.
func (d *Decoder) BlockTimeBounds(b int) (int64, int64, bool) {
	directory := d.streams.dir
	if b < 0 || b >= len(directory.minKeys) {
		return 0, 0, false
	}
	return directory.minKeys[b], directory.maxKeys[b], true
}

func (c *ColumnDesc) decodeChunkOptions(r *bitstream.Reader, x byte) error {
	if x&xVariable != 0 {
		if err := c.decodeVariableRows(r); err != nil {
			return err
		}
	}
	if x&xCompactDir != 0 {
		if c.MaxBlockRows == 0 {
			return errors.Wrap(ErrCorrupt, "compact directory requires variable chunks")
		}
		c.CompactDir = true
	}
	if x&xTimeBounds != 0 {
		if c.MaxBlockRows == 0 || c.Kind != KindInt64 {
			return errors.Wrap(ErrCorrupt, "invalid chunk time bounds")
		}
		c.TimeBounds = true
	}
	return nil
}
