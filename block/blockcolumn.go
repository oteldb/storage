package block

import (
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// errCursorEOF is returned by a blocked cursor's Next when every block has been consumed. The
// streaming merge drives each cursor with a known row count and never over-reads, so this only
// surfaces a caller bug.
var errCursorEOF = errors.New("block: blocked cursor exhausted")

// Block-framed columns split a column into fixed-size row blocks, each an independent codec stream
// (the chunk codecs reset their running state — delta-of-delta, Gorilla leading/trailing — at row 0
// of every Encode call, so a block over rows [lo,hi) is decodable without touching any earlier row).
// A small directory records each block's byte length, so a reader can locate and decode a single
// block — the basis for sub-part seek (decode only the blocks a query's time window or matched
// series' row range touches) without decoding the whole column.
//
// Object layout (before per-block compression is applied to each stream, after the directory):
//
//	[uvarint nBlocks][uvarint blockRows][nBlocks × uvarint blockLen][block0][block1]…
//
// blockRows is the nominal rows per block (the last block may hold fewer); row r lives in block
// r/blockRows. Each blockN is comp.Compress(codecStream(rows of that block)). The block boundaries
// align with the part's marks granules (same size), so the marks index already carries each block's
// [minTime,maxTime] for time-pruning — the directory need not repeat it.
//
// A blocked column is marked by [flagBlocked] in its descriptor; an unblocked column keeps the prior
// single-stream layout byte-for-byte, so existing parts read unchanged.

// encodeBlocked serializes c as a block-framed column: each blockRows-row slice is codec-encoded
// (codec, with the decimal precision budget for CodecDecimal) and block-compressed independently,
// preceded by the directory. blockRows must be > 0.
func encodeBlocked(c Column, codec chunk.Codec, budget uint8, comp *compress.Compressor, blockRows int) ([]byte, error) {
	if blockRows <= 0 {
		return nil, errors.Errorf("block: blockRows must be > 0, got %d", blockRows)
	}

	n := c.rows()
	nBlocks := (n + blockRows - 1) / blockRows

	blocks := make([][]byte, 0, nBlocks)
	for lo := 0; lo < n; lo += blockRows {
		hi := min(lo+blockRows, n)

		stream, err := encodeBlockStream(c, codec, budget, lo, hi)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, comp.Compress(nil, stream))
	}

	// Directory: counts then per-block lengths, all uvarint.
	var dst []byte
	dst = binary.AppendUvarint(dst, uint64(nBlocks))
	dst = binary.AppendUvarint(dst, uint64(blockRows))

	for _, b := range blocks {
		dst = binary.AppendUvarint(dst, uint64(len(b)))
	}

	for _, b := range blocks {
		dst = append(dst, b...)
	}

	return dst, nil
}

// encodeBlockStream codec-encodes c's rows [lo,hi) into a single chunk stream. Only the per-row
// sequential codecs used by the metric ts/value/sf columns are blockable; other codecs error.
func encodeBlockStream(c Column, codec chunk.Codec, budget uint8, lo, hi int) ([]byte, error) {
	switch {
	case c.Kind == KindInt64 && codec == chunk.CodecDoD:
		return chunk.EncodeTimestamps(nil, c.Int64[lo:hi]), nil
	case c.Kind == KindInt64 && codec == chunk.CodecT64:
		return chunk.EncodeIntsT64(nil, c.Int64[lo:hi]), nil
	case c.Kind == KindFloat64 && codec == chunk.CodecGorilla:
		return chunk.EncodeFloats(nil, c.Float64[lo:hi]), nil
	case c.Kind == KindFloat64 && codec == chunk.CodecDecimal:
		return chunk.EncodeFloatsDecimal(nil, c.Float64[lo:hi], decimalPrecision(budget)), nil
	default:
		return nil, errors.Errorf("block: codec %s for kind %s is not blockable", codec, c.Kind)
	}
}

// decimalPrecision maps a lossy precision budget (0 ⇒ lossless) to the bit count the scaled-decimal
// codec takes, mirroring the unblocked path (which uses [decimalPrecisionLossless] for lossless).
func decimalPrecision(budget uint8) uint8 {
	if budget == 0 || budget >= decimalPrecisionLossless {
		return decimalPrecisionLossless
	}

	return budget
}

// blockDir is a parsed block directory: the per-block byte spans within the data region.
type blockDir struct {
	blockRows int
	offsets   []int // cumulative byte offsets into data; len == nBlocks+1
	data      []byte
}

// nBlocks returns the number of blocks.
func (d blockDir) nBlocks() int { return len(d.offsets) - 1 }

// block returns block i's raw (still block-compressed) bytes.
func (d blockDir) block(i int) []byte { return d.data[d.offsets[i]:d.offsets[i+1]] }

// parseBlockDir reads the directory from a blocked column object and returns the per-block spans. It
// bounds-checks every field against the object length so a corrupt object errors rather than panics.
func parseBlockDir(object []byte) (blockDir, error) {
	nBlocks64, n := binary.Uvarint(object)
	if n <= 0 {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir nBlocks")
	}

	pos := n

	blockRows64, n := binary.Uvarint(object[pos:])
	if n <= 0 {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir blockRows")
	}

	if blockRows64 == 0 {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir blockRows is 0")
	}

	pos += n

	nBlocks := int(nBlocks64)
	// Each block length is ≥1 byte of directory, so the count cannot exceed the object length.
	if nBlocks64 > uint64(len(object)) {
		return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir nBlocks %d exceeds object", nBlocks64)
	}

	offsets := make([]int, nBlocks+1)

	total := 0

	for i := range nBlocks {
		l64, n := binary.Uvarint(object[pos:])
		if n <= 0 {
			return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir len %d", i)
		}

		pos += n

		// Bound each length and the running total against the object so a corrupt uvarint cannot
		// overflow int(l64) into a negative span (which would panic the data slice below).
		if l64 > uint64(len(object)) {
			return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir len %d too large", i)
		}

		total += int(l64)
		if total < 0 || total > len(object) {
			return blockDir{}, errors.Wrapf(ErrCorrupt, "block dir data %d exceeds object", i)
		}

		offsets[i+1] = total
	}

	if pos+total > len(object) {
		return blockDir{}, errors.Wrap(ErrCorrupt, "block dir data exceeds object")
	}

	return blockDir{blockRows: int(blockRows64), offsets: offsets, data: object[pos : pos+total]}, nil
}

// decodeBlockedColumn decodes every block of a blocked column into dst (sized to rows) in place: each
// block decodes directly into its row span of dst, so the whole-column path adds no per-row copy over
// the single-stream path. dec is the per-block typed decoder (DecodeTimestamps, DecodeFloats, …).
func decodeBlockedColumn[T any](
	dir blockDir, comp *compress.Compressor, rows int, dst []T, dec func([]T, []byte) ([]T, int, error),
) ([]T, error) {
	out := dst[:0]
	if cap(out) < rows {
		out = make([]T, 0, rows)
	}

	out = out[:rows]

	base := 0

	for i := range dir.nBlocks() {
		stream, err := comp.Decompress(nil, dir.block(i))
		if err != nil {
			return nil, errors.Wrapf(err, "decompress block %d", i)
		}

		// cap(out[base:]) == rows-base ≥ this block's row count (blocks partition [0,rows)), so the
		// decoder fills out[base:base+blkRows] in place without reallocating.
		sub, _, err := dec(out[base:base], stream)
		if err != nil {
			return nil, errors.Wrapf(err, "decode block %d", i)
		}

		base += len(sub)
		if base > rows {
			return nil, errors.Wrapf(ErrCorrupt, "block %d overran row count %d", i, rows)
		}
	}

	return out[:base], nil
}

// decodeBlocksInto decodes the given block indices into their row spans of out (sized to rows),
// leaving rows outside those blocks untouched. It is the engine's series-skip primitive: decode only
// the blocks a query's matched series touch.
func decodeBlocksInto[T any](
	dir blockDir, comp *compress.Compressor, rows int, out []T, blocks []int, dec func([]T, []byte) ([]T, int, error),
) error {
	if dec == nil {
		return errors.New("block: nil decoder")
	}

	for _, b := range blocks {
		if b < 0 || b >= dir.nBlocks() {
			return errors.Errorf("block: block %d out of range [0,%d)", b, dir.nBlocks())
		}

		lo := b * dir.blockRows
		// A corrupt/mismatched directory can place a block past the destination's row count; guard
		// before slicing out[lo:] so a bad object errors rather than panicking.
		if lo >= rows {
			return errors.Wrapf(ErrCorrupt, "block %d start %d past rows %d", b, lo, rows)
		}

		hi := min(lo+dir.blockRows, rows)

		stream, err := comp.Decompress(nil, dir.block(b))
		if err != nil {
			return errors.Wrapf(err, "decompress block %d", b)
		}

		// cap(out[lo:]) == rows-lo ≥ this block's row count, so the decoder fills out[lo:hi] in place.
		sub, _, err := dec(out[lo:lo], stream)
		if err != nil {
			return errors.Wrapf(err, "decode block %d", b)
		}

		if len(sub) != hi-lo {
			return errors.Wrapf(ErrCorrupt, "block %d decoded %d rows, want %d", b, len(sub), hi-lo)
		}
	}

	return nil
}

// decodeBlockedRange decodes only the blocks spanning rows [lo,hi) and returns that row range. It is
// the seek primitive: a query touching a fraction of a column decodes a fraction of its blocks. The
// result is a view into a buffer reusing dst's capacity; lo/hi must satisfy 0 ≤ lo < hi ≤ rows.
func decodeBlockedRange[T any](
	dir blockDir, comp *compress.Compressor, lo, hi int, dst []T, dec func([]T, []byte) ([]T, int, error),
) ([]T, error) {
	if lo < 0 || hi <= lo {
		return nil, errors.Errorf("block: bad range [%d,%d)", lo, hi)
	}

	first := lo / dir.blockRows
	last := (hi - 1) / dir.blockRows

	if last >= dir.nBlocks() {
		return nil, errors.Wrapf(ErrCorrupt, "range [%d,%d) past blocks", lo, hi)
	}

	out := dst[:0]

	var scratch []T

	for i := first; i <= last; i++ {
		stream, err := comp.Decompress(nil, dir.block(i))
		if err != nil {
			return nil, errors.Wrapf(err, "decompress block %d", i)
		}

		scratch, _, err = dec(scratch, stream)
		if err != nil {
			return nil, errors.Wrapf(err, "decode block %d", i)
		}

		out = append(out, scratch...)
	}

	// out now holds rows [first*blockRows, …); slice out the requested window within it.
	relLo := lo - first*dir.blockRows

	relHi := hi - first*dir.blockRows
	if relHi > len(out) {
		return nil, errors.Wrapf(ErrCorrupt, "range [%d,%d) past decoded rows", lo, hi)
	}

	return out[relLo:relHi], nil
}

// blockedTsCursor is a forward [chunk.TsCursor] over a blocked int64 column: it decodes one block at
// a time, opening the next block when the current is exhausted, so it spans block boundaries
// transparently. Each block is an independent codec stream (its row 0 is absolute), so crossing a
// boundary just starts a fresh per-block decoder — no cross-block state.
type blockedTsCursor struct {
	dir  blockDir
	comp *compress.Compressor
	rows int
	pos  int
	blk  int            // index of the open block; -1 before the first
	cur  chunk.TsCursor // decoder for block blk; advanced past its end opens the next
}

func newBlockedTsCursor(dir blockDir, comp *compress.Compressor, rows int) *blockedTsCursor {
	return &blockedTsCursor{dir: dir, comp: comp, rows: rows, blk: -1}
}

func (c *blockedTsCursor) Len() int { return c.rows }
func (c *blockedTsCursor) Pos() int { return c.pos }

func (c *blockedTsCursor) Next() (int64, error) {
	for c.cur == nil || c.cur.Pos() >= c.cur.Len() {
		c.blk++
		if c.blk >= c.dir.nBlocks() {
			return 0, errCursorEOF
		}

		stream, err := c.comp.Decompress(nil, c.dir.block(c.blk))
		if err != nil {
			return 0, errors.Wrapf(err, "decompress block %d", c.blk)
		}

		c.cur, err = chunk.NewTsDecoder(stream)
		if err != nil {
			return 0, errors.Wrapf(err, "block %d", c.blk)
		}
	}

	v, err := c.cur.Next()
	if err != nil {
		return 0, err
	}

	c.pos++

	return v, nil
}

// blockedFloatCursor is the float64 analog of [blockedTsCursor], over a blocked Gorilla/decimal
// column.
type blockedFloatCursor struct {
	dir   blockDir
	comp  *compress.Compressor
	codec chunk.Codec
	rows  int
	pos   int
	blk   int
	cur   chunk.FloatDecoder
}

func newBlockedFloatCursor(dir blockDir, comp *compress.Compressor, codec chunk.Codec, rows int) *blockedFloatCursor {
	return &blockedFloatCursor{dir: dir, comp: comp, codec: codec, rows: rows, blk: -1}
}

func (c *blockedFloatCursor) Len() int { return c.rows }
func (c *blockedFloatCursor) Pos() int { return c.pos }

func (c *blockedFloatCursor) Next() (float64, error) {
	for c.cur == nil || c.cur.Pos() >= c.cur.Len() {
		c.blk++
		if c.blk >= c.dir.nBlocks() {
			return 0, errCursorEOF
		}

		stream, err := c.comp.Decompress(nil, c.dir.block(c.blk))
		if err != nil {
			return 0, errors.Wrapf(err, "decompress block %d", c.blk)
		}

		c.cur, err = chunk.NewFloatDecoder(c.codec, stream)
		if err != nil {
			return 0, errors.Wrapf(err, "block %d", c.blk)
		}
	}

	v, err := c.cur.Next()
	if err != nil {
		return 0, err
	}

	c.pos++

	return v, nil
}
