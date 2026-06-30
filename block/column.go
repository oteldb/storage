package block

import (
	"bytes"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/simd"
)

// decimalPrecisionLossless is the precision passed to the scaled-decimal codec when a
// float column opts into [chunk.CodecDecimal]: full precision (lossless).
const decimalPrecisionLossless = 64

// Column is an input column for a part: a name, a physical [Kind], and the matching
// typed slice (exactly one of Int64/Float64/Bytes per Kind). Codec and Compress are
// optional overrides; the zero values select the per-kind default codec and no block
// compression (the chunk codecs already compress well).
type Column struct {
	Name     string
	Kind     Kind
	Int64    []int64
	Float64  []float64
	Bytes    [][]byte
	Int128   []chunk.U128
	Codec    chunk.Codec
	Compress compress.Algorithm
	// AutoCodec, when set on a float64 column with no explicit Codec, picks the smaller of the
	// lossless float codecs (Gorilla XOR vs scaled-decimal+delta) by trial-encoding — so an
	// integer-valued or low-precision column (e.g. a counter) takes the far denser decimal path
	// while a high-entropy column keeps Gorilla. Lossless either way (see [chooseFloatCodec]).
	AutoCodec bool
	// FloatPrecisionBits, when in 1..63 on an AutoCodec float column, requests *lossy* encoding:
	// the scaled-decimal codec retains only this many significant mantissa bits (fewer ⇒ denser,
	// less accurate), competing against lossless Gorilla so the result is never worse than
	// lossless. 0 (or ≥64) means lossless. The budget is recorded in the descriptor so the merge
	// engine reaches a fixed point (it never re-applies a budget it has already met). Set per age
	// tier by the merge engine so only old data trades accuracy for size.
	FloatPrecisionBits uint8
	// Block requests block-framed encoding: the column is split into blockRows-row blocks (the
	// part's granule size), each an independent codec stream, so a reader can decode one block at a
	// time (sub-part seek). Only the per-row sequential codecs (DoD/T64 int64, Gorilla/decimal
	// float64) are blockable — the metric ts/value/sf columns. The zero value keeps the prior
	// single-stream layout. See blockcolumn.go.
	Block bool
}

// rows returns the column's row count from the active typed slice.
func (c Column) rows() int {
	switch c.Kind {
	case KindInt64:
		return len(c.Int64)
	case KindFloat64:
		return len(c.Float64)
	case KindBytes:
		return len(c.Bytes)
	case KindInt128:
		return len(c.Int128)
	default:
		return 0
	}
}

// defaultCodec is the codec used when [Column.Codec] is unset (CodecNone). The
// timestamp/sort column overrides this to [chunk.CodecDoD] via the part writer.
func defaultCodec(k Kind) chunk.Codec {
	switch k {
	case KindInt64:
		return chunk.CodecT64
	case KindFloat64:
		return chunk.CodecGorilla
	case KindInt128:
		return chunk.CodecID128
	default:
		return chunk.CodecDict
	}
}

// buildColumn computes a column's descriptor and serialized object. A constant column
// collapses to its descriptor with no object (the value lives in the manifest); every
// other column is a chunk-codec stream wrapped in comp's block frame. comp selects the
// block-compression algorithm recorded in the descriptor.
func buildColumn(c Column, comp *compress.Compressor, blockRows int) (ColumnDesc, []byte, error) {
	if !c.Kind.valid() {
		return ColumnDesc{}, nil, errors.Errorf("block: column %q has invalid kind %d", c.Name, c.Kind)
	}

	codec := c.Codec
	if codec == chunk.CodecNone {
		codec = defaultCodec(c.Kind)
	}

	desc := ColumnDesc{Name: c.Name, Kind: c.Kind, Codec: codec, Compress: comp.Algorithm()}

	switch c.Kind {
	case KindInt64:
		fillInt64Stats(&desc, c.Int64)
	case KindFloat64:
		fillFloat64Stats(&desc, c.Float64)
	case KindBytes:
		fillBytesConst(&desc, c.Bytes)
	case KindInt128:
		// No stats and never constant-collapsed: the RLE codec already shrinks a
		// single-id column to a handful of bytes, and id columns carry no min/max.
	}

	if desc.Const {
		return desc, nil, nil
	}

	// Adaptive float codec: when opted in and no codec was forced, pick the smaller encoding and
	// record the winner (and the lossy budget, if any) in the descriptor so the reader dispatches
	// correctly and the merge engine can reach its precision fixed point.
	budget := uint8(0)
	if c.AutoCodec && c.Kind == KindFloat64 && c.Codec == chunk.CodecNone {
		budget = c.FloatPrecisionBits
		if budget >= 64 {
			budget = 0 // ≥64 bits is lossless; normalize to the lossless sentinel
		}

		var stream []byte
		codec, stream = chooseFloatCodec(c.Float64, comp, budget)
		desc.Codec = codec
		desc.FloatPrecisionBits = budget

		if !c.Block {
			return desc, comp.Compress(nil, stream), nil
		}
	}

	if c.Block {
		desc.Blocked = true

		obj, err := encodeBlocked(c, codec, budget, comp, blockRows)
		if err != nil {
			return ColumnDesc{}, nil, err
		}

		return desc, obj, nil
	}

	stream, err := encodeStream(c, codec)
	if err != nil {
		return ColumnDesc{}, nil, err
	}

	return desc, comp.Compress(nil, stream), nil
}

// chooseFloatCodec picks the denser float encoding for vals, returning the winning codec and its
// encoded (pre-block-compression) stream. Gorilla XOR is always lossless and is the floor.
//
// budget == 0 selects the lossless regime: the scaled-decimal codec is taken only when it is
// strictly smaller after block compression AND a verification decode reproduces vals (so an
// integer/low-precision column compresses far better, never risking precision).
//
// budget in 1..63 selects the lossy regime requested by an age tier: the scaled-decimal codec
// retains only budget significant bits, trading accuracy for size. It is offered only for all-
// finite columns (it cannot represent NaN/±Inf) and chosen only when actually smaller than
// Gorilla — so even in a lossy tier a column that compresses better losslessly stays lossless.
// Sizes are compared post-compression via comp so the choice reflects the bytes actually written.
func chooseFloatCodec(vals []float64, comp *compress.Compressor, budget uint8) (chunk.Codec, []byte) {
	gorilla := chunk.EncodeFloats(nil, vals)
	bestStream, bestCodec := gorilla, chunk.CodecGorilla
	bestSize := len(comp.Compress(nil, gorilla))

	if budget == 0 {
		decimal := chunk.EncodeFloatsDecimal(nil, vals, decimalPrecisionLossless)
		if len(comp.Compress(nil, decimal)) < bestSize && decimalRoundTrips(decimal, vals) {
			bestStream, bestCodec = decimal, chunk.CodecDecimal
		}

		return bestCodec, bestStream
	}

	if floatsAllFinite(vals) {
		decimal := chunk.EncodeFloatsDecimal(nil, vals, budget)
		if len(comp.Compress(nil, decimal)) < bestSize {
			bestStream, bestCodec = decimal, chunk.CodecDecimal
		}
	}

	return bestCodec, bestStream
}

// floatsAllFinite reports whether vals contains no NaN or ±Inf — the precondition for the
// scaled-decimal codec, which cannot represent non-finite values.
func floatsAllFinite(vals []float64) bool {
	for _, v := range vals {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
	}

	return true
}

// decimalRoundTrips reports whether the scaled-decimal stream decodes back to vals with no loss
// of metric value. It is the lossless guard for the adaptive choice: a column the decimal codec
// cannot represent is left on Gorilla. The comparison is IEEE numeric equality, which rejects
// any real precision loss and (because NaN ≠ NaN) any NaN, while tolerating the single harmless
// divergence the codec can introduce — negative zero decoding to positive zero. −0 and +0 are
// numerically identical and indistinguishable to every metric query, so collapsing a spurious −0
// (typically a rounding artifact) to +0 is value-preserving and lets a whole column take the far
// denser decimal path instead of being poisoned by one −0.
func decimalRoundTrips(stream []byte, vals []float64) bool {
	got, _, err := chunk.DecodeFloatsDecimal(nil, stream)
	if err != nil || len(got) != len(vals) {
		return false
	}

	for i := range vals {
		if got[i] != vals[i] {
			return false
		}
	}

	return true
}

func encodeStream(c Column, codec chunk.Codec) ([]byte, error) {
	switch {
	case c.Kind == KindInt64 && codec == chunk.CodecDoD:
		return chunk.EncodeTimestamps(nil, c.Int64), nil
	case c.Kind == KindInt64 && codec == chunk.CodecT64:
		return chunk.EncodeIntsT64(nil, c.Int64), nil
	case c.Kind == KindFloat64 && codec == chunk.CodecGorilla:
		return chunk.EncodeFloats(nil, c.Float64), nil
	case c.Kind == KindFloat64 && codec == chunk.CodecDecimal:
		return chunk.EncodeFloatsDecimal(nil, c.Float64, decimalPrecisionLossless), nil
	case c.Kind == KindBytes && codec == chunk.CodecDict:
		return chunk.EncodeBytes(nil, c.Bytes), nil
	case c.Kind == KindBytes && codec == chunk.CodecBytesRaw:
		return chunk.EncodeBytesRaw(nil, c.Bytes), nil
	case c.Kind == KindInt128 && codec == chunk.CodecID128:
		return chunk.EncodeU128(nil, c.Int128), nil
	default:
		return nil, errors.Errorf("block: codec %s invalid for kind %s", codec, c.Kind)
	}
}

func fillInt64Stats(d *ColumnDesc, vals []int64) {
	if len(vals) == 0 {
		return
	}

	lo, hi := simd.MinMaxInt64(vals)

	d.MinInt64, d.MaxInt64 = lo, hi
	if lo == hi {
		d.Const = true
		d.ConstInt64 = lo
	}
}

func fillFloat64Stats(d *ColumnDesc, vals []float64) {
	if len(vals) == 0 {
		return
	}

	// All-same (Const) detection: bail on the first differing bit pattern, so varied data pays O(1)
	// here. An all-constant column is its own min and max (NaN included, matching the prior
	// behavior), so it needs no min/max scan.
	firstBits := math.Float64bits(vals[0])
	allSame := true

	for _, v := range vals[1:] {
		if math.Float64bits(v) != firstBits {
			allSame = false

			break
		}
	}

	if allSame {
		d.MinFloat64, d.MaxFloat64 = vals[0], vals[0]
		d.Const = true
		d.ConstFloat64 = vals[0]

		return
	}

	// Min/max ignoring NaN, vectorized (SIMD). The (+Inf, -Inf) sentinel (min > max) means every
	// value was NaN, for which there is no meaningful range — fall back to the first value.
	lo, hi := simd.MinMaxFloat64(vals)
	if lo > hi {
		lo, hi = vals[0], vals[0]
	}

	d.MinFloat64, d.MaxFloat64 = lo, hi
}

func fillBytesConst(d *ColumnDesc, vals [][]byte) {
	if len(vals) == 0 {
		return
	}

	for _, v := range vals[1:] {
		if !bytes.Equal(v, vals[0]) {
			return
		}
	}

	d.Const = true
	d.ConstBytes = append([]byte(nil), vals[0]...)
}

// ColumnReader gives lazy, decode-on-demand access to one column of a part. Constant
// columns are synthesized from the manifest with no I/O; other columns are decompressed
// and decoded only when an accessor is called (DESIGN.md §7: decode only what the query
// touches). It is created by [PartReader.Column].
type ColumnReader struct {
	desc   ColumnDesc
	object []byte // compress-framed stream; nil for a constant column
	comp   *compress.Compressor
	rows   int
}

func newColumnReader(desc ColumnDesc, object []byte, comp *compress.Compressor, rows int) *ColumnReader {
	return &ColumnReader{desc: desc, object: object, comp: comp, rows: rows}
}

// Kind reports the column's physical type.
func (r *ColumnReader) Kind() Kind { return r.desc.Kind }

// Len reports the column's row count.
func (r *ColumnReader) Len() int { return r.rows }

// Const returns the column's constant value (int64/float64/[]byte per Kind) and true if
// the column was constant-collapsed; otherwise (nil, false).
func (r *ColumnReader) Const() (any, bool) {
	if !r.desc.Const {
		return nil, false
	}

	switch r.desc.Kind {
	case KindInt64:
		return r.desc.ConstInt64, true
	case KindFloat64:
		return r.desc.ConstFloat64, true
	default:
		return r.desc.ConstBytes, true
	}
}

// Int64 decodes the column into dst (reusing its capacity) and returns the result.
// It errors if the column is not [KindInt64].
func (r *ColumnReader) Int64(dst []int64) ([]int64, error) {
	if r.desc.Kind != KindInt64 {
		return nil, errors.Errorf("block: column %q is %s, not int64", r.desc.Name, r.desc.Kind)
	}

	if r.desc.Const {
		return fillConst(dst, r.rows, r.desc.ConstInt64), nil
	}

	return decodeColumn(r, dst, r.int64Decoder())
}

// Float64 decodes the column into dst (reusing its capacity) and returns the result.
// It errors if the column is not [KindFloat64].
func (r *ColumnReader) Float64(dst []float64) ([]float64, error) {
	if r.desc.Kind != KindFloat64 {
		return nil, errors.Errorf("block: column %q is %s, not float64", r.desc.Name, r.desc.Kind)
	}

	if r.desc.Const {
		return fillConst(dst, r.rows, r.desc.ConstFloat64), nil
	}

	return decodeColumn(r, dst, r.float64Decoder())
}

// ID128 decodes the column into dst (reusing its capacity) and returns the result. It
// errors if the column is not [KindInt128]. Id columns are never constant-collapsed, so
// the value always comes from the decoded RLE stream.
func (r *ColumnReader) ID128(dst []chunk.U128) ([]chunk.U128, error) {
	if r.desc.Kind != KindInt128 {
		return nil, errors.Errorf("block: column %q is %s, not int128", r.desc.Name, r.desc.Kind)
	}

	stream, err := r.stream()
	if err != nil {
		return nil, err
	}

	out, _, err := chunk.DecodeU128(dst[:0], stream)

	return out, err
}

// fillConst materializes a constant column: n copies of v into dst (reusing capacity).
func fillConst[T any](dst []T, n int, v T) []T {
	dst = dst[:0]
	for range n {
		dst = append(dst, v)
	}

	return dst
}

// decodeColumn runs the selected typed decoder over the column. A blocked column decodes each block
// in turn (its object is a [blockDir] + per-block streams); an unblocked column decompresses its
// single stream and decodes it. A nil decoder means the descriptor's codec does not match the
// column's kind.
func decodeColumn[T any](r *ColumnReader, dst []T, dec func([]T, []byte) ([]T, int, error)) ([]T, error) {
	if dec == nil {
		return nil, errors.Errorf("block: codec %s invalid for column %q", r.desc.Codec, r.desc.Name)
	}

	if r.desc.Blocked {
		dir, err := parseBlockDir(r.object)
		if err != nil {
			return nil, errors.Wrapf(err, "column %q", r.desc.Name)
		}

		return decodeBlockedColumn(dir, r.comp, r.rows, dst, dec)
	}

	stream, err := r.stream()
	if err != nil {
		return nil, err
	}

	out, _, err := dec(dst[:0], stream)

	return out, err
}

// RangeInt64 decodes only rows [lo,hi) of an int64 column into a buffer reusing dst's capacity. For a
// blocked column it decodes just the blocks spanning the range (sub-part seek); for an unblocked one
// it decodes the whole column and slices, so callers get a uniform seek API regardless of layout.
// Requires 0 ≤ lo < hi ≤ Len().
func (r *ColumnReader) RangeInt64(dst []int64, lo, hi int) ([]int64, error) {
	if r.desc.Kind != KindInt64 {
		return nil, errors.Errorf("block: column %q is %s, not int64", r.desc.Name, r.desc.Kind)
	}

	return decodeRange(r, dst, lo, hi, r.desc.ConstInt64, r.int64Decoder())
}

// RangeFloat64 decodes only rows [lo,hi) of a float64 column. See [ColumnReader.RangeInt64].
func (r *ColumnReader) RangeFloat64(dst []float64, lo, hi int) ([]float64, error) {
	if r.desc.Kind != KindFloat64 {
		return nil, errors.Errorf("block: column %q is %s, not float64", r.desc.Name, r.desc.Kind)
	}

	return decodeRange(r, dst, lo, hi, r.desc.ConstFloat64, r.float64Decoder())
}

// decodeRange returns rows [lo,hi) of a column. A constant column repeats its value; a blocked column
// decodes only the spanning blocks; an unblocked column decodes fully and slices.
func decodeRange[T any](r *ColumnReader, dst []T, lo, hi int, constVal T, dec func([]T, []byte) ([]T, int, error)) ([]T, error) {
	if lo < 0 || hi <= lo || hi > r.rows {
		return nil, errors.Errorf("block: range [%d,%d) out of [0,%d) for column %q", lo, hi, r.rows, r.desc.Name)
	}

	if r.desc.Const {
		return fillConst(dst, hi-lo, constVal), nil
	}

	if dec == nil {
		return nil, errors.Errorf("block: codec %s invalid for column %q", r.desc.Codec, r.desc.Name)
	}

	if r.desc.Blocked {
		dir, err := parseBlockDir(r.object)
		if err != nil {
			return nil, errors.Wrapf(err, "column %q", r.desc.Name)
		}

		return decodeBlockedRange(dir, r.comp, lo, hi, dst, dec)
	}

	// Unblocked: decode the whole stream, then slice the requested rows.
	stream, err := r.stream()
	if err != nil {
		return nil, err
	}

	full, _, err := dec(dst[:0], stream)
	if err != nil {
		return nil, err
	}

	if hi > len(full) {
		return nil, errors.Wrapf(ErrCorrupt, "range [%d,%d) past decoded rows %d", lo, hi, len(full))
	}

	return full[lo:hi], nil
}

// Bytes decodes the column into its split [chunk.DictColumn] form (unique entries + a
// per-row id array), deferring the per-row gather to [chunk.DictColumn.At]. A constant
// column is synthesized as a single-entry dictionary. It errors if the column is not
// [KindBytes].
func (r *ColumnReader) Bytes() (*chunk.DictColumn, error) {
	if r.desc.Kind != KindBytes {
		return nil, errors.Errorf("block: column %q is %s, not bytes", r.desc.Name, r.desc.Kind)
	}

	if r.desc.Const {
		return &chunk.DictColumn{
			Entries: [][]byte{r.desc.ConstBytes},
			IDs:     make([]byte, r.rows), // all zero ⇒ every row maps to the single entry
			IDWidth: 1,
		}, nil
	}

	stream, err := r.stream()
	if err != nil {
		return nil, err
	}

	dc, _, err := chunk.DecodeBytesDict(stream)

	return dc, err
}

// TsCursor returns a forward cursor over a [KindInt64] timestamp column (delta-of-delta). A
// constant-collapsed column yields a repeating cursor. It is the streaming-merge form of Int64:
// same decode, but one row at a time so a merge holds only one series range resident per part.
func (r *ColumnReader) TsCursor() (chunk.TsCursor, error) {
	if r.desc.Kind != KindInt64 {
		return nil, errors.Errorf("block: column %q is %s, not int64", r.desc.Name, r.desc.Kind)
	}

	if r.desc.Const {
		return chunk.NewConstTsCursor(r.rows, r.desc.ConstInt64), nil
	}

	if r.desc.Codec != chunk.CodecDoD {
		return nil, errors.Errorf("block: codec %s not a streamable timestamp codec for %q", r.desc.Codec, r.desc.Name)
	}

	if r.desc.Blocked {
		dir, err := parseBlockDir(r.object)
		if err != nil {
			return nil, errors.Wrapf(err, "column %q", r.desc.Name)
		}

		return newBlockedTsCursor(dir, r.comp, r.rows), nil
	}

	stream, err := r.stream()
	if err != nil {
		return nil, err
	}

	return chunk.NewTsDecoder(stream)
}

// FloatCursor returns a forward cursor over a [KindFloat64] column (Gorilla or scaled-decimal). A
// constant-collapsed column yields a repeating cursor. It is the streaming-merge form of Float64.
func (r *ColumnReader) FloatCursor() (chunk.FloatDecoder, error) {
	if r.desc.Kind != KindFloat64 {
		return nil, errors.Errorf("block: column %q is %s, not float64", r.desc.Name, r.desc.Kind)
	}

	if r.desc.Const {
		return chunk.NewConstFloatDecoder(r.rows, r.desc.ConstFloat64), nil
	}

	if r.desc.Blocked {
		dir, err := parseBlockDir(r.object)
		if err != nil {
			return nil, errors.Wrapf(err, "column %q", r.desc.Name)
		}

		return newBlockedFloatCursor(dir, r.comp, r.desc.Codec, r.rows), nil
	}

	stream, err := r.stream()
	if err != nil {
		return nil, err
	}

	return chunk.NewFloatDecoder(r.desc.Codec, stream)
}

// Blocked reports whether the column is block-framed (and so supports DecodeBlocks*/BlockRows).
func (r *ColumnReader) Blocked() bool { return r.desc.Blocked }

// BlockRows returns the column's block size in rows, or 0 for an unblocked column.
func (r *ColumnReader) BlockRows() (int, error) {
	if !r.desc.Blocked {
		return 0, nil
	}

	dir, err := parseBlockDir(r.object)
	if err != nil {
		return 0, errors.Wrapf(err, "column %q", r.desc.Name)
	}

	return dir.blockRows, nil
}

// DecodeBlocksInt64 decodes only the given block indices of a blocked column into dst (reused), and
// returns a full-length (Len()) slice with those blocks' row spans populated — the rest of dst is
// left as-is (a caller reads only the rows it selected, which fall in the requested blocks). An
// unblocked/const column has no blocks to skip, so it decodes whole. blocks must be in range.
func (r *ColumnReader) DecodeBlocksInt64(dst []int64, blocks []int) ([]int64, error) {
	if !r.desc.Blocked {
		return r.Int64(dst)
	}

	dir, err := parseBlockDir(r.object)
	if err != nil {
		return nil, errors.Wrapf(err, "column %q", r.desc.Name)
	}

	out := growLen(dst, r.rows)

	return out, decodeBlocksInto(dir, r.comp, r.rows, out, blocks, r.int64Decoder())
}

// DecodeBlocksFloat64 is the float64 analog of [ColumnReader.DecodeBlocksInt64].
func (r *ColumnReader) DecodeBlocksFloat64(dst []float64, blocks []int) ([]float64, error) {
	if !r.desc.Blocked {
		return r.Float64(dst)
	}

	dir, err := parseBlockDir(r.object)
	if err != nil {
		return nil, errors.Wrapf(err, "column %q", r.desc.Name)
	}

	out := growLen(dst, r.rows)

	return out, decodeBlocksInto(dir, r.comp, r.rows, out, blocks, r.float64Decoder())
}

// growLen returns a slice of length n reusing dst's backing array when its capacity allows.
func growLen[T any](dst []T, n int) []T {
	if cap(dst) < n {
		return make([]T, n)
	}

	return dst[:n]
}

// int64Decoder returns the per-block typed decoder for this column's codec, or nil for a codec that
// is not an int64 codec (the caller reports the mismatch).
func (r *ColumnReader) int64Decoder() func([]int64, []byte) ([]int64, int, error) {
	switch r.desc.Codec {
	case chunk.CodecDoD:
		return chunk.DecodeTimestamps
	case chunk.CodecT64:
		return chunk.DecodeIntsT64
	default:
		return nil
	}
}

// float64Decoder returns the per-block typed decoder for this column's codec, or nil for a codec that
// is not a float64 codec (the caller reports the mismatch).
func (r *ColumnReader) float64Decoder() func([]float64, []byte) ([]float64, int, error) {
	switch r.desc.Codec {
	case chunk.CodecGorilla:
		return chunk.DecodeFloats
	case chunk.CodecDecimal:
		return chunk.DecodeFloatsDecimal
	default:
		return nil
	}
}

// stream decompresses the column's block frame into its raw codec stream.
func (r *ColumnReader) stream() ([]byte, error) {
	out, err := r.comp.Decompress(nil, r.object)
	if err != nil {
		return nil, errors.Wrapf(err, "decompress column %q", r.desc.Name)
	}

	return out, nil
}
