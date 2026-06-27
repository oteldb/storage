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
func buildColumn(c Column, comp *compress.Compressor) (ColumnDesc, []byte, error) {
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
	if c.AutoCodec && c.Kind == KindFloat64 && c.Codec == chunk.CodecNone {
		budget := c.FloatPrecisionBits
		if budget >= 64 {
			budget = 0 // ≥64 bits is lossless; normalize to the lossless sentinel
		}

		var stream []byte
		codec, stream = chooseFloatCodec(c.Float64, comp, budget)
		desc.Codec = codec
		desc.FloatPrecisionBits = budget

		return desc, comp.Compress(nil, stream), nil
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

	firstBits := math.Float64bits(vals[0])
	allSame := true

	var lo, hi float64

	haveReal := false

	for _, v := range vals {
		if math.Float64bits(v) != firstBits {
			allSame = false
		}

		if math.IsNaN(v) {
			continue
		}

		if !haveReal {
			lo, hi, haveReal = v, v, true

			continue
		}

		if v < lo {
			lo = v
		}

		if v > hi {
			hi = v
		}
	}

	if !haveReal { // all-NaN column
		lo, hi = vals[0], vals[0]
	}

	d.MinFloat64, d.MaxFloat64 = lo, hi
	if allSame {
		d.Const = true
		d.ConstFloat64 = vals[0]
	}
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

	var dec func([]int64, []byte) ([]int64, int, error)

	switch r.desc.Codec {
	case chunk.CodecDoD:
		dec = chunk.DecodeTimestamps
	case chunk.CodecT64:
		dec = chunk.DecodeIntsT64
	default: // dec stays nil ⇒ decodeColumn reports the bad codec
	}

	return decodeColumn(r, dst, dec)
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

	var dec func([]float64, []byte) ([]float64, int, error)

	switch r.desc.Codec {
	case chunk.CodecGorilla:
		dec = chunk.DecodeFloats
	case chunk.CodecDecimal:
		dec = chunk.DecodeFloatsDecimal
	default: // dec stays nil ⇒ decodeColumn reports the bad codec
	}

	return decodeColumn(r, dst, dec)
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

// decodeColumn decompresses the column object and runs the selected typed decoder. A nil
// decoder means the descriptor's codec does not match the column's kind.
func decodeColumn[T any](r *ColumnReader, dst []T, dec func([]T, []byte) ([]T, int, error)) ([]T, error) {
	if dec == nil {
		return nil, errors.Errorf("block: codec %s invalid for column %q", r.desc.Codec, r.desc.Name)
	}

	stream, err := r.stream()
	if err != nil {
		return nil, err
	}

	out, _, err := dec(dst[:0], stream)

	return out, err
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

// stream decompresses the column's block frame into its raw codec stream.
func (r *ColumnReader) stream() ([]byte, error) {
	out, err := r.comp.Decompress(nil, r.object)
	if err != nil {
		return nil, errors.Wrapf(err, "decompress column %q", r.desc.Name)
	}

	return out, nil
}
