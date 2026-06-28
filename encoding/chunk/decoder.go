package chunk

import (
	"errors"
	"math"

	"github.com/oteldb/storage/encoding/bitstream"
)

// errUnsupportedCodec is returned by NewFloatDecoder for a non-float codec.
var errUnsupportedCodec = errors.New("chunk: codec is not a float64 codec")

// This file provides forward decoders: stateful cursors that decode a column one row at a time from
// its (sequential, bit-packed) codec stream. They decode exactly the same bytes as the one-shot
// Decode* functions — same header, same per-row state machine — but hold only the current codec
// state plus one decoded value, not the whole column.
//
// A streaming (k-way) merge uses them to advance through each source part's (series, ts)-sorted
// columns one series range at a time: since the merge visits series in ascending order and each
// part's series occupy contiguous row runs, a part's cursors move strictly forward, decoding the
// part's whole column exactly once across the merge while keeping only one series range resident
// (vs the one-shot path, which decodes every source part's whole column up front and holds them all).

// TsDecoder is a forward cursor over a delta-of-delta encoded timestamp column ([EncodeTimestamps]).
type TsDecoder struct {
	r    *bitstream.Reader
	rows int
	i    int
	prev int64 // last decoded timestamp
	dod  int64 // last delta (tDelta), advanced by dod per row ≥2
}

// NewTsDecoder parses the column header and returns a cursor over its rows. bound is the byte length
// of the column stream (header + payload), used to bound-check the row count.
func NewTsDecoder(src []byte) (*TsDecoder, error) {
	r, rows, consumed, err := readHeader(src)
	if err != nil {
		return nil, err
	}

	if rows == 0 {
		return &TsDecoder{r: r, rows: 0}, nil
	}

	// DoD is bit-packed (≥1 bit/row), so a count above 8×payload bytes is a corrupt header.
	if err := boundRows(rows, 8*(len(src)-consumed)); err != nil {
		return nil, err
	}

	return &TsDecoder{r: r, rows: rows}, nil
}

// Len returns the column's total row count.
func (d *TsDecoder) Len() int { return d.rows }

// Pos returns the number of rows already decoded.
func (d *TsDecoder) Pos() int { return d.i }

// Next decodes and returns the next timestamp. It returns an [IsEOF] error after the last row.
func (d *TsDecoder) Next() (int64, error) {
	if d.i >= d.rows {
		return 0, errEOF
	}

	var v int64

	switch d.i {
	case 0:
		t0, err := d.r.ReadVarint()
		if err != nil {
			return 0, err
		}

		v = t0
	case 1:
		td, err := d.r.ReadUvarint()
		if err != nil {
			return 0, err
		}

		d.dod = int64(td)
		v = d.prev + d.dod
	default:
		delta, err := readDoD(d.r)
		if err != nil {
			return 0, err
		}

		d.dod += delta
		v = d.prev + d.dod
	}

	d.prev = v
	d.i++

	return v, nil
}

// GorillaDecoder is a forward cursor over a Gorilla XOR encoded float64 column ([EncodeFloats]).
type GorillaDecoder struct {
	r        *bitstream.Reader
	rows     int
	i        int
	val      float64
	leading  uint8
	trailing uint8
}

// NewGorillaDecoder parses the column header and returns a cursor over its rows.
func NewGorillaDecoder(src []byte) (*GorillaDecoder, error) {
	r, rows, consumed, err := readHeader(src)
	if err != nil {
		return nil, err
	}

	if rows == 0 {
		return &GorillaDecoder{r: r, rows: 0}, nil
	}

	if err := boundRows(rows, 8*(len(src)-consumed)); err != nil {
		return nil, err
	}

	return &GorillaDecoder{r: r, rows: rows, leading: 0xff}, nil
}

// Len returns the column's total row count.
func (d *GorillaDecoder) Len() int { return d.rows }

// Pos returns the number of rows already decoded.
func (d *GorillaDecoder) Pos() int { return d.i }

// Next decodes and returns the next value. It returns an [IsEOF] error after the last row.
func (d *GorillaDecoder) Next() (float64, error) {
	if d.i >= d.rows {
		return 0, errEOF
	}

	if d.i == 0 {
		b, err := d.r.ReadBits(64)
		if err != nil {
			return 0, err
		}

		d.val = math.Float64frombits(b)
	} else {
		if err := xorRead(d.r, &d.val, &d.leading, &d.trailing); err != nil {
			return 0, err
		}
	}

	d.i++

	return d.val, nil
}

// DecimalDecoder is a forward cursor over a scaled-decimal + nearest-delta encoded float64 column
// ([EncodeFloatsDecimal]).
type DecimalDecoder struct {
	r     *bitstream.Reader
	rows  int
	i     int
	cur   int64 // accumulated scaled value
	exp   int
	scale float64
}

// NewDecimalDecoder parses the column header and returns a cursor over its rows.
func NewDecimalDecoder(src []byte) (*DecimalDecoder, error) {
	r, rows, _, err := readHeader(src)
	if err != nil {
		return nil, err
	}

	if rows == 0 {
		return &DecimalDecoder{r: r, rows: 0}, nil
	}

	if err := boundRows(rows, maxColumnRows); err != nil {
		return nil, err
	}

	d := &DecimalDecoder{r: r, rows: rows}

	// Per-column header: precisionBits, exponent, then the first scaled value.
	if _, err := r.ReadUvarint(); err != nil { // precisionBits — decode-only, discarded
		return nil, err
	}

	exp64, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}

	d.exp = int(exp64)
	d.scale = decimalScale(d.exp)

	v0, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}

	d.cur = v0

	return d, nil
}

// Len returns the column's total row count.
func (d *DecimalDecoder) Len() int { return d.rows }

// Pos returns the number of rows already decoded.
func (d *DecimalDecoder) Pos() int { return d.i }

// Next decodes and returns the next value. It returns an [IsEOF] error after the last row.
func (d *DecimalDecoder) Next() (float64, error) {
	if d.i >= d.rows {
		return 0, errEOF
	}

	if d.i > 0 {
		delta, err := d.r.ReadVarint()
		if err != nil {
			return 0, err
		}

		d.cur += delta
	}

	d.i++

	return scaledToFloat(d.cur, d.exp, d.scale), nil
}

// FloatDecoder is the forward-cursor interface over a float64 column, regardless of codec.
type FloatDecoder interface {
	Len() int
	Pos() int
	Next() (float64, error)
}

// NewFloatDecoder returns a forward cursor over a float64 column encoded with codec, or an error if
// codec is not a float codec ([CodecGorilla], [CodecDecimal]).
func NewFloatDecoder(codec Codec, src []byte) (FloatDecoder, error) {
	switch codec {
	case CodecGorilla:
		return NewGorillaDecoder(src)
	case CodecDecimal:
		return NewDecimalDecoder(src)
	default:
		return nil, errUnsupportedCodec
	}
}
