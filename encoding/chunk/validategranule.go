package chunk

import (
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/bitstream"
)

// ValidateFloatGroupsRows checks all nested codec streams without materializing floats.
func ValidateFloatGroupsRows(src []byte, expected int) error {
	return VisitFloatGroupsRows(src, expected, nil)
}

// VisitFloatGroupsRows checks nested streams and visits values without allocating sample buffers.
func VisitFloatGroupsRows(src []byte, expected int, visit func(float64)) error {
	rows, rest, err := floatGroupHeader(src)
	if err != nil {
		return err
	}
	if rows != expected {
		return errors.New("float groups: unexpected row count")
	}
	for len(rest) > 0 {
		g, err := readFloatGroup(rest)
		if err != nil {
			return err
		}
		switch g.codec {
		case CodecNone:
			if visit != nil {
				value := math.Float64frombits(binary.BigEndian.Uint64(g.data))
				for range g.rows {
					visit(value)
				}
			}
		case CodecGorilla:
			err = visitGorilla(g.data, visit)
		case CodecDecimal:
			err = visitDecimal(g.data, visit)
		default:
			return errors.New("float groups: invalid fragment codec")
		}
		if err != nil {
			return err
		}
		rest = g.rest
	}
	return nil
}

func visitGorilla(src []byte, visit func(float64)) error {
	var r bitstream.Reader
	rows, header, err := readHeaderInto(src, &r)
	if err != nil {
		return err
	}
	if rows == 0 {
		return checkFloatGroupLength(src, header)
	}
	if err := boundRows(rows, 8*(len(src)-header)); err != nil {
		return err
	}
	value, err := r.ReadBits(64)
	if err != nil {
		return err
	}
	if visit != nil {
		visit(math.Float64frombits(value))
	}
	leading, trailing := uint8(0xff), uint8(0)
	for i := 1; i < rows; i++ {
		changed, window, err := xorReadControl(&r)
		if err != nil {
			return err
		}
		if !changed {
			if visit != nil {
				visit(math.Float64frombits(value))
			}
			continue
		}
		shift, bits, err := xorReadWindow(&r, window, &leading, &trailing)
		if err != nil {
			return err
		}
		delta, err := r.ReadBits(bits)
		if err != nil {
			return err
		}
		if visit != nil {
			value ^= delta << shift
			visit(math.Float64frombits(value))
		}
	}
	if header+r.ConsumedBytes() != len(src) {
		return errors.New("float groups: trailing gorilla bytes")
	}
	return nil
}

func visitDecimal(src []byte, visit func(float64)) error {
	var r bitstream.Reader
	rows, header, err := readHeaderInto(src, &r)
	if err != nil {
		return err
	}
	if rows == 0 {
		return checkFloatGroupLength(src, header)
	}
	if err := boundRows(rows, len(src)-header); err != nil {
		return err
	}
	if _, err := r.ReadUvarint(); err != nil {
		return err
	}
	exponent, err := r.ReadVarint()
	if err != nil {
		return err
	}
	var current int64
	var scale float64
	if visit != nil {
		scale = decimalScale(int(exponent))
	}
	for range rows {
		delta, err := r.ReadVarint()
		if err != nil {
			return err
		}
		if visit != nil {
			current += delta
			visit(scaledToFloat(current, int(exponent), scale))
		}
	}
	if header+r.ConsumedBytes() != len(src) {
		return errors.New("float groups: trailing decimal bytes")
	}
	return nil
}

// ValidateTimestampsAdaptiveRows returns actual bounds and strict ordering without allocating rows.
func ValidateTimestampsAdaptiveRows(src []byte, expected int) (lo, hi int64, ordered bool, err error) {
	if len(src) == 0 || expected <= 0 || expected > MaxFloatGroupRows {
		return 0, 0, false, errors.New("adaptive timestamps: invalid granule")
	}
	if src[0] == 0 {
		rows, first, step, err := adaptiveTimestampHeader(src[1:])
		if err != nil {
			return 0, 0, false, err
		}
		if rows != expected {
			return 0, 0, false, errors.New("adaptive timestamps: inconsistent rows")
		}
		if lo, hi, ordered, ok := regularTimestampBounds(rows, first, step); ok {
			return lo, hi, ordered, nil
		}
	}
	var reader bitstream.Reader
	fallback := scaledTsDecoder{TsDecoder: TsDecoder{r: &reader}}
	cursor := adaptiveTimestampCursor{fallback: &fallback}
	if err := cursor.Reset(src); err != nil {
		return 0, 0, false, err
	}
	if cursor.Len() != expected {
		return 0, 0, false, errors.New("adaptive timestamps: inconsistent rows")
	}
	var prior int64
	ordered = true
	for i := range expected {
		value, err := cursor.Next()
		if err != nil {
			return 0, 0, false, err
		}
		if i == 0 {
			lo, hi = value, value
		} else {
			lo, hi = min(lo, value), max(hi, value)
			ordered = ordered && prior < value
		}
		prior = value
	}
	return lo, hi, ordered, nil
}

func checkFloatGroupLength(src []byte, consumed int) error {
	if consumed != len(src) {
		return errors.New("float groups: trailing codec bytes")
	}
	return nil
}

func regularTimestampBounds(rows int, first, step int64) (int64, int64, bool, bool) {
	if rows == 1 || step == 0 {
		return first, first, rows == 1, true
	}
	steps := uint64(rows - 1)
	if step > 0 && steps <= (uint64(math.MaxInt64)-uint64(first))/uint64(step) {
		return first, first + int64(steps)*step, true, true
	}
	if step < 0 && steps <= (uint64(first)-(uint64(1)<<63))/(uint64(-(step+1))+1) {
		return first + int64(steps)*step, first, false, true
	}
	return 0, 0, false, false
}
