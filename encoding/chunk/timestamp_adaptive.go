package chunk

import (
	"encoding/binary"

	"github.com/go-faster/errors"
)

// EncodeTimestampsAdaptive appends a regular-cadence or scaled delta-of-delta stream.
func EncodeTimestampsAdaptive(dst []byte, values []int64) ([]byte, error) {
	if len(values) > MaxFloatGroupRows {
		return dst, errors.New("adaptive timestamps: too many rows")
	}
	var step int64
	if len(values) > 1 {
		step = values[1] - values[0]
	}
	for i := 2; i < len(values); i++ {
		if values[i]-values[i-1] != step {
			return EncodeTimestampsScaled(append(dst, 1), values), nil
		}
	}
	dst = append(dst, 0)
	dst = binary.AppendUvarint(dst, uint64(len(values)))
	if len(values) > 0 {
		dst = binary.AppendVarint(dst, values[0])
		dst = binary.AppendVarint(dst, step)
	}
	return dst, nil
}

func adaptiveTimestampHeader(src []byte) (rows int, first, step int64, err error) {
	count, n := binary.Uvarint(src)
	if n <= 0 || count > MaxFloatGroupRows {
		return 0, 0, 0, errors.New("adaptive timestamps: invalid rows")
	}
	src = src[n:]
	if count == 0 {
		if len(src) != 0 {
			return 0, 0, 0, errors.New("adaptive timestamps: trailing bytes")
		}
		return 0, 0, 0, nil
	}
	first, n = binary.Varint(src)
	if n <= 0 {
		return 0, 0, 0, errors.New("adaptive timestamps: invalid first value")
	}
	src = src[n:]
	step, n = binary.Varint(src)
	if n <= 0 || n != len(src) {
		return 0, 0, 0, errors.New("adaptive timestamps: invalid stride")
	}
	return int(count), first, step, nil
}

// DecodeTimestampsAdaptive decodes one bounded adaptive timestamp chunk into dst.
func DecodeTimestampsAdaptive(dst []int64, src []byte) ([]int64, int, error) {
	if len(src) == 0 {
		return dst, 0, errors.New("adaptive timestamps: missing mode")
	}
	switch src[0] {
	case 0:
		rows, first, step, err := adaptiveTimestampHeader(src[1:])
		if err != nil {
			return dst, 0, err
		}
		dst = resize(dst, rows)
		for i := range dst {
			dst[i] = first
			first += step
		}
		return dst, len(src), nil
	case 1:
		rows, n := binary.Uvarint(src[1:])
		if n <= 0 || rows == 0 || rows > MaxFloatGroupRows {
			return dst, 0, errors.New("adaptive timestamps: invalid fallback rows")
		}
		out, used, err := DecodeTimestampsScaled(dst, src[1:])
		if err != nil {
			return dst, 0, err
		}
		if used != len(src)-1 {
			return dst, 0, errors.New("adaptive timestamps: trailing fallback bytes")
		}
		return out, used + 1, nil
	default:
		return dst, 0, errors.New("adaptive timestamps: unsupported mode")
	}
}

// DecodeTimestampsAdaptiveRows verifies the expected row count before decoding.
func DecodeTimestampsAdaptiveRows(dst []int64, src []byte, rows int) ([]int64, int, error) {
	if len(src) < 2 {
		return dst, 0, errors.New("adaptive timestamps: missing header")
	}
	n, k := binary.Uvarint(src[1:])
	if k <= 0 || n != uint64(rows) {
		return dst, 0, errors.New("adaptive timestamps: inconsistent rows")
	}
	return DecodeTimestampsAdaptive(dst, src)
}

type adaptiveTimestampCursor struct {
	rows, pos                     int
	next, step                    int64
	fallback                      *scaledTsDecoder
	fallbackBytes, fallbackHeader int
	failed                        error
}

// NewAdaptiveTimestampCursor opens an adaptive timestamp chunk without materializing its rows.
func NewAdaptiveTimestampCursor(src []byte) (TsCursor, error) {
	if len(src) == 0 {
		return nil, errors.New("adaptive timestamps: missing mode")
	}
	switch src[0] {
	case 0:
		rows, first, step, err := adaptiveTimestampHeader(src[1:])
		if err != nil {
			return nil, err
		}
		return &adaptiveTimestampCursor{rows: rows, next: first, step: step}, nil
	case 1:
		rows, n := binary.Uvarint(src[1:])
		if n <= 0 || rows == 0 || rows > MaxFloatGroupRows {
			return nil, errors.New("adaptive timestamps: invalid fallback rows")
		}
		decoded, err := NewTsDecoder(src[1:])
		if err != nil {
			return nil, err
		}
		cur := &scaledTsDecoder{TsDecoder: *decoded}
		return &adaptiveTimestampCursor{rows: int(rows), fallback: cur, fallbackBytes: len(src) - 1, fallbackHeader: n}, nil

	default:
		return nil, errors.New("adaptive timestamps: unsupported mode")
	}
}

func (c *adaptiveTimestampCursor) Len() int { return c.rows }
func (c *adaptiveTimestampCursor) Pos() int { return c.pos }
func (c *adaptiveTimestampCursor) Next() (int64, error) {
	if c.failed != nil {
		return 0, c.failed
	}
	if c.pos >= c.rows {
		return 0, errEOF
	}
	if c.fallback != nil {
		v, err := c.fallback.Next()
		if err == nil {
			c.pos++
			if c.pos == c.rows && c.fallbackHeader+c.fallback.r.ConsumedBytes() != c.fallbackBytes {
				err = errors.New("adaptive timestamps: trailing fallback bytes")
			}
		}
		if err != nil {
			c.failed = err
		}
		return v, err
	}
	v := c.next
	c.next += c.step
	c.pos++
	return v, nil
}
