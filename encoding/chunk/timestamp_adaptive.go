package chunk

import (
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/bitstream"
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
	fallbackActive                bool
	failed                        error
}

// NewAdaptiveTimestampCursor opens an adaptive timestamp chunk without materializing its rows.
func NewAdaptiveTimestampCursor(src []byte) (TsCursor, error) {
	c := &adaptiveTimestampCursor{}
	if err := c.Reset(src); err != nil {
		return nil, err
	}
	return c, nil
}

// Reset opens another adaptive stream, reusing fallback reader state.
func (c *adaptiveTimestampCursor) Reset(src []byte) error {
	fallback := c.fallback
	if fallback != nil {
		reader := fallback.r
		reader.Reset(nil)
		*fallback = scaledTsDecoder{TsDecoder: TsDecoder{r: reader}}
	}
	*c = adaptiveTimestampCursor{fallback: fallback}
	if err := c.reset(src); err != nil {
		c.failed = err
		return err
	}
	return nil
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
	if c.fallbackActive {
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

func (c *adaptiveTimestampCursor) reset(src []byte) error {
	if len(src) == 0 {
		return errors.New("adaptive timestamps: missing mode")
	}
	switch src[0] {
	case 0:
		rows, first, step, err := adaptiveTimestampHeader(src[1:])
		if err != nil {
			return err
		}
		c.rows, c.next, c.step = rows, first, step
		return nil
	case 1:
		rows, n := binary.Uvarint(src[1:])
		if n <= 0 || rows == 0 || rows > MaxFloatGroupRows {
			return errors.New("adaptive timestamps: invalid fallback rows")
		}
		if c.fallback == nil {
			c.fallback = &scaledTsDecoder{TsDecoder: TsDecoder{r: new(bitstream.Reader)}}
		}
		_, consumed, err := readHeaderInto(src[1:], c.fallback.r)
		if err != nil {
			return err
		}
		if err := boundRows(int(rows), 8*(len(src)-1-consumed)); err != nil {
			return err
		}
		c.rows, c.fallback.rows = int(rows), int(rows)
		c.fallbackBytes, c.fallbackHeader = len(src)-1, n
		c.fallbackActive = true
		return nil

	default:
		return errors.New("adaptive timestamps: unsupported mode")
	}
}
