package chunk

import (
	"encoding/binary"
	"math"
	"slices"

	"github.com/go-faster/errors"
)

// MaxFloatGroupRows bounds the decoded rows of a grouped stream.
const MaxFloatGroupRows = 1 << 20

// FloatGroupEncoder reuses scratch while selecting bit-exact codecs for independent float groups.
type FloatGroupEncoder struct {
	gorilla, decimal []byte
	candidate        []byte
	decoded          []float64
	decimalWork      decimalScratch
}

// Encode appends grouped floats. ends contains positive, increasing group ends, ending at len(vals).
func (e *FloatGroupEncoder) Encode(dst []byte, vals []float64, ends []int) ([]byte, error) {
	if len(vals) > MaxFloatGroupRows {
		return dst, errors.New("float groups: too many rows")
	}
	prev := 0
	for _, end := range ends {
		if end <= prev || end > len(vals) {
			return dst, errors.New("float groups: invalid boundary")
		}
		prev = end
	}
	if prev != len(vals) {
		return dst, errors.New("float groups: incomplete boundaries")
	}
	dst = append(dst, 1)
	dst = binary.AppendUvarint(dst, uint64(len(vals)))
	dst = binary.AppendUvarint(dst, uint64(len(ends)))
	prev = 0
	for _, end := range ends {
		group := vals[prev:end]
		prev = end
		dst = e.appendGroup(dst, group)
	}
	return dst, nil
}

// EncodeGroupWithCost chooses a bit-exact stream using framed-size cost, which must not retain or mutate its input.
func (e *FloatGroupEncoder) EncodeGroupWithCost(dst []byte, values []float64, cost func([]byte) int) ([]byte, error) {
	start := len(dst)
	var ends []int
	if len(values) > 0 {
		ends = []int{len(values)}
	}
	out, err := e.Encode(dst, values, ends)
	if err != nil {
		return dst, err
	}
	if len(values) == 0 || cost == nil {
		return out, nil
	}
	_, rest, err := floatGroupHeader(out[start:])
	if err != nil {
		return dst, err
	}
	group, err := readFloatGroup(rest)
	if err != nil {
		return dst, err
	}
	if group.codec == CodecNone {
		return out, nil
	}
	best := cost(out[start:])
	if group.codec != CodecGorilla {
		e.gorilla = EncodeFloats(e.gorilla[:0], values)
		e.candidate = singleFloatGroup(e.candidate[:0], len(values), CodecGorilla, e.gorilla)
		if n := cost(e.candidate); n < best {
			out = append(out[:start], e.candidate...)
			best = n
		}
	}
	if group.codec != CodecDecimal {
		e.decimal = encodeFloatsDecimal(e.decimal[:0], values, 64, &e.decimalWork)
		var used int
		e.decoded, used, err = DecodeFloatsDecimal(e.decoded[:0], e.decimal)
		if err == nil && used == len(e.decimal) && equalFloatBits(values, e.decoded) {
			e.candidate = singleFloatGroup(e.candidate[:0], len(values), CodecDecimal, e.decimal)
			if n := cost(e.candidate); n < best {
				out = append(out[:start], e.candidate...)
			}
		}
	}
	return out, nil
}

// ResidentBytes reports reusable candidate storage held by the encoder.
func (e *FloatGroupEncoder) ResidentBytes() int64 {
	return int64(cap(e.gorilla)+cap(e.decimal)+cap(e.candidate)) + int64(cap(e.decoded)+cap(e.decimalWork.scaled)+cap(e.decimalWork.exps))*8
}

func (e *FloatGroupEncoder) appendGroup(dst []byte, group []float64) []byte {
	bits := math.Float64bits(group[0])
	constant := true
	for _, v := range group[1:] {
		if math.Float64bits(v) != bits {
			constant = false
			break
		}
	}
	if constant {
		dst = append(dst, byte(CodecNone))
		dst = binary.AppendUvarint(dst, uint64(len(group)))
		dst = binary.AppendUvarint(dst, 8)
		dst = binary.BigEndian.AppendUint64(dst, bits)
		return dst
	}
	e.gorilla = EncodeFloats(e.gorilla[:0], group)
	payload := e.gorilla
	codec := CodecGorilla
	e.decimal = encodeFloatsDecimal(e.decimal[:0], group, 64, &e.decimalWork)
	if len(e.decimal) < len(payload) {
		var n int
		var err error
		e.decoded, n, err = DecodeFloatsDecimal(e.decoded[:0], e.decimal)
		exact := err == nil && n == len(e.decimal) && len(e.decoded) == len(group)
		exact = exact && equalFloatBits(e.decoded, group)
		if exact {
			payload = e.decimal
			codec = CodecDecimal
		}
	}
	dst = append(dst, byte(codec))
	dst = binary.AppendUvarint(dst, uint64(len(group)))
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	dst = append(dst, payload...)
	return dst
}

func equalFloatBits(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if math.Float64bits(v) != math.Float64bits(b[i]) {
			return false
		}
	}
	return true
}

type floatGroup struct {
	codec      Codec
	rows       int
	data, rest []byte
}

func readFloatGroup(src []byte) (floatGroup, error) {
	if len(src) == 0 {
		return floatGroup{}, errors.New("float groups: missing fragment")
	}
	codec := Codec(src[0])
	src = src[1:]
	rows, n := binary.Uvarint(src)
	if n <= 0 || rows == 0 || rows > MaxFloatGroupRows {
		return floatGroup{}, errors.New("float groups: invalid fragment rows")
	}
	src = src[n:]
	size, n := binary.Uvarint(src)
	if n <= 0 || size > uint64(len(src)-n) {
		return floatGroup{}, errors.New("float groups: invalid fragment length")
	}
	src = src[n:]
	switch codec {
	case CodecNone:
		if size != 8 {
			return floatGroup{}, errors.New("float groups: invalid constant")
		}
	case CodecGorilla, CodecDecimal:
	default:
		return floatGroup{}, errors.New("float groups: invalid fragment codec")
	}
	return floatGroup{codec: codec, rows: int(rows), data: src[:int(size)], rest: src[int(size):]}, nil
}

func floatGroupHeader(src []byte) (rows int, rest []byte, err error) {
	if len(src) == 0 || src[0] != 1 {
		return 0, nil, errors.New("float groups: unsupported version")
	}
	src = src[1:]
	count, n := binary.Uvarint(src)
	if n <= 0 || count > MaxFloatGroupRows {
		return 0, nil, errors.New("float groups: invalid row count")
	}
	src = src[n:]
	groups, n := binary.Uvarint(src)
	if n <= 0 || groups > count {
		return 0, nil, errors.New("float groups: invalid group count")
	}
	src = src[n:]
	rest = src
	sum := 0
	for range groups {
		g, e := readFloatGroup(src)
		if e != nil {
			return 0, nil, e
		}
		if g.rows > int(count)-sum {
			return 0, nil, errors.New("float groups: row overflow")
		}
		sum += g.rows
		src = g.rest
		if g.codec != CodecNone {
			inner, m := binary.Uvarint(g.data)
			if m <= 0 || inner != uint64(g.rows) {
				return 0, nil, errors.New("float groups: inconsistent fragment count")
			}
		}
	}
	if sum != int(count) || len(src) != 0 {
		return 0, nil, errors.New("float groups: inconsistent length")
	}
	return int(count), rest, nil
}

func decodeFloatGroup(dst []float64, g floatGroup) ([]float64, error) {
	if g.codec == CodecNone {
		value := math.Float64frombits(binary.BigEndian.Uint64(g.data))
		start := len(dst)
		dst = slices.Grow(dst, g.rows)
		dst = dst[:start+g.rows]
		for i := start; i < len(dst); i++ {
			dst[i] = value
		}
		return dst, nil
	}
	dst = slices.Grow(dst, g.rows)
	start := len(dst)
	var decoded []float64
	var n int
	var err error
	switch g.codec {
	case CodecGorilla:
		decoded, n, err = DecodeFloats(dst[start:], g.data)
	case CodecDecimal:
		decoded, n, err = DecodeFloatsDecimal(dst[start:], g.data)
	default:
		return nil, errors.New("float groups: invalid codec")
	}
	if err != nil {
		return nil, err
	}
	if n != len(g.data) || len(decoded) != g.rows {
		return nil, errors.New("float groups: inconsistent decoded fragment")
	}
	dst = dst[:start+len(decoded)]
	return dst, nil
}

// DecodeFloatGroups decodes a checked grouped stream into dst, reusing its capacity.
func DecodeFloatGroups(dst []float64, src []byte) ([]float64, int, error) {
	rows, rest, err := floatGroupHeader(src)
	if err != nil {
		return dst, 0, err
	}
	dst = slices.Grow(dst[:0], rows)
	for len(rest) > 0 {
		g, e := readFloatGroup(rest)
		if e != nil {
			return dst, 0, e
		}
		dst, e = decodeFloatGroup(dst, g)
		if e != nil {
			return dst, 0, e
		}
		rest = g.rest
	}
	return dst, len(src), nil
}

// FloatGroupsDecoder reads a grouped float stream with one reusable fragment buffer.
type FloatGroupsDecoder struct {
	rest            []byte
	values          []float64
	rows, pos, next int
	constant        float64
	remaining       int
	failed          error
}

// NewFloatGroupsDecoder validates grouped framing before returning a cursor.
func NewFloatGroupsDecoder(src []byte) (*FloatGroupsDecoder, error) {
	rows, rest, err := floatGroupHeader(src)
	if err != nil {
		return nil, err
	}
	return &FloatGroupsDecoder{rest: rest, rows: rows}, nil
}

// Len returns the stream's row count.
func (d *FloatGroupsDecoder) Len() int { return d.rows }

// Pos returns the number of emitted rows.
func (d *FloatGroupsDecoder) Pos() int { return d.pos }

// Next returns the next float, preserving its bits.
func (d *FloatGroupsDecoder) Next() (float64, error) {
	if d.failed != nil {
		return 0, d.failed
	}
	if d.pos >= d.rows {
		return 0, errEOF
	}
	if d.remaining == 0 {
		if err := d.advanceGroup(); err != nil {
			d.failed = err
			return 0, err
		}
	}

	value := d.constant
	if len(d.values) > 0 {
		value = d.values[d.next]
		d.next++
	}
	d.remaining--
	d.pos++
	return value, nil
}

// DecodeFloatGroupsRows verifies the expected rows before allocating decoded storage.
func DecodeFloatGroupsRows(dst []float64, src []byte, expected int) ([]float64, int, error) {
	rows, _, err := floatGroupHeader(src)
	if err != nil {
		return dst, 0, err
	}
	if rows != expected {
		return dst, 0, errors.New("float groups: unexpected row count")
	}
	return DecodeFloatGroups(dst, src)
}

func (d *FloatGroupsDecoder) advanceGroup() error {
	g, err := readFloatGroup(d.rest)
	if err != nil {
		return err
	}
	d.rest = g.rest
	d.next = 0
	d.remaining = g.rows
	if g.codec == CodecNone {
		d.constant = math.Float64frombits(binary.BigEndian.Uint64(g.data))
		d.values = d.values[:0]
		return nil
	}
	d.values, err = decodeFloatGroup(d.values[:0], g)
	return err
}

func singleFloatGroup(dst []byte, rows int, codec Codec, data []byte) []byte {
	dst = append(dst, 1)
	dst = binary.AppendUvarint(dst, uint64(rows))
	dst = append(dst, 1, byte(codec))
	dst = binary.AppendUvarint(dst, uint64(rows))
	dst = binary.AppendUvarint(dst, uint64(len(data)))
	return append(dst, data...)
}
