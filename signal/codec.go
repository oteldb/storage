package signal

import (
	"encoding/binary"

	"github.com/go-faster/errors"
)

// ErrMalformed is returned when a serialized [Value] or [Attributes] fails to parse.
var ErrMalformed = errors.New("signal: malformed attribute encoding")

// AppendValue appends the canonical, type-tagged binary encoding of v to dst and returns
// it. The encoding is the same one used in the identity hash pre-image, so it preserves
// type (int 5, "5" and 5.0 encode differently) and round-trips through [DecodeValue].
// Engines intern AppendValue(nil, v) to get a value's symbol id.
func AppendValue(dst []byte, v Value) []byte { return v.appendHash(dst) }

// DecodeValue parses one value from the front of src and returns it with the number of
// bytes consumed. String/byte values alias src. It bounds-checks every field and never
// panics, so it is safe to fuzz on arbitrary input.
func DecodeValue(src []byte) (Value, int, error) {
	if len(src) == 0 {
		return Value{}, 0, errors.Wrap(ErrMalformed, "value kind")
	}

	kind := ValueKind(src[0])
	off := 1

	switch kind {
	case KindEmpty:
		return EmptyValue(), off, nil
	case KindStr, KindBytes:
		b, n, err := readLenBytes(src[off:])
		if err != nil {
			return Value{}, 0, errors.Wrap(err, "value bytes")
		}

		return Value{kind: kind, b: b}, off + n, nil
	case KindBool, KindInt, KindDouble:
		if len(src)-off < 8 {
			return Value{}, 0, errors.Wrap(ErrMalformed, "scalar")
		}

		return Value{kind: kind, num: binary.BigEndian.Uint64(src[off:])}, off + 8, nil
	case KindSlice:
		return decodeSlice(src, off)
	case KindMap:
		m, n, err := DecodeAttributes(src[off:])
		if err != nil {
			return Value{}, 0, err
		}

		return Value{kind: KindMap, ref: m}, off + n, nil
	default:
		return Value{}, 0, errors.Wrapf(ErrMalformed, "unknown value kind %d", kind)
	}
}

// DecodeSeries parses the canonical binary form produced by [Series.AppendHashInput]:
// resource, then scope, then data-point attributes. All byte fields alias src.
func DecodeSeries(src []byte) (Series, int, error) {
	res, rn, err := decodeResource(src)
	if err != nil {
		return Series{}, 0, errors.Wrap(err, "resource")
	}

	sc, sn, err := decodeScope(src[rn:])
	if err != nil {
		return Series{}, 0, errors.Wrap(err, "scope")
	}

	attrs, an, err := DecodeAttributes(src[rn+sn:])
	if err != nil {
		return Series{}, 0, errors.Wrap(err, "attributes")
	}

	return Series{Resource: res, Scope: sc, Attributes: attrs}, rn + sn + an, nil
}

func decodeResource(src []byte) (Resource, int, error) {
	schema, n, err := readLenBytes(src)
	if err != nil {
		return Resource{}, 0, errors.Wrap(err, "schema_url")
	}

	attrs, an, err := DecodeAttributes(src[n:])
	if err != nil {
		return Resource{}, 0, err
	}

	return Resource{SchemaURL: schema, Attributes: attrs}, n + an, nil
}

func decodeScope(src []byte) (Scope, int, error) {
	name, n1, err := readLenBytes(src)
	if err != nil {
		return Scope{}, 0, errors.Wrap(err, "name")
	}

	version, n2, err := readLenBytes(src[n1:])
	if err != nil {
		return Scope{}, 0, errors.Wrap(err, "version")
	}

	schema, n3, err := readLenBytes(src[n1+n2:])
	if err != nil {
		return Scope{}, 0, errors.Wrap(err, "schema_url")
	}

	off := n1 + n2 + n3

	attrs, an, err := DecodeAttributes(src[off:])
	if err != nil {
		return Scope{}, 0, err
	}

	return Scope{Name: name, Version: version, SchemaURL: schema, Attributes: attrs}, off + an, nil
}

// DecodeAttributes parses the canonical binary form produced by
// [Attributes.AppendHashInput]. Keys and string/byte values alias src.
func DecodeAttributes(src []byte) (Attributes, int, error) {
	count, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, 0, errors.Wrap(ErrMalformed, "attribute count")
	}

	off := n
	if count > uint64(len(src)) { // each attribute needs ≥1 byte; guards against OOM
		return nil, 0, errors.Wrapf(ErrMalformed, "attribute count %d exceeds input", count)
	}

	a := make(Attributes, 0, count)
	for range count {
		key, kn, err := readLenBytes(src[off:])
		if err != nil {
			return nil, 0, errors.Wrap(err, "attribute key")
		}

		off += kn

		v, vn, err := DecodeValue(src[off:])
		if err != nil {
			return nil, 0, err
		}

		off += vn
		a = append(a, KeyValue{Key: key, Value: v})
	}

	return a, off, nil
}

func decodeSlice(src []byte, off int) (Value, int, error) {
	count, n := binary.Uvarint(src[off:])
	if n <= 0 {
		return Value{}, 0, errors.Wrap(ErrMalformed, "slice count")
	}

	off += n
	if count > uint64(len(src)) {
		return Value{}, 0, errors.Wrapf(ErrMalformed, "slice count %d exceeds input", count)
	}

	out := make([]Value, 0, count)
	for range count {
		ev, en, err := DecodeValue(src[off:])
		if err != nil {
			return Value{}, 0, err
		}

		off += en
		out = append(out, ev)
	}

	return Value{kind: KindSlice, ref: out}, off, nil
}

// readLenBytes reads a uvarint-length-prefixed byte slice (aliasing src).
func readLenBytes(src []byte) ([]byte, int, error) {
	ln, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, 0, errors.Wrap(ErrMalformed, "length")
	}

	end := n + int(ln)
	if int(ln) < 0 || end < n || end > len(src) {
		return nil, 0, errors.Wrap(ErrMalformed, "length out of range")
	}

	return src[n:end], end, nil
}
