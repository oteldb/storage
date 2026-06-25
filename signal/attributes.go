package signal

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"strconv"

	"github.com/zeebo/xxh3"
)

// SeriesID is the content-addressed identity of a time series: a hash of its sorted
// attribute set. Equal attribute sets yield equal SeriesID on every node, which makes
// placement and dedup deterministic without a central id allocator.
type SeriesID uint64

// ValueKind is the dynamic type of an OTel attribute [Value] — the OTLP AnyValue oneof.
// Values are persisted in the canonical hash pre-image and the symbol/series formats;
// never reorder.
type ValueKind uint8

const (
	// KindEmpty is an unset/null value (distinct from an empty string or zero).
	KindEmpty ValueKind = iota
	// KindStr is a UTF-8 string, held as raw bytes.
	KindStr
	// KindBool is a boolean.
	KindBool
	// KindInt is a signed 64-bit integer.
	KindInt
	// KindDouble is an IEEE-754 float64.
	KindDouble
	// KindBytes is a raw byte string.
	KindBytes
	// KindSlice is an ordered array of values (ArrayValue).
	KindSlice
	// KindMap is an unordered map of key→value (KeyValueList); compared and hashed
	// irrespective of order, per the OTel spec.
	KindMap
)

// Value is an OTel attribute value: the typed AnyValue sum. String and byte values are
// held as []byte (never string) so projecting from an OTLP decode buffer and interning
// stay allocation-free — keys and values are compared and hashed as bytes. Scalars are
// stored inline; bytes/slice/map hold a reference the caller must not mutate. The zero
// Value is [KindEmpty]. Treat values as immutable.
type Value struct {
	kind ValueKind
	num  uint64 // KindBool (0/1), KindInt (int64 bits), KindDouble (Float64bits)
	b    []byte // KindStr (UTF-8 bytes) or KindBytes
	ref  any    // KindSlice []Value | KindMap Attributes
}

// StringValue returns a string attribute value over s (not copied).
func StringValue(s []byte) Value { return Value{kind: KindStr, b: s} }

// BytesValue returns a raw-bytes attribute value over b (not copied).
func BytesValue(b []byte) Value { return Value{kind: KindBytes, b: b} }

// BoolValue returns a boolean attribute value.
func BoolValue(b bool) Value {
	var n uint64
	if b {
		n = 1
	}

	return Value{kind: KindBool, num: n}
}

// IntValue returns an int64 attribute value.
func IntValue(i int64) Value { return Value{kind: KindInt, num: uint64(i)} }

// DoubleValue returns a float64 attribute value.
func DoubleValue(f float64) Value { return Value{kind: KindDouble, num: math.Float64bits(f)} }

// SliceValue returns an ordered array attribute value.
func SliceValue(vs ...Value) Value { return Value{kind: KindSlice, ref: vs} }

// MapValue returns a map (kvlist) attribute value; the entries are sorted by key.
func MapValue(kvs ...KeyValue) Value { return Value{kind: KindMap, ref: NewAttributes(kvs...)} }

// EmptyValue returns the unset/null value.
func EmptyValue() Value { return Value{kind: KindEmpty} }

// Kind reports the value's dynamic type.
func (v Value) Kind() ValueKind { return v.kind }

// Str returns the string bytes (nil if not [KindStr]). The result aliases the value.
func (v Value) Str() []byte {
	if v.kind != KindStr {
		return nil
	}

	return v.b
}

// Bytes returns the byte value (nil if not [KindBytes]). The result aliases the value.
func (v Value) Bytes() []byte {
	if v.kind != KindBytes {
		return nil
	}

	return v.b
}

// Bool returns the boolean value (false if not [KindBool]).
func (v Value) Bool() bool { return v.num != 0 }

// Int returns the int64 value (0 if not [KindInt]).
func (v Value) Int() int64 { return int64(v.num) }

// Double returns the float64 value (0 if not [KindDouble]).
func (v Value) Double() float64 { return math.Float64frombits(v.num) }

// Slice returns the array elements (nil if not [KindSlice]).
func (v Value) Slice() []Value {
	s, _ := v.ref.([]Value)

	return s
}

// Map returns the map entries (nil if not [KindMap]).
func (v Value) Map() Attributes {
	m, _ := v.ref.(Attributes)

	return m
}

// Equal reports whether two values are deeply equal (maps are order-independent because
// [Attributes] is kept sorted).
func (v Value) Equal(o Value) bool {
	if v.kind != o.kind {
		return false
	}

	switch v.kind {
	case KindEmpty:
		return true
	case KindStr, KindBytes:
		return bytes.Equal(v.b, o.b)
	case KindBool, KindInt, KindDouble:
		return v.num == o.num
	case KindSlice:
		return slices.EqualFunc(v.Slice(), o.Slice(), Value.Equal)
	case KindMap:
		return v.Map().Equal(o.Map())
	default:
		return false
	}
}

// Clone returns a deep copy of the value (including nested slices/maps and the string/
// byte payload).
func (v Value) Clone() Value {
	switch v.kind {
	case KindStr, KindBytes:
		return Value{kind: v.kind, b: slices.Clone(v.b)}
	case KindSlice:
		src := v.Slice()
		out := make([]Value, len(src))
		for i := range src {
			out[i] = src[i].Clone()
		}

		return Value{kind: KindSlice, ref: out}
	case KindMap:
		return Value{kind: KindMap, ref: v.Map().Clone()}
	default:
		return v // scalars are values
	}
}

// AppendText appends a canonical text projection of the value to dst (append-style, so
// callers reuse one buffer). It is used by the string-keyed matching layer and for
// display; it is not the identity (that is the typed [Attributes.Hash]).
func (v Value) AppendText(dst []byte) []byte {
	switch v.kind {
	case KindEmpty:
		return dst
	case KindStr, KindBytes:
		return append(dst, v.b...)
	case KindBool:
		return strconv.AppendBool(dst, v.Bool())
	case KindInt:
		return strconv.AppendInt(dst, v.Int(), 10)
	case KindDouble:
		return strconv.AppendFloat(dst, v.Double(), 'g', -1, 64)
	case KindSlice, KindMap:
		return v.appendJSON(dst)
	default:
		return dst
	}
}

func (v Value) appendHash(dst []byte) []byte {
	dst = append(dst, byte(v.kind))

	switch v.kind {
	case KindEmpty:
	case KindStr, KindBytes:
		dst = appendLenBytes(dst, v.b)
	case KindBool, KindInt, KindDouble:
		dst = binary.BigEndian.AppendUint64(dst, v.num)
	case KindSlice:
		s := v.Slice()
		dst = binary.AppendUvarint(dst, uint64(len(s)))
		for i := range s {
			dst = s[i].appendHash(dst)
		}
	case KindMap:
		dst = v.Map().AppendHashInput(dst)
	}

	return dst
}

func (v Value) appendJSON(dst []byte) []byte {
	switch v.kind {
	case KindSlice:
		dst = append(dst, '[')
		for i, e := range v.Slice() {
			if i > 0 {
				dst = append(dst, ',')
			}

			dst = e.appendJSONElem(dst)
		}

		return append(dst, ']')
	case KindMap:
		dst = append(dst, '{')
		for i, kv := range v.Map() {
			if i > 0 {
				dst = append(dst, ',')
			}

			dst = strconv.AppendQuote(dst, string(kv.Key))
			dst = append(dst, ':')
			dst = kv.Value.appendJSONElem(dst)
		}

		return append(dst, '}')
	default:
		return dst
	}
}

func (v Value) appendJSONElem(dst []byte) []byte {
	switch v.kind {
	case KindSlice, KindMap:
		return v.appendJSON(dst)
	case KindStr, KindBytes:
		return strconv.AppendQuote(dst, string(v.b))
	default:
		return v.AppendText(dst)
	}
}

// KeyValue is one attribute: a key and its typed value. Keys are unique within an
// [Attributes] set and case-sensitive. The key is held as []byte to keep interning and
// projection allocation-free.
type KeyValue struct {
	Key   []byte
	Value Value
}

// Attributes is an OTel attribute set, kept **sorted by key**. It models a Resource,
// Scope, or data-point attribute set; its [Attributes.Hash] is the series identity.
// Construct one with [NewAttributes].
type Attributes []KeyValue

// NewAttributes returns the attributes sorted by key (sorted in place, stable). Keys are
// assumed unique; the stable sort makes even malformed duplicate-key input hash
// deterministically.
func NewAttributes(kvs ...KeyValue) Attributes {
	slices.SortStableFunc(kvs, func(a, b KeyValue) int { return bytes.Compare(a.Key, b.Key) })

	return kvs
}

// Get returns the value for key and whether it is present.
func (a Attributes) Get(key []byte) (Value, bool) {
	for i := range a {
		if bytes.Equal(a[i].Key, key) {
			return a[i].Value, true
		}
	}

	return Value{}, false
}

// Hash returns the content-addressed [SeriesID] of the sorted attribute set.
func (a Attributes) Hash() SeriesID {
	return SeriesID(xxh3.Hash(a.AppendHashInput(nil)))
}

// AppendHashInput appends the canonical, type-tagged hash pre-image of the (sorted)
// attribute set to dst and returns it. Callers that hash many series reuse one buffer to
// stay zero-alloc. The encoding is unambiguous: each key is length-prefixed and each
// value carries its kind tag, so no two distinct attribute sets share a pre-image.
func (a Attributes) AppendHashInput(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(a)))
	for i := range a {
		dst = appendLenBytes(dst, a[i].Key)
		dst = a[i].Value.appendHash(dst)
	}

	return dst
}

// Equal reports whether two sorted attribute sets are deeply equal.
func (a Attributes) Equal(other Attributes) bool {
	if len(a) != len(other) {
		return false
	}

	for i := range a {
		if !bytes.Equal(a[i].Key, other[i].Key) || !a[i].Value.Equal(other[i].Value) {
			return false
		}
	}

	return true
}

// Clone returns a deep copy of the attribute set (including keys and nested values).
func (a Attributes) Clone() Attributes {
	if a == nil {
		return nil
	}

	out := make(Attributes, len(a))
	for i := range a {
		out[i] = KeyValue{Key: slices.Clone(a[i].Key), Value: a[i].Value.Clone()}
	}

	return out
}

func appendLenBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))

	return append(dst, b...)
}
