package signal

import (
	"encoding/binary"
	"math"
	"slices"
	"strconv"

	"github.com/zeebo/xxh3"
)

// SeriesID is the content-addressed identity of a time series: a hash of its sorted
// attribute set (DESIGN.md §6). Equal attribute sets ⇒ equal SeriesID on every node,
// which makes placement and dedup deterministic without a central id allocator.
type SeriesID uint64

// ValueKind is the dynamic type of an OTel attribute [Value] (the OTLP `AnyValue`
// oneof; _ref/docs/opentelemetry-specification.md §0). Values are persisted in the
// canonical hash pre-image and the symbol/series formats — never reorder.
type ValueKind uint8

const (
	// KindEmpty is an unset/null value (distinct from an empty string or zero).
	KindEmpty ValueKind = iota
	// KindStr is a UTF-8 string.
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
	// KindMap is an unordered map of string→value (KeyValueList); compared and hashed
	// irrespective of order, per the OTel spec.
	KindMap
)

// Value is an OTel attribute value: the typed `AnyValue` sum. Scalars are stored inline
// (no allocation); bytes/slice/map hold a reference. The zero Value is [KindEmpty].
// Construct values with the typed constructors; treat them as immutable.
type Value struct {
	kind ValueKind
	num  uint64 // KindBool (0/1), KindInt (int64 bits), KindDouble (Float64bits)
	str  string // KindStr
	ref  any    // KindBytes []byte | KindSlice []Value | KindMap Attributes
}

// StringValue returns a string attribute value.
func StringValue(s string) Value { return Value{kind: KindStr, str: s} }

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

// BytesValue returns a raw-bytes attribute value.
func BytesValue(b []byte) Value { return Value{kind: KindBytes, ref: b} }

// SliceValue returns an ordered array attribute value.
func SliceValue(vs ...Value) Value { return Value{kind: KindSlice, ref: vs} }

// MapValue returns a map (kvlist) attribute value; the entries are sorted by key.
func MapValue(kvs ...KeyValue) Value { return Value{kind: KindMap, ref: NewAttributes(kvs...)} }

// EmptyValue returns the unset/null value.
func EmptyValue() Value { return Value{kind: KindEmpty} }

// Kind reports the value's dynamic type.
func (v Value) Kind() ValueKind { return v.kind }

// Str returns the string value (zero value if not [KindStr]).
func (v Value) Str() string { return v.str }

// Bool returns the boolean value (false if not [KindBool]).
func (v Value) Bool() bool { return v.num != 0 }

// Int returns the int64 value (0 if not [KindInt]).
func (v Value) Int() int64 { return int64(v.num) }

// Double returns the float64 value (0 if not [KindDouble]).
func (v Value) Double() float64 { return math.Float64frombits(v.num) }

// Bytes returns the byte value (nil if not [KindBytes]).
func (v Value) Bytes() []byte {
	b, _ := v.ref.([]byte)

	return b
}

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

// KeyValue is one attribute: a key and its typed value. Keys are unique within an
// [Attributes] set and case-sensitive (OTel spec).
type KeyValue struct {
	Key   string
	Value Value
}

// Attributes is an OTel attribute set, kept **sorted by key**. It models a Resource,
// Scope, or data-point attribute set; its [Attributes.Hash] is the series identity
// contribution. Construct one with [NewAttributes].
type Attributes []KeyValue

// NewAttributes returns the attributes sorted by key (sorted in place). Keys are
// assumed unique (the OTel spec requires it). The sort is stable, so even malformed
// input with duplicate keys hashes deterministically.
func NewAttributes(kvs ...KeyValue) Attributes {
	slices.SortStableFunc(kvs, func(a, b KeyValue) int {
		if a.Key < b.Key {
			return -1
		}

		if a.Key > b.Key {
			return 1
		}

		return 0
	})

	return kvs
}

// Get returns the value for key and whether it is present.
func (a Attributes) Get(key string) (Value, bool) {
	for i := range a {
		if a[i].Key == key {
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
// attribute set to dst. Callers that hash many series reuse one buffer to stay
// zero-alloc. The encoding is unambiguous: each key is length-prefixed and each value
// carries its kind tag, so no two distinct attribute sets share a pre-image.
func (a Attributes) AppendHashInput(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(a)))
	for i := range a {
		dst = appendLenString(dst, a[i].Key)
		dst = a[i].Value.appendHash(dst)
	}

	return dst
}

// Equal reports whether two attribute sets are deeply equal (both sorted).
func (a Attributes) Equal(other Attributes) bool {
	if len(a) != len(other) {
		return false
	}

	for i := range a {
		if a[i].Key != other[i].Key || !a[i].Value.Equal(other[i].Value) {
			return false
		}
	}

	return true
}

// Clone returns a deep copy of the attribute set (including nested slices/maps).
func (a Attributes) Clone() Attributes {
	if a == nil {
		return nil
	}

	out := make(Attributes, len(a))
	for i := range a {
		out[i] = KeyValue{Key: a[i].Key, Value: a[i].Value.Clone()}
	}

	return out
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
	case KindStr:
		return v.str == o.str
	case KindBool, KindInt, KindDouble:
		return v.num == o.num
	case KindBytes:
		return slices.Equal(v.Bytes(), o.Bytes())
	case KindSlice:
		return slices.EqualFunc(v.Slice(), o.Slice(), Value.Equal)
	case KindMap:
		return v.Map().Equal(o.Map())
	default:
		return false
	}
}

// Clone returns a deep copy of the value.
func (v Value) Clone() Value {
	switch v.kind {
	case KindBytes:
		return Value{kind: KindBytes, ref: slices.Clone(v.Bytes())}
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

// AsString returns a canonical string projection of the value, used by the string-keyed
// matching layer (postings) and for display. It is not the identity (that is the typed
// [Attributes.Hash]); two values of different kinds may project to the same string.
func (v Value) AsString() string {
	switch v.kind {
	case KindEmpty:
		return ""
	case KindStr:
		return v.str
	case KindBool:
		return strconv.FormatBool(v.Bool())
	case KindInt:
		return strconv.FormatInt(v.Int(), 10)
	case KindDouble:
		return strconv.FormatFloat(v.Double(), 'g', -1, 64)
	case KindBytes:
		return string(v.Bytes())
	case KindSlice, KindMap:
		return string(v.appendJSON(nil))
	default:
		return ""
	}
}

func (v Value) appendHash(dst []byte) []byte {
	dst = append(dst, byte(v.kind))

	switch v.kind {
	case KindEmpty:
	case KindStr:
		dst = appendLenString(dst, v.str)
	case KindBool, KindInt, KindDouble:
		dst = binary.BigEndian.AppendUint64(dst, v.num)
	case KindBytes:
		dst = appendLenBytes(dst, v.Bytes())
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

// appendJSON renders slices and maps as a stable JSON-ish string for AsString.
func (v Value) appendJSON(dst []byte) []byte {
	switch v.kind {
	case KindSlice:
		dst = append(dst, '[')
		for i, e := range v.Slice() {
			if i > 0 {
				dst = append(dst, ',')
			}

			dst = e.appendJSONScalarOrNested(dst)
		}

		return append(dst, ']')
	case KindMap:
		dst = append(dst, '{')
		for i, kv := range v.Map() {
			if i > 0 {
				dst = append(dst, ',')
			}

			dst = strconv.AppendQuote(dst, kv.Key)
			dst = append(dst, ':')
			dst = kv.Value.appendJSONScalarOrNested(dst)
		}

		return append(dst, '}')
	default:
		return dst
	}
}

func (v Value) appendJSONScalarOrNested(dst []byte) []byte {
	switch v.kind {
	case KindSlice, KindMap:
		return v.appendJSON(dst)
	case KindStr, KindBytes:
		return strconv.AppendQuote(dst, v.AsString())
	default:
		return append(dst, v.AsString()...)
	}
}

func appendLenString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))

	return append(dst, s...)
}

func appendLenBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))

	return append(dst, b...)
}
