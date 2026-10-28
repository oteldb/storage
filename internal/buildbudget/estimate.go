// Package buildbudget accounts flush working sets independently of compressed part sizes.
package buildbudget

import (
	"math"

	"github.com/oteldb/storage/signal"
)

// Add adds nonnegative byte estimates, saturating on overflow.
func Add(a, b int64) int64 {
	if b > math.MaxInt64-a {
		return math.MaxInt64
	}
	return a + b
}

// Mul multiplies nonnegative byte estimates, saturating on overflow.
func Mul(a, b int64) int64 {
	if b != 0 && a > math.MaxInt64/b {
		return math.MaxInt64
	}
	return a * b
}

// Attributes charges expanded keys, values, and per-entry materialization and dictionary slots.
func Attributes(attrs signal.Attributes) int64 {
	var n int64
	for _, kv := range attrs {
		n = Add(n, Add(128+int64(len(kv.Key)), value(kv.Value)))
	}
	return n
}

func value(v signal.Value) int64 {
	switch v.Kind() {
	case signal.KindStr:
		return 64 + int64(len(v.Str()))
	case signal.KindBytes:
		return 64 + int64(len(v.Bytes()))
	case signal.KindMap:
		return Add(64, Attributes(v.Map()))
	case signal.KindSlice:
		n := int64(64)
		for _, elem := range v.Slice() {
			n = Add(n, value(elem))
		}
		return n
	default:
		return 64
	}
}

// Flush estimates copied columns, sorting, codecs, identities, sidecars and map/offset arrays.
// It is a conservative admission estimate, not an allocator or RSS ceiling.
func Flush(rows, values, identities, streams, columns, side, workspace int64) int64 {
	n := int64(8 << 20)
	n = Add(n, Mul(values, 8))
	n = Add(n, Mul(rows, Add(64, Mul(columns, 16))))
	n = Add(n, Mul(identities, 4))
	n = Add(n, Mul(streams, 256))
	return Add(Add(n, Mul(side, 16)), workspace)
}
