package series

import (
	"encoding/binary"

	"github.com/oteldb/storage/signal"
)

// Context borrows identity i's immutable resource and scope.
func (v View) Context(i int) (signal.Resource, signal.Scope) {
	c := v.contexts[v.rows[i].context]
	return c.resource, c.scope
}

// AttributeCount returns the number of point attributes on identity i.
func (v View) AttributeCount(i int) int { return len(v.keys[v.rows[i].keys]) }

// EncodedAttributes visits identity i's point attributes in stored order.
// The borrowed keys and type-tagged values must not be mutated.
func (v View) EncodedAttributes(i int, visit func(key, encodedValue []byte)) {
	r := v.rows[i]
	for j, key := range v.keys[r.keys] {
		visit(v.symbols[key], v.symbols[v.values[int(r.offset)+j]])
	}
}

// AppendCanonical appends identity i's [signal.Series.AppendHashInput] without decoding point values.
func (v View) AppendCanonical(dst []byte, i int) []byte {
	r := v.rows[i]
	c := v.contexts[r.context]
	dst = c.resource.AppendHashInput(dst)
	dst = c.scope.AppendHashInput(dst)
	keys := v.keys[r.keys]
	dst = binary.AppendUvarint(dst, uint64(len(keys)))
	for j, key := range keys {
		name := v.symbols[key]
		dst = binary.AppendUvarint(dst, uint64(len(name)))
		dst = append(dst, name...)
		dst = append(dst, v.symbols[v.values[int(r.offset)+j]]...)
	}
	return dst
}
