package series

import (
	"slices"

	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/signal"
)

// ContextRef returns identity i's resource/scope reference, scoped to this view's registry.
func (v View) ContextRef(i int) uint32 { return v.rows[i].context }

// Context borrows the immutable resource and scope of a valid context reference.
func (v View) Context(ref uint32) (signal.Resource, signal.Scope) {
	c := v.contexts[ref]
	return c.resource, c.scope
}

// AttributeRefs borrows identity i's aligned key and encoded-value symbol references.
// The slices and their referenced symbols are immutable and scoped to this view's registry.
func (v View) AttributeRefs(i int) (keys, values []symbols.ID) {
	r := v.rows[i]
	if r.keys == 0 {
		return nil, nil
	}
	keys = v.keys[r.keys]
	values = v.values[r.offset : r.offset+uint64(len(keys))]
	return slices.Clip(keys), slices.Clip(values)
}

// Symbol borrows the immutable bytes of a valid symbol reference.
// Value symbols contain the canonical type-tagged encoding, including nested values.
func (v View) Symbol(ref symbols.ID) []byte {
	b := v.symbols[ref]
	return slices.Clip(b)
}
