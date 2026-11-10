package series

import (
	"encoding/binary"
	"math"

	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

// AddView registers identity i using its established ID and encoded point values.
// The index owns its copy; the source view can be discarded afterward.
func (ix *Index) AddView(v View, i int) signal.SeriesID {
	r := v.rows[i]
	if _, ok := ix.byID[r.id]; ok {
		return r.id
	}
	if uint64(len(ix.rows)) > math.MaxUint32 {
		panic("series: local reference exhausted")
	}
	c := v.contexts[r.context]
	context := ix.internContext(c.resource, c.scope)
	keys := uint32(0)
	if r.keys != 0 {
		ix.kbuf = ix.kbuf[:0]
		for _, key := range v.keys[r.keys] {
			ix.kbuf = binary.LittleEndian.AppendUint32(ix.kbuf, uint32(ix.sym.Intern(v.symbols[key])))
		}
		keys = ix.internKeyBuffer(len(v.keys[r.keys]))
	}
	offset := uint64(len(ix.values))
	for j := range v.keys[r.keys] {
		ix.values = append(ix.values, ix.sym.Intern(v.symbols[v.values[int(r.offset)+j]]))
	}
	ix.byID[r.id] = uint32(len(ix.rows))
	ix.rows = append(ix.rows, row{id: r.id, offset: offset, context: context, keys: keys})
	ix.bytes += memsize.MapEntry[signal.SeriesID, uint32]()
	return r.id
}

func (ix *Index) internContext(resource signal.Resource, scope signal.Scope) uint32 {
	c := contextKey{resource: ix.internResource(resource), scope: ix.internScope(scope)}
	ref, ok := ix.context[c]
	if !ok {
		ref = uint32(len(ix.contexts))
		ix.context[c] = ref
		ix.contexts = append(ix.contexts, identityContext{
			resource: ix.resources[c.resource], scope: ix.scopes[c.scope],
		})
		ix.bytes += memsize.MapEntry[contextKey, uint32]()
	}
	return ref
}

func (ix *Index) internKeyBuffer(n int) uint32 {
	if ref, ok := ix.keysets[string(ix.kbuf)]; ok {
		return ref
	}
	keys := make([]symbols.ID, n)
	for i := range keys {
		keys[i] = symbols.ID(binary.LittleEndian.Uint32(ix.kbuf[i*4:]))
	}
	ref := uint32(len(ix.keys))
	ix.keys = append(ix.keys, keys)
	ix.keysets[string(ix.kbuf)] = ref
	ix.bytes += memsize.MapEntry[string, uint32]() + int64(len(ix.kbuf)) + memsize.Slice(keys)
	return ref
}
