package identity

import (
	"encoding/binary"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/index/symbols"
)

// EncodeView appends selected registry identities in ascending canonical ID order.
// References and view must belong to the same immutable registry snapshot.
func EncodeView(dst []byte, view series.View, refs []uint32) []byte {
	tab := symbols.New()
	enc := encoder{tab: tab}
	remap := make(map[symbols.ID]symbols.ID)
	var body []byte
	var previous uint32
	var contextStart, contextEnd int
	haveContext := false
	body = binary.AppendUvarint(body, uint64(len(refs)))
	for _, ref := range refs {
		i := int(ref)
		id := view.ID(i)
		body = binary.BigEndian.AppendUint64(body, id.Hi)
		body = binary.BigEndian.AppendUint64(body, id.Lo)
		contextRef := view.ContextRef(i)
		if haveContext && previous == contextRef {
			body = append(body, body[contextStart:contextEnd]...)
		} else {
			resource, scope := view.Context(contextRef)
			contextStart = len(body)
			body = enc.appendContext(body, resource, scope)
			contextEnd = len(body)
			previous, haveContext = contextRef, true
		}
		keys, values := view.AttributeRefs(i)
		body = binary.AppendUvarint(body, uint64(len(keys)))
		for j, key := range keys {
			body = binary.AppendUvarint(body, uint64(remapSymbol(tab, remap, view, key)))
			body = binary.AppendUvarint(body, uint64(remapSymbol(tab, remap, view, values[j])))
		}
	}
	return appendObject(dst, tab, body)
}

func remapSymbol(tab *symbols.Table, remap map[symbols.ID]symbols.ID, view series.View, ref symbols.ID) symbols.ID {
	if id, ok := remap[ref]; ok {
		return id
	}
	id := tab.Intern(view.Symbol(ref))
	remap[ref] = id
	return id
}
