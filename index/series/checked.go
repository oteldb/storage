package series

import (
	"math"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

// AddChecked registers s only when its canonical hash matches expected.
// It returns the canonical byte length; only scratch may change on a mismatch.
func (ix *Index) AddChecked(expected signal.SeriesID, s signal.Series) (int, bool) {
	ix.buf = s.AppendHashInput(ix.buf[:0])
	id := signal.HashBytes(ix.buf)
	if id != expected {
		return len(ix.buf), false
	}
	if _, ok := ix.byID[id]; ok {
		return len(ix.buf), true
	}
	if uint64(len(ix.rows)) > math.MaxUint32 {
		panic("series: local reference exhausted")
	}

	c := contextKey{resource: ix.internResource(s.Resource), scope: ix.internScope(s.Scope)}
	context, ok := ix.context[c]
	if !ok {
		context = uint32(len(ix.contexts))
		ix.context[c] = context
		ix.contexts = append(ix.contexts, identityContext{
			resource: ix.resources[c.resource], scope: ix.scopes[c.scope],
		})
		ix.bytes += memsize.MapEntry[contextKey, uint32]()
	}
	keys := ix.internKeys(s.Attributes)
	offset := uint64(len(ix.values))
	for i := range s.Attributes {
		ix.vbuf = signal.AppendValue(ix.vbuf[:0], s.Attributes[i].Value)
		ix.values = append(ix.values, ix.sym.Intern(ix.vbuf))
	}
	ix.byID[id] = uint32(len(ix.rows))
	ix.rows = append(ix.rows, row{id: id, offset: offset, context: context, keys: keys})
	ix.bytes += memsize.MapEntry[signal.SeriesID, uint32]()
	return len(ix.buf), true
}
