package series

import (
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/signal"
)

// View is an immutable snapshot, safe to read while its index receives additions.
type View struct {
	rows     []row
	contexts []identityContext
	keys     [][]symbols.ID
	values   []symbols.ID
	symbols  [][]byte
}

// View captures the append-only arenas without materializing identities.
func (ix *Index) View() View {
	return View{rows: ix.rows, contexts: ix.contexts, keys: ix.keys, values: ix.values, symbols: ix.sym.Snapshot()}
}

// Len returns the number of identities captured by v.
func (v View) Len() int { return len(v.rows) }

// ID returns identity i's canonical ID.
func (v View) ID(i int) signal.SeriesID { return v.rows[i].id }

// At materializes identity i, reusing dst for its point attributes.
func (v View) At(i int, dst signal.Attributes) signal.Series {
	r := v.rows[i]
	c := v.contexts[r.context]
	if r.keys == 0 {
		return signal.Series{Resource: c.resource, Scope: c.scope}
	}
	keys := v.keys[r.keys]
	if dst == nil {
		dst = make(signal.Attributes, 0, len(keys))
	}
	dst = dst[:0]
	for j, key := range keys {
		value, _, err := signal.DecodeValue(v.symbols[v.values[int(r.offset)+j]])
		if err != nil {
			panic(err)
		}
		dst = append(dst, signal.KeyValue{Key: v.symbols[key], Value: value})
	}
	return signal.Series{Resource: c.resource, Scope: c.scope, Attributes: dst}
}
