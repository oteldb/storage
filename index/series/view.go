package series

import (
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/internal/buildbudget"
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

// EncodingBytes bounds the series-record bytes, including repeated resource/scope references.
func (v View) EncodingBytes(i int) int64 {
	r := v.rows[i]
	c := v.contexts[r.context]
	attrs := len(c.resource.Attributes) + len(c.scope.Attributes)
	if r.keys != 0 {
		attrs += len(v.keys[r.keys])
	}
	// A symbol ID is uint32 (at most five varint bytes); every attribute stores two IDs.
	return buildbudget.Add(80, buildbudget.Mul(int64(attrs), 10))
}

// EncodingWorkspace bounds the symbol encoder's copies and lookup tables for this snapshot.
func (v View) EncodingWorkspace() int64 {
	var n int64
	for _, data := range v.symbols {
		n = buildbudget.Add(n, buildbudget.Add(128, buildbudget.Mul(int64(len(data)), 4)))
	}
	// Resource/scope values are interned in memory but get type-tagged encodings on disk.
	for i := range v.contexts {
		c := &v.contexts[i]
		n = buildbudget.Add(n, buildbudget.Mul(buildbudget.Attributes(c.resource.Attributes), 4))
		n = buildbudget.Add(n, buildbudget.Mul(buildbudget.Attributes(c.scope.Attributes), 4))
	}
	return n
}

// MaterializationBytes bounds a decoded point-attribute slice, including nested values.
func (v View) MaterializationBytes(i int) int64 {
	r := v.rows[i]
	if r.keys == 0 {
		return 0
	}
	n := buildbudget.Mul(int64(len(v.keys[r.keys])), 160)
	for j := range v.keys[r.keys] {
		data := v.symbols[v.values[int(r.offset)+j]]
		if len(data) > 0 && (signal.ValueKind(data[0]) == signal.KindSlice || signal.ValueKind(data[0]) == signal.KindMap) {
			n = buildbudget.Add(n, buildbudget.Mul(int64(len(data)), 128))
		}
	}
	return n
}

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
