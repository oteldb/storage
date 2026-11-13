package series

import (
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/internal/buildbudget"
)

// EncodingBudget bounds the symbol workspace for selected identities of one [View].
type EncodingBudget struct {
	symbols   map[symbols.ID]struct{}
	contexts  map[uint32]struct{}
	bytes     int64
	retained  int64
	templates map[*attributeTemplate]struct{}
}

// Add includes identity i, counting shared payloads and estimator entries once.
func (b *EncodingBudget) Add(v View, i int) {
	if b.symbols == nil {
		b.symbols = make(map[symbols.ID]struct{})
		b.contexts = make(map[uint32]struct{})
		b.bytes = 256
	}
	r := v.rows[i]
	if _, ok := b.contexts[r.context]; !ok {
		b.contexts[r.context] = struct{}{}
		b.bytes = buildbudget.Add(b.bytes, v.contextWorkspace(r.context))
	}
	if r.keys == 0 {
		return
	}
	if r.keys&templateFlag != 0 {
		t := v.templates[r.keys&^templateFlag]
		if _, ok := b.templates[t]; !ok {
			if b.templates == nil {
				b.templates = make(map[*attributeTemplate]struct{})
			}
			b.templates[t] = struct{}{}
			b.bytes = buildbudget.Add(b.bytes, t.encodingWorkspace())
		}
		for j := range t.dynamic {
			b.addSymbol(v, v.values[int(r.offset)+int(j)])
		}
		return
	}
	for j, key := range v.keys[r.keys] {
		b.addSymbol(v, key)
		b.addSymbol(v, v.values[int(r.offset)+j])
	}
}

// Bytes reports the conservative symbol workspace, including this accumulator's maps.
func (b *EncodingBudget) Bytes() int64 { return b.bytes }

// Reset clears selected identities while charging retained estimator capacity.
func (b *EncodingBudget) Reset() {
	b.retained = max(b.retained, buildbudget.Mul(int64(len(b.symbols)+len(b.contexts)+len(b.templates)), 64))
	clear(b.symbols)
	clear(b.contexts)
	clear(b.templates)
	b.bytes = buildbudget.Add(256, b.retained)
}

// IdentityWorkspace bounds a single identity's workspace without allocating an estimator.
func (v View) IdentityWorkspace(i int) int64 {
	r := v.rows[i]
	n := buildbudget.Add(256, v.contextWorkspace(r.context))
	if r.keys&templateFlag != 0 {
		t := v.templates[r.keys&^templateFlag]
		n = buildbudget.Add(n, t.encodingWorkspace())
		for j := range t.dynamic {
			n = buildbudget.Add(n, v.symbolWorkspace(v.values[int(r.offset)+int(j)]))
		}
		return n
	}
	if r.keys != 0 {
		for j, key := range v.keys[r.keys] {
			n = buildbudget.Add(n, v.symbolWorkspace(key))
			n = buildbudget.Add(n, v.symbolWorkspace(v.values[int(r.offset)+j]))
		}
	}
	return n
}

func (v View) contextWorkspace(ref uint32) int64 {
	c := v.contexts[ref]
	n := buildbudget.Add(buildbudget.Attributes(c.resource.Attributes), buildbudget.Attributes(c.scope.Attributes))
	n = buildbudget.Add(n, int64(len(c.resource.SchemaURL)+len(c.scope.Name)+len(c.scope.Version)+len(c.scope.SchemaURL)))
	return buildbudget.Add(128, buildbudget.Mul(n, 4))
}

func (v View) symbolWorkspace(id symbols.ID) int64 {
	return buildbudget.Add(192, buildbudget.Mul(int64(len(v.symbols[id])), 4))
}

func (b *EncodingBudget) addSymbol(v View, id symbols.ID) {
	if _, ok := b.symbols[id]; ok {
		return
	}
	b.symbols[id] = struct{}{}
	b.bytes = buildbudget.Add(b.bytes, v.symbolWorkspace(id))
}
