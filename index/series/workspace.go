package series

import (
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/internal/buildbudget"
)

// EncodingBudget bounds the symbol workspace for selected identities of one [View].
type EncodingBudget struct {
	symbols   map[symbols.ID]struct{}
	dense     []uint64
	contexts  map[uint32]struct{}
	bytes     int64
	retained  int64
	templates map[*attributeTemplate]struct{}
}

// DenseBudgetBytes prices a bitmap when the selection is large enough to amortize it.
func (v View) DenseBudgetBytes(selected int) int64 {
	const maxSymbols = 8 << 20
	if selected < 1024 || len(v.symbols) > maxSymbols || len(v.symbols)/8 > selected {
		return 0
	}
	return int64((len(v.symbols) + 63) / 64 * 8)
}

// UseDenseSymbols initializes a fresh budget with admitted [View.DenseBudgetBytes].
func (b *EncodingBudget) UseDenseSymbols(bytes int64) {
	b.dense = make([]uint64, bytes/8)
	b.bytes = bytes
}

// Add includes identity i, counting shared payloads and estimator entries once.
func (b *EncodingBudget) Add(v View, i int) {
	if b.contexts == nil {
		b.contexts = make(map[uint32]struct{})
		if len(b.dense) == 0 {
			b.symbols = make(map[symbols.ID]struct{})
		}
		b.bytes = buildbudget.Add(256+int64(cap(b.dense))*8, b.retained)
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

// Bytes reports the conservative symbol workspace, including the estimator.
func (b *EncodingBudget) Bytes() int64 { return b.bytes }

// Reset clears selected identities while charging retained estimator capacity.
func (b *EncodingBudget) Reset() {
	b.retained = max(b.retained, buildbudget.Mul(int64(len(b.symbols)+len(b.contexts)+len(b.templates)), 64))
	clear(b.symbols)
	clear(b.dense)
	clear(b.contexts)
	clear(b.templates)
	b.bytes = buildbudget.Add(256+int64(cap(b.dense))*8, b.retained)
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
	resource, scope := &v.resources[c.resource].value, &v.scopes[c.scope].value
	n := buildbudget.Add(c.attributeWorkspace, int64(len(resource.SchemaURL)+len(scope.Name)+len(scope.Version)+len(scope.SchemaURL)))
	return buildbudget.Add(128, buildbudget.Mul(n, 4))
}

func (v View) symbolWorkspace(id symbols.ID) int64 {
	return buildbudget.Add(192, buildbudget.Mul(int64(len(v.symbols[id])), 4))
}

func (b *EncodingBudget) addSymbol(v View, id symbols.ID) {
	if len(b.dense) != 0 {
		word := &b.dense[id/64]
		mask := uint64(1) << (id % 64)
		if *word&mask != 0 {
			return
		}
		*word |= mask
	} else {
		if _, ok := b.symbols[id]; ok {
			return
		}
		b.symbols[id] = struct{}{}
	}
	b.bytes = buildbudget.Add(b.bytes, v.symbolWorkspace(id))
}
