package engine

import "slices"

// descriptorProofs indexes catalog-confirmed membership by the head's identity ordinal.
type descriptorProofs struct {
	bits    []uint64
	lineage uint64
}

func (p *descriptorProofs) has(ref uint32) bool {
	word := int(ref / 64)
	return word < len(p.bits) && p.bits[word]&(1<<(ref%64)) != 0
}

func (p *descriptorProofs) add(ref uint32, lineage uint64) {
	if lineage < p.lineage {
		return
	}
	if lineage != p.lineage {
		clear(p.bits)
		p.lineage = lineage
	}
	word := int(ref / 64)
	if n := word + 1 - len(p.bits); n > 0 {
		p.bits = slices.Grow(p.bits, n)[:word+1]
	}
	p.bits[word] |= 1 << (ref % 64)
}
