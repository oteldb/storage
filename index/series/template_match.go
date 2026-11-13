package series

import (
	"bytes"
	"math"

	"github.com/oteldb/storage/signal"
)

func (ix *Index) matchesTemplate(attrs signal.Attributes, t *attributeTemplate) bool {
	if len(attrs) != len(t.fields) {
		return false
	}
	for i := range attrs {
		f := &t.fields[i]
		if !bytes.Equal(attrs[i].Key, t.canonical[f.keyOffset:f.keyOffset+f.keyLength]) {
			return false
		}
		if f.slot != math.MaxUint32 {
			continue
		}
		value := &attrs[i].Value
		if value.Kind() != f.kind {
			return false
		}
		switch f.kind {
		case signal.KindEmpty:
		case signal.KindInt:
			if uint64(value.Int()) != f.scalar {
				return false
			}
		case signal.KindDouble:
			if math.Float64bits(value.Double()) != f.scalar {
				return false
			}
		case signal.KindStr, signal.KindBytes:
			offset, length := uint32(f.scalar>>32), uint32(f.scalar)
			data := value.Str()
			if f.kind == signal.KindBytes {
				data = value.Bytes()
			}
			if !bytes.Equal(data, t.canonical[offset:offset+length]) {
				return false
			}
		default:
			// Encoding preserves raw boolean payloads and nested typed values exactly.
			ix.vbuf = signal.AppendValue(ix.vbuf[:0], *value)
			if !bytes.Equal(ix.vbuf, t.canonical[f.valueOffset:f.valueOffset+f.valueLength]) {
				return false
			}
		}
	}
	return true
}

type templateMatchPolicy struct{ misses, skip uint8 }

func (p *templateMatchPolicy) probe() bool {
	if p.skip != 0 {
		p.skip--
		return false
	}
	return true
}

func (p *templateMatchPolicy) observe(matched bool) {
	if matched {
		p.misses = 0
		return
	}
	if p.misses < 8 {
		p.misses++
	}
	if p.misses == 8 {
		p.skip = 63
	}
}
