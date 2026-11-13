package series

import (
	"math"
	"slices"

	"github.com/oteldb/storage/signal"
)

func (t *attributeTemplate) attributes(v View, r row, dst signal.Attributes) signal.Attributes {
	dst = slices.Grow(dst, len(t.fields))[:len(t.fields)]
	for i, f := range t.fields {
		out := &dst[i]
		out.Key = t.canonical[f.keyOffset : f.keyOffset+f.keyLength]
		if f.slot != math.MaxUint32 || f.kind == signal.KindBool && f.scalar > 1 {
			_, encoded := v.encodedField(r, f, t)
			out.Value = decodeTemplateValue(encoded)
			continue
		}
		switch f.kind {
		case signal.KindEmpty:
			out.Value = signal.EmptyValue()
		case signal.KindInt:
			out.Value = signal.IntValue(int64(f.scalar))
		case signal.KindDouble:
			out.Value = signal.DoubleValue(math.Float64frombits(f.scalar))
		case signal.KindBool:
			out.Value = signal.BoolValue(f.scalar == 1)
		case signal.KindMap, signal.KindSlice:
			_, encoded := v.encodedField(r, f, t)
			out.Value = decodeTemplateValue(encoded)
		case signal.KindStr, signal.KindBytes:
			offset, length := uint32(f.scalar>>32), uint32(f.scalar)
			data := t.canonical[offset : offset+length]
			if f.kind == signal.KindStr {
				out.Value = signal.StringValue(data)
			} else {
				out.Value = signal.BytesValue(data)
			}
		}
	}
	return dst
}

func decodeTemplateValue(encoded []byte) signal.Value {
	value, _, err := signal.DecodeValue(encoded)
	if err != nil {
		panic(err)
	}
	return value
}
