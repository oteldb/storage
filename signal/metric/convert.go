package metric

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/oteldb/storage/signal"
)

// convertMap projects an OTLP attribute map to internal typed [signal.Attributes]. It is
// the pdata→internal boundary: byte values are copied out of the pdata buffers so the
// internal model owns them and pdata never reaches the hot storage path.
func convertMap(m pcommon.Map) signal.Attributes {
	if m.Len() == 0 {
		return nil
	}

	kvs := make([]signal.KeyValue, 0, m.Len())
	for k, v := range m.All() {
		kvs = append(kvs, signal.KeyValue{Key: []byte(k), Value: convertValue(v)})
	}

	return signal.NewAttributes(kvs...)
}

// convertValue projects an OTLP AnyValue to the internal typed [signal.Value],
// preserving type (and recursing into slices/maps).
func convertValue(v pcommon.Value) signal.Value {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return signal.StringValue([]byte(v.Str()))
	case pcommon.ValueTypeBool:
		return signal.BoolValue(v.Bool())
	case pcommon.ValueTypeInt:
		return signal.IntValue(v.Int())
	case pcommon.ValueTypeDouble:
		return signal.DoubleValue(v.Double())
	case pcommon.ValueTypeBytes:
		return signal.BytesValue(v.Bytes().AsRaw())
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		vs := make([]signal.Value, s.Len())
		for i := range s.Len() {
			vs[i] = convertValue(s.At(i))
		}

		return signal.SliceValue(vs...)
	case pcommon.ValueTypeMap:
		return signal.MapValue(convertMap(v.Map())...)
	default: // ValueTypeEmpty
		return signal.EmptyValue()
	}
}
