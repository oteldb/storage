package buildbudget

import (
	"encoding/binary"

	"github.com/oteldb/storage/signal"
)

// SeriesEncoding bounds canonical identity bytes without allocating encoding buffers.
func SeriesEncoding(s signal.Series) int64 {
	n := int64(4 * binary.MaxVarintLen64)
	for _, data := range [][]byte{s.Resource.SchemaURL, s.Scope.Name, s.Scope.Version, s.Scope.SchemaURL} {
		n = Add(n, int64(len(data)))
	}
	n = Add(n, attributesEncoding(s.Resource.Attributes))
	n = Add(n, attributesEncoding(s.Scope.Attributes))
	return Add(n, attributesEncoding(s.Attributes))
}

func attributesEncoding(attrs signal.Attributes) int64 {
	n := int64(binary.MaxVarintLen64)
	for _, kv := range attrs {
		n = Add(n, binary.MaxVarintLen64+int64(len(kv.Key)))
		n = Add(n, valueEncoding(kv.Value))
	}
	return n
}

func valueEncoding(v signal.Value) int64 {
	n := int64(1)
	switch v.Kind() {
	case signal.KindEmpty:
		return n
	case signal.KindStr:
		return Add(n, binary.MaxVarintLen64+int64(len(v.Str())))
	case signal.KindBytes:
		return Add(n, binary.MaxVarintLen64+int64(len(v.Bytes())))
	case signal.KindBool, signal.KindInt, signal.KindDouble:
		return n + 8
	case signal.KindMap:
		return Add(n, attributesEncoding(v.Map()))
	case signal.KindSlice:
		n += binary.MaxVarintLen64
		for _, item := range v.Slice() {
			n = Add(n, valueEncoding(item))
		}
	}
	return n
}
