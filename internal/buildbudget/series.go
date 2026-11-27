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

// SeriesWorkspace estimates expanded identity allocations and canonical bytes in one traversal.
func SeriesWorkspace(s signal.Series) (workspace, encoded int64) {
	workspace = int64(len(s.Resource.SchemaURL) + len(s.Scope.SchemaURL) + len(s.Scope.Name) + len(s.Scope.Version))
	encoded = Add(4*binary.MaxVarintLen64, workspace)
	for _, attrs := range [3]signal.Attributes{s.Resource.Attributes, s.Scope.Attributes, s.Attributes} {
		attrsWork, attrsEncoded := attributeWorkspace(attrs)
		workspace = Add(workspace, attrsWork)
		encoded = Add(encoded, attrsEncoded)
	}
	return workspace, encoded
}

func attributeWorkspace(attrs signal.Attributes) (workspace, encoded int64) {
	encoded = binary.MaxVarintLen64
	for _, kv := range attrs {
		valWork, valEncoded := valueWorkspace(kv.Value)
		workspace = Add(workspace, Add(128+int64(len(kv.Key)), valWork))
		encoded = Add(encoded, Add(binary.MaxVarintLen64+int64(len(kv.Key)), valEncoded))
	}
	return workspace, encoded
}

func valueWorkspace(v signal.Value) (workspace, encoded int64) {
	switch v.Kind() {
	case signal.KindStr:
		n := int64(len(v.Str()))
		return 64 + n, 1 + binary.MaxVarintLen64 + n
	case signal.KindBytes:
		n := int64(len(v.Bytes()))
		return 64 + n, 1 + binary.MaxVarintLen64 + n
	case signal.KindBool, signal.KindInt, signal.KindDouble:
		return 64, 9
	case signal.KindMap:
		workspace, encoded := attributeWorkspace(v.Map())
		return Add(64, workspace), Add(1, encoded)
	case signal.KindSlice:
		workspace, encoded := int64(64), int64(1+binary.MaxVarintLen64)
		for _, item := range v.Slice() {
			childWork, childEncoded := valueWorkspace(item)
			workspace = Add(workspace, childWork)
			encoded = Add(encoded, childEncoded)
		}
		return workspace, encoded
	default:
		return 64, 1
	}
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
