package recordengine

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/signal"
)

// BenchmarkKeyInfoSlice measures keyInfoSlice's key sort — run once per Keys() call, over every
// distinct attribute key the engine has ever seen.
func BenchmarkKeyInfoSlice(b *testing.B) {
	const n = 4096

	scopes := make(map[string]KeyScope, n)
	for i := range n {
		scopes[fmt.Sprintf("attribute.key.%04d", i)] = KeyScopeResource
	}

	b.ReportAllocs()

	for b.Loop() {
		keyInfoSlice(scopes)
	}
}

// BenchmarkDistinctRecordKeys measures the per-flush distinct-key extraction (decode + sort) over a
// part's serialized-attributes column.
func BenchmarkDistinctRecordKeys(b *testing.B) {
	const (
		rows         = 4096
		distinctKeys = 64
	)

	schema := NewSchema(
		Column{Name: "attrs", Kind: KindBytes, Bloom: BloomAttrs},
	)

	c := newRecordCols(schema, rows, fullSel(schema))
	for i := range rows {
		k := i % distinctKeys
		attrs := signal.NewAttributes(signal.KeyValue{
			Key:   fmt.Appendf(nil, "attribute.key.%04d", k),
			Value: signal.StringValue([]byte("some-value")),
		}).AppendHashInput(nil)

		c.appendClone(rec{ts: int64(i), bytes: [][]byte{attrs}})
	}

	b.ReportAllocs()

	for b.Loop() {
		distinctRecordKeys(schema, c)
	}
}
