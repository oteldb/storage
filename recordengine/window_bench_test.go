package recordengine

import (
	"context"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// BenchmarkAppendWindow guards the unfiltered window gather: it flushes one stream of n ts-sorted
// rows to a part, decodes it once, then times [appendWindowRows] over a narrow window (100 of n rows
// in the middle) — the shape where a stream's range dwarfs the query window, so the binary search
// pays off over a per-row timestamp scan of the whole range.
func BenchmarkAppendWindow(b *testing.B) {
	ctx := context.Background()
	schema := NewSchema(
		Column{Name: "sev", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict},
	)
	e := New(Config{Schema: schema, Backend: backend.Memory(), Prefix: "t/w"})

	const n = 50_000
	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}

	ts := make([]int64, n)
	sev := make([]int64, n)
	bodies := make([][]byte, n)
	for i := range n {
		ts[i], sev[i], bodies[i] = int64(i), int64(i%5), []byte("GET /api/v1/thing status=200 done")
	}

	if _, err := e.AppendBatch(&Batch{
		Stream: series.Hash(), Identity: func() signal.Series { return series },
		Ts: ts, Ints: [][]int64{sev}, Bytes: [][][]byte{bodies},
	}, AppendLimits{}); err != nil {
		b.Fatal(err)
	}

	if err := e.Flush(ctx); err != nil {
		b.Fatal(err)
	}

	sel := fullSel(schema)

	cols, err := e.parts[0].readCols(ctx, sel, nil)
	if err != nil {
		b.Fatal(err)
	}

	rng := e.parts[0].ranges[series.Hash()]
	start, end := int64(n/2), int64(n/2+99) // 100 rows in the middle

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		acc := newRecordCols(schema, 128, sel)
		appendWindowRows(acc, cols, rng, start, end)
	}
}
