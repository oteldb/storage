package recordengine

import (
	"context"
	"strings"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func BenchmarkProjectedFetchBudget(b *testing.B) {
	const rows = 4096
	ctx := context.Background()
	e := New(Config{Schema: NewSchema(
		Column{Name: "severity", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "severity_text", Kind: KindBytes, Codec: chunk.CodecDict},
		Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict},
	), Backend: backend.Memory(), Prefix: "bench/projected"})
	series := signal.Series{}
	batch := &Batch{Stream: series.Hash(), Identity: func() signal.Series { return series },
		Ints: make([][]int64, 1), Bytes: make([][][]byte, 2)}
	body := []byte(strings.Repeat("b", 1024))
	for i := range rows {
		batch.Ts = append(batch.Ts, int64(i))
		batch.Ints[0] = append(batch.Ints[0], int64(9+i%3))
		batch.Bytes[0] = append(batch.Bytes[0], []byte("info"))
		batch.Bytes[1] = append(batch.Bytes[1], body)
	}
	if _, err := e.AppendBatch(batch, AppendLimits{}); err != nil {
		b.Fatal(err)
	}
	if err := e.Flush(ctx); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		start, end int64
		rows       int
	}{
		{"full", 0, rows - 1, rows},
		{"one_row", 2000, 2000, 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			budget := readbudget.New(32 << 20)
			bctx := readbudget.With(ctx, budget)
			r := fetch.Request{Start: tc.start, End: tc.end, Projection: []string{"severity", "severity_text"}}
			b.SetBytes(int64(tc.rows * (8 + 8 + 4)))
			b.ReportAllocs()
			for b.Loop() {
				it, err := e.Fetch(bctx, r)
				if err != nil {
					b.Fatal(err)
				}
				got, err := fetch.Drain(bctx, it)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != 1 || len(got[0].Timestamps) != tc.rows {
					b.Fatal("unexpected projected result")
				}
			}
			b.ReportMetric(float64(budget.Peak()), "reserved-B")
		})
	}
}
