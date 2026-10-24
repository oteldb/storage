package recordengine

import (
	"context"
	"strings"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/signal"
)

const (
	budgetLogRows = 2048
	budgetLogBody = 4096
)

func budgetLogEngine(tb testing.TB, be backend.Backend, parts int) *Engine {
	tb.Helper()
	e := New(Config{
		Schema:  NewSchema(Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict}),
		Backend: be, Prefix: "budget/logs", MaxPartBytes: 64 << 20, MergeMemoryBytes: 32 << 20,
		MergeCompression: compress.AlgorithmZSTD, MergeCompressionLevel: compress.LevelFast,
	})
	series := signal.Series{}
	body := [][]byte{[]byte(strings.Repeat("a", budgetLogBody)), []byte(strings.Repeat("b", budgetLogBody))}
	for p := range parts {
		batch := &Batch{Stream: series.Hash(), Identity: func() signal.Series { return series }, Bytes: make([][][]byte, 1)}
		for i := range budgetLogRows {
			batch.Ts = append(batch.Ts, streamedDay+int64(p*budgetLogRows+i))
			batch.Bytes[0] = append(batch.Bytes[0], body[i%len(body)])
		}
		if _, err := e.AppendBatch(batch, AppendLimits{}); err != nil {
			tb.Fatal(err)
		}
		if err := e.Flush(context.Background()); err != nil {
			tb.Fatal(err)
		}
	}
	return e
}

// BenchmarkMergeDiskBudget excludes fixture ingestion and drains the same highly compressible logs.
func BenchmarkMergeDiskBudget(b *testing.B) {
	const parts = 24
	b.SetBytes(parts * budgetLogRows * (budgetLogBody + 8))
	b.ReportAllocs()
	var finalParts int
	for b.Loop() {
		b.StopTimer()
		e := budgetLogEngine(b, backend.Memory(), parts)
		b.StartTimer()
		for cycle := range 100 {
			before := append([]*part(nil), e.parts...)
			if err := e.MergeWith(context.Background(), MergeOptions{Force: true}); err != nil {
				b.Fatal(err)
			}
			if len(before) == len(e.parts) && before[0] == e.parts[0] {
				break
			}
			if cycle == 99 {
				b.Fatal("compaction did not converge")
			}
		}
		finalParts = e.PartCount()
		b.StopTimer()
		if err := e.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(finalParts), "parts")
}
