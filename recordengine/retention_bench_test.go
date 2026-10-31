package recordengine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

func BenchmarkRetentionBoundary(b *testing.B) {
	const rows, bodyBytes, turns = 2048, 4096, 60
	for _, backlog := range []int{0, 32} {
		b.Run(fmt.Sprintf("backlog=%d", backlog), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(rows * (bodyBytes + 8))
			var bytesOut, merges int64
			var parts int
			ctx := context.Background()
			for b.Loop() {
				b.StopTimer()
				base := int64(48 * time.Hour)
				now := time.Unix(0, base).Add(7 * 24 * time.Hour)
				e := New(Config{Schema: NewSchema(Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict}),
					Backend: backend.Memory(), Prefix: "retention/logs", MergeMemoryBytes: -1,
					Now: func() time.Time { return now }})
				series := signal.Series{}
				body := [][]byte{[]byte(strings.Repeat("a", bodyBytes)), []byte(strings.Repeat("b", bodyBytes))}
				batch := &Batch{Stream: series.Hash(), Identity: func() signal.Series { return series }, Bytes: make([][][]byte, 1)}
				for i := range rows {
					batch.Ts = append(batch.Ts, base+int64(i)*int64(10*time.Second))
					batch.Bytes[0] = append(batch.Bytes[0], body[i%len(body)])
				}
				if _, err := e.AppendBatch(batch, AppendLimits{}); err != nil {
					b.Fatal(err)
				}
				if err := e.Flush(ctx); err != nil {
					b.Fatal(err)
				}
				for i := range backlog {
					batch.Ts = []int64{base + int64(48*time.Hour) + int64(i)}
					batch.Bytes[0] = [][]byte{body[0]}
					if _, err := e.AppendBatch(batch, AppendLimits{}); err != nil {
						b.Fatal(err)
					}
					if err := e.Flush(ctx); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				for turn := range turns {
					now = now.Add(10 * time.Second)
					result, err := e.merge(ctx, MergeOptions{RetainFrom: base + int64(turn+1)*int64(10*time.Second), Background: true})
					if err != nil {
						b.Fatal(err)
					}
					bytesOut += result.bytesOut
					if result.parts > 0 {
						merges++
					}
				}
				b.StopTimer()
				parts = e.PartCount()
				if err := e.Close(ctx); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(bytesOut)/float64(b.N), "rewrite-B/op")
			b.ReportMetric(float64(merges)/float64(b.N), "merges/op")
			b.ReportMetric(float64(parts), "parts")
		})
	}
}
