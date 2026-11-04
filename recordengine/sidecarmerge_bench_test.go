package recordengine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

func BenchmarkSidecarMergeBudget(b *testing.B) {
	for _, budget := range []int64{32 << 20, 64 << 20} {
		b.Run(fmt.Sprintf("MiB=%d", budget>>20), func(b *testing.B) {
			be, err := file.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			e := benchMergeEngine(b, be, 4, 32<<10)
			e.cfg.MergeMemoryBytes = budget
			benchSidecarBudget(b, e, be)
		})
	}
}

func benchSidecarBudget(b *testing.B, e *Engine, be backend.Backend) {
	b.Helper()
	src := e.parts
	var logical, rows int64
	for _, p := range src {
		logical += p.decodedBytes()
		rows += p.rows()
	}
	b.SetBytes(logical)
	b.ReportAllocs()
	var peak, grant int64
	old := mergeResidentObserver
	mergeResidentObserver = func(p, _, _, g int64) { peak = max(peak, p); grant = max(grant, g) }
	defer func() { mergeResidentObserver = old }()
	var parts int
	var stored int64
	for b.Loop() {
		out, err := e.compactParts(context.Background(), src, minInt64, 1<<30, nil)
		if err != nil {
			b.Fatal(err)
		}
		parts, stored = len(out), 0
		for _, p := range out {
			stored += p.reader.Manifest().DiskBytes
		}
		b.StopTimer()
		for _, p := range out {
			if err := deletePart(context.Background(), be, p.prefix); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(parts), "parts")
	b.ReportMetric(float64(stored)/float64(rows), "stored-B/row")
	b.ReportMetric(float64(peak), "writers-peak-B")
	b.ReportMetric(float64(grant), "grant-B")
}

func BenchmarkSidecarRepeatedMergeBudget(b *testing.B) {
	for _, budget := range []int64{32 << 20, 64 << 20} {
		b.Run(fmt.Sprintf("MiB=%d", budget>>20), func(b *testing.B) {
			be, err := file.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			schema := NewSchema(
				Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomFullText},
				Column{Name: "attrs", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomAttrs},
				Column{Name: "trace", Kind: KindBytes, Codec: chunk.CodecBytesRaw, Bloom: BloomEquality},
			)
			e := New(Config{Schema: schema, Backend: be, Prefix: "bench/repeated", MergeMemoryBytes: budget})
			bodies := make([][]byte, 64)
			attrs := make([][]byte, 1024)
			for i := range bodies {
				bodies[i] = fmt.Appendf(nil, "request path=/api/items/%d %s", i, strings.Repeat("field=value ", 64))
			}
			for i := range attrs {
				attrs[i] = attrsCell(b, signal.KeyValue{Key: []byte("host"), Value: signal.StringValue(fmt.Appendf(nil, "node-%d", i))})
			}
			for part := range 4 {
				for stream := range 16 {
					id := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue(fmt.Appendf(nil, "svc-%d", stream))})}}
					batch := &Batch{Stream: id.Hash(), Identity: func() signal.Series { return id }, Bytes: make([][][]byte, 3)}
					for row := range 2048 {
						n := part*(32<<10) + stream*2048 + row
						batch.Ts = append(batch.Ts, streamedDay+int64(n))
						batch.Bytes[0] = append(batch.Bytes[0], bodies[n%len(bodies)])
						batch.Bytes[1] = append(batch.Bytes[1], attrs[n%len(attrs)])
						batch.Bytes[2] = append(batch.Bytes[2], fmt.Appendf(nil, "%032x", n))
					}
					if _, err := e.AppendBatch(batch, AppendLimits{}); err != nil {
						b.Fatal(err)
					}
				}
				if err := e.Flush(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
			benchSidecarBudget(b, e, be)
		})
	}
}
