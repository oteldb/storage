package recordengine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// benchLogSchema is the log signal's column set: small ints, a low-cardinality severity, a templated
// body, a near-unique dictionary-coded trace id, and serialized attributes.
var benchLogSchema = NewSchema(
	Column{Name: "observed", Kind: KindInt64, Codec: chunk.CodecT64},
	Column{Name: "severity", Kind: KindInt64, Codec: chunk.CodecT64},
	Column{Name: "severity_text", Kind: KindBytes, Codec: chunk.CodecDict},
	Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomFullText},
	Column{Name: "trace_id", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomEquality},
	Column{Name: "attrs", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomAttrs},
)

// benchMergeEngine flushes parts log-shaped parts of rows records each, spread over 16 streams.
func benchMergeEngine(b *testing.B, be backend.Backend, parts, rows int) *Engine {
	b.Helper()

	ctx := context.Background()
	e := New(Config{Schema: benchLogSchema, Backend: be, Prefix: "bench/logs", MergeMemoryBytes: -1})
	r := rand.New(rand.NewPCG(1, 2))

	const streams = 16

	sevText := []string{"DEBUG", "INFO", "WARN", "ERROR"}

	for p := range parts {
		for s := range streams {
			series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
				signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc-" + strconv.Itoa(s)))},
			)}}

			n := rows / streams
			batch := &Batch{
				Stream: series.Hash(), Identity: func() signal.Series { return series },
				Ints: make([][]int64, 2), Bytes: make([][][]byte, 4),
			}

			for i := range n {
				ts := int64(p*n+i)*1_000_000 + r.Int64N(1000)
				sev := r.IntN(len(sevText))

				batch.Ts = append(batch.Ts, ts)
				batch.Ints[0] = append(batch.Ints[0], ts+r.Int64N(50_000))
				batch.Ints[1] = append(batch.Ints[1], int64(sev*4+1))
				batch.Bytes[0] = append(batch.Bytes[0], []byte(sevText[sev]))
				batch.Bytes[1] = append(batch.Bytes[1], fmt.Appendf(nil,
					"handled request method=GET path=/api/v1/items/%d status=%d duration_ms=%d",
					r.IntN(200), 200+100*r.IntN(4), r.IntN(500)))
				batch.Bytes[2] = append(batch.Bytes[2], fmt.Appendf(nil, "%016x%016x", r.Uint64(), r.Uint64()))
				batch.Bytes[3] = append(batch.Bytes[3], signal.NewAttributes(
					signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte("node-" + strconv.Itoa(r.IntN(12))))},
					signal.KeyValue{Key: []byte("user"), Value: signal.StringValue([]byte("u" + strconv.Itoa(r.IntN(3000))))},
				).AppendHashInput(nil))
			}

			if _, err := e.AppendBatch(batch, AppendLimits{}); err != nil {
				b.Fatal(err)
			}
		}

		if err := e.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}

	return e
}

// BenchmarkMergeCompact times one record merge of every flushed part into one output part: reading
// the sources, the stream sweep, and writing the output with its blooms and sidecars. Throughput is
// the sources' decoded bytes.
func BenchmarkMergeCompact(b *testing.B) {
	for _, tc := range []struct {
		name string
		open func(b *testing.B) backend.Backend
	}{
		{"memory", func(*testing.B) backend.Backend { return backend.Memory() }},
		{"file", func(b *testing.B) backend.Backend {
			b.Helper()

			fb, err := file.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}

			return fb
		}},
	} {
		for _, shape := range []struct{ parts, rows int }{{4, 32 << 10}, {8, 64 << 10}} {
			b.Run(fmt.Sprintf("%s/parts=%d/rows=%d", tc.name, shape.parts, shape.rows), func(b *testing.B) {
				ctx := context.Background()
				be := tc.open(b)
				e := benchMergeEngine(b, be, shape.parts, shape.rows)
				src := e.parts

				b.SetBytes(partsBytes(src))
				b.ReportAllocs()

				for b.Loop() {
					out, err := e.compactParts(ctx, src, minInt64, 0)
					if err != nil {
						b.Fatal(err)
					}

					b.StopTimer()

					for _, p := range out {
						if err := deletePart(ctx, be, p.prefix); err != nil {
							b.Fatal(err)
						}
					}

					b.StartTimer()
				}
			})
		}
	}
}
