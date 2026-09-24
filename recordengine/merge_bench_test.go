package recordengine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
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
func benchMergeEngine(tb testing.TB, be backend.Backend, parts, rows int) *Engine {
	tb.Helper()

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
				tb.Fatal(err)
			}
		}

		if err := e.Flush(ctx); err != nil {
			tb.Fatal(err)
		}
	}

	return e
}

// benchTraceSchema is trace-shaped: several int columns, a raw id, and dictionary columns that repeat
// enough to be written on a shared dictionary — every column a merge can read by granule.
var benchTraceSchema = NewSchema(
	Column{Name: "duration", Kind: KindInt64, Codec: chunk.CodecT64},
	Column{Name: "parent", Kind: KindInt64, Codec: chunk.CodecT64},
	Column{Name: "left", Kind: KindInt64, Codec: chunk.CodecT64},
	Column{Name: "right", Kind: KindInt64, Codec: chunk.CodecT64},
	Column{Name: "trace_id", Kind: KindBytes, Codec: chunk.CodecBytesRaw},
	Column{Name: "name", Kind: KindBytes, Codec: chunk.CodecDict},
	Column{Name: "attrs", Kind: KindBytes, Codec: chunk.CodecDict},
)

// benchTraceEngine flushes the given number of trace-shaped parts of rows records each over be, 1024
// per stream. Streams stay the same length as the parts grow, because a merge accumulates a whole
// stream before it seals: a corpus of few long streams measures the stream, not the read side.
func benchTraceEngine(tb testing.TB, be backend.Backend, parts, rows int) *Engine {
	tb.Helper()

	ctx := context.Background()
	e := New(Config{Schema: benchTraceSchema, Backend: be, Prefix: "bench/traces", MergeMemoryBytes: -1})
	r := rand.New(rand.NewPCG(3, 5))

	const perStream = 1024

	for p := range parts {
		for s := range rows / perStream {
			series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
				signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc-" + strconv.Itoa(s)))},
			)}}

			batch := &Batch{
				Stream: series.Hash(), Identity: func() signal.Series { return series },
				Ints: make([][]int64, 4), Bytes: make([][][]byte, 3),
			}

			for i := range perStream {
				batch.Ts = append(batch.Ts, int64(p*perStream+i)*1_000_000+r.Int64N(1000))

				for k := range batch.Ints {
					batch.Ints[k] = append(batch.Ints[k], r.Int64N(1<<40))
				}

				var id [16]byte
				for j := range id {
					id[j] = byte(r.Uint32())
				}

				batch.Bytes[0] = append(batch.Bytes[0], id[:])
				batch.Bytes[1] = append(batch.Bytes[1], fmt.Appendf(nil, "GET /api/v1/items/{id} op=%d", r.IntN(40)))
				batch.Bytes[2] = append(batch.Bytes[2], signal.NewAttributes(
					signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte("node-" + strconv.Itoa(r.IntN(12))))},
				).AppendHashInput(nil))
			}

			if _, err := e.AppendBatch(batch, AppendLimits{}); err != nil {
				tb.Fatal(err)
			}
		}

		if err := e.Flush(ctx); err != nil {
			tb.Fatal(err)
		}
	}

	return e
}

// BenchmarkMergeCompactTraces is [BenchmarkMergeCompact] over trace-shaped parts, whose columns a
// merge reads granule by granule rather than whole.
func BenchmarkMergeCompactTraces(b *testing.B) {
	for _, rows := range []int{64 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("file/parts=4/rows=%d", rows), func(b *testing.B) {
			be, err := file.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}

			benchCompact(b, benchTraceEngine(b, be, 4, rows), be)
		})
	}
}

// BenchmarkMergeCompact times one record merge of every flushed part into one output part: reading
// the sources, the stream sweep, and writing the output with its blooms and sidecars. Throughput is
// the sources' decoded bytes ([partsBytes]), not their on-disk size, so its MB/s does not compare
// with the metric engine's merge benchmarks.
func BenchmarkMergeCompact(b *testing.B) {
	for _, tc := range []backendtest.Case{backendtest.Memory(), backendtest.Dir("file", file.New)} {
		for _, shape := range []struct{ parts, rows int }{{4, 32 << 10}, {8, 64 << 10}} {
			b.Run(fmt.Sprintf("%s/parts=%d/rows=%d", tc.Name, shape.parts, shape.rows), func(b *testing.B) {
				be := tc.Open(b)
				benchCompact(b, benchMergeEngine(b, be, shape.parts, shape.rows), be)
			})
		}
	}
}

// benchCompact times merging every part of e into one, deleting the output between rounds so each
// round starts from the same sources. Throughput is [partsBytes] of the sources.
func benchCompact(b *testing.B, e *Engine, be backend.Backend) {
	b.Helper()

	ctx := context.Background()
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
}
