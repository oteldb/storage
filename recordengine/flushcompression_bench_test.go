package recordengine

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type flushCompressionCorpus struct {
	name      string
	rows      int
	streams   int
	exemplars bool
	unique    bool
}

func flushCompressionCorpora() []flushCompressionCorpus {
	return []flushCompressionCorpus{
		{name: "logs_repeated", rows: 16384, streams: 16},
		{name: "logs_unique", rows: 16384, streams: 16, unique: true},
		{name: "exemplars_360", rows: 360, streams: 46, exemplars: true},
		{name: "exemplars_16k", rows: 16384, streams: 46, exemplars: true},
	}
}

func newFlushCompressionCorpus(tb testing.TB, c flushCompressionCorpus) (*Engine, *flushColumns, identitySet) {
	tb.Helper()

	schema := benchLogSchema
	if c.exemplars {
		schema = NewSchema(
			Column{Name: "value", Kind: KindInt64, Codec: chunk.CodecT64},
			Column{Name: "trace_id", Kind: KindBytes, Codec: chunk.CodecBytesRaw, Bloom: BloomEquality},
			Column{Name: "span_id", Kind: KindBytes, Codec: chunk.CodecBytesRaw},
			Column{Name: "attrs", Kind: KindBytes, Codec: chunk.CodecDict, Bloom: BloomAttrs},
		)
	}

	e := New(Config{Schema: schema, Backend: backend.Memory(), Prefix: "bench/flush"})
	r := rand.New(rand.NewPCG(1, 2))

	for s := range c.streams {
		series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue(fmt.Appendf(nil, "service-%d", s))},
			signal.KeyValue{Key: []byte("host.name"), Value: signal.StringValue([]byte("node-1"))},
		)}}
		batch := &Batch{
			Stream: series.Hash(), Identity: func() signal.Series { return series },
			Ints: make([][]int64, schema.numInts()), Bytes: make([][][]byte, schema.numBytes()),
		}
		for i := s; i < c.rows; i += c.streams {
			ts := int64(1_790_000_000_000_000_000+i*1_000_000) + r.Int64N(1000)
			batch.Ts = append(batch.Ts, ts)
			if c.exemplars {
				batch.Ints[0] = append(batch.Ints[0], int64(math.Float64bits(r.Float64()*400)))
				traceID, spanID := make([]byte, 16), make([]byte, 8)
				for k := range traceID {
					traceID[k] = byte(r.Uint32())
				}
				for k := range spanID {
					spanID[k] = byte(r.Uint32())
				}
				batch.Bytes[0] = append(batch.Bytes[0], traceID)
				batch.Bytes[1] = append(batch.Bytes[1], spanID)
				batch.Bytes[2] = append(batch.Bytes[2], nil)
				continue
			}

			batch.Ints[0] = append(batch.Ints[0], ts+30_000)
			batch.Ints[1] = append(batch.Ints[1], 9)
			batch.Bytes[0] = append(batch.Bytes[0], []byte("INFO"))
			requestID := fmt.Appendf(nil, "%016x%016x", r.Uint64(), r.Uint64())
			bodyID := fmt.Appendf(nil, "%d", i%128)
			if c.unique {
				bodyID = requestID
			}
			batch.Bytes[1] = append(batch.Bytes[1], fmt.Appendf(nil,
				"handled request method=GET path=/api/v1/items/%d status=200 request_id=%s upstream=backend.internal namespace=production duration_ms=%d",
				i%128, bodyID, i%100))
			batch.Bytes[2] = append(batch.Bytes[2], requestID)
			batch.Bytes[3] = append(batch.Bytes[3], signal.NewAttributes(
				signal.KeyValue{Key: []byte("http.method"), Value: signal.StringValue([]byte("GET"))},
				signal.KeyValue{Key: []byte("user"), Value: signal.StringValue(fmt.Appendf(nil, "user-%d", i%3000))},
			).AppendHashInput(nil))
		}
		if _, err := e.AppendBatch(batch, AppendLimits{}); err != nil {
			tb.Fatal(err)
		}
	}

	columns := buildFlushColumns(schema, e.head.records, nil)
	idents := e.identitiesOf(e.head.records)
	_, _ = e.head.detach()
	return e, columns, idents
}

func sealFlushCompressionPart(tb testing.TB, e *Engine, columns *flushColumns, idents identitySet, algorithm compress.Algorithm) *part {
	tb.Helper()

	ctx := context.Background()
	prefix := e.cfg.Prefix + "/part"
	level := compress.LevelDefault
	if algorithm != compress.AlgorithmNone {
		level = compress.LevelFast
	}
	if err := writePart(ctx, e.cfg.Backend, e.cfg.Schema, prefix, columns, idents, algorithm, level, e.blooms()); err != nil {
		tb.Fatal(err)
	}
	p, err := openPart(ctx, e.cfg.Backend, e.cfg.Schema, prefix, e.cfg.Obs.Corruption, e.readCompressors)
	if err != nil {
		tb.Fatal(err)
	}
	return p
}

func flushCompressionStoredBytes(tb testing.TB, e *Engine) int64 {
	tb.Helper()
	ctx := context.Background()
	keys, err := e.cfg.Backend.List(ctx, e.cfg.Prefix+"/part/")
	if err != nil {
		tb.Fatal(err)
	}
	var stored int64
	for _, key := range keys {
		data, err := e.cfg.Backend.Read(ctx, key)
		if err != nil {
			tb.Fatal(err)
		}
		stored += int64(len(data))
	}
	return stored
}

// BenchmarkFlushCompression isolates part encoding, sidecars and read-back from head preparation,
// WAL and index commit. The memory backend exposes CPU/alloc costs without disk latency.
func BenchmarkFlushCompression(b *testing.B) {
	for _, c := range flushCompressionCorpora() {
		for _, algorithm := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD} {
			b.Run(fmt.Sprintf("%s/compression=%s", c.name, algorithm), func(b *testing.B) {
				e, columns, idents := newFlushCompressionCorpus(b, c)
				sealFlushCompressionPart(b, e, columns, idents, algorithm)
				stored := flushCompressionStoredBytes(b, e)
				b.ReportAllocs()
				b.SetBytes(columns.byteSize())
				for b.Loop() {
					sealFlushCompressionPart(b, e, columns, idents, algorithm)
				}
				b.ReportMetric(float64(stored)/float64(c.rows), "stored-B/item")
			})
		}
	}
}

func BenchmarkFlushCompressionFetch(b *testing.B) {
	for _, c := range flushCompressionCorpora() {
		for _, algorithm := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD} {
			b.Run(fmt.Sprintf("%s/compression=%s", c.name, algorithm), func(b *testing.B) {
				e, columns, idents := newFlushCompressionCorpus(b, c)
				e.parts = []*part{sealFlushCompressionPart(b, e, columns, idents, algorithm)}
				request := fetch.Request{Start: 0, End: math.MaxInt64, Recycle: true}
				ctx := context.Background()
				b.ReportAllocs()
				b.SetBytes(columns.byteSize())
				for b.Loop() {
					it, err := e.Fetch(ctx, request)
					if err != nil {
						b.Fatal(err)
					}
					rows := 0
					for {
						batch, err := it.Next(ctx)
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							b.Fatal(err)
						}
						rows += len(batch.Timestamps)
						batch.Release()
					}
					if err := it.Close(); err != nil {
						b.Fatal(err)
					}
					if rows != c.rows {
						b.Fatalf("got %d rows, want %d", rows, c.rows)
					}
				}
			})
		}
	}
}

func TestFlushCompressionRoundTrip(t *testing.T) {
	t.Parallel()
	for _, c := range flushCompressionCorpora() {
		for _, algorithm := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD} {
			t.Run(fmt.Sprintf("%s/compression=%s", c.name, algorithm), func(t *testing.T) {
				t.Parallel()
				e, columns, idents := newFlushCompressionCorpus(t, c)
				p := sealFlushCompressionPart(t, e, columns, idents, algorithm)
				decoded, err := p.readForMerge(context.Background())
				require.NoError(t, err)
				assert.Equal(t, columns.cols.ts, decoded.ts)
				assert.Equal(t, columns.cols.ints, decoded.ints)
				for k, col := range decoded.bytes {
					for row := range c.rows {
						if !assert.Equal(t, columns.cols.bytes[k].at(row), col.col.At(row)) {
							return
						}
					}
				}
			})
		}
	}
}
