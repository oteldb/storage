package block

import (
	"context"
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// Retain the same cached backend while declining its optional page capability.
type uncachedFramePages struct{ backend.Backend }

func (b uncachedFramePages) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return backend.ReadAt(ctx, b.Backend, key, off, n)
}

func (b uncachedFramePages) ReadViewAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return backend.ReadViewAt(ctx, b.Backend, key, off, n)
}

func BenchmarkCachedPointFrames(b *testing.B) {
	for _, random := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			b.Run(fmt.Sprintf("random=%t/cached=%t", random, cached), func(b *testing.B) {
				benchmarkCachedPointFrames(b, random, cached)
			})
		}
	}
}

func benchmarkCachedPointFrames(b *testing.B, random, cached bool) {
	b.Helper()
	const rows, blockRows, selected = 512 << 10, 1024, 32
	files, err := file.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	values := make([]int64, rows)
	for i := range values {
		values[i] = 1700000000000000000 + int64(i%2)*15000000000 + int64((i/2)%3)*1000000000
	}
	w := NewPartWriter(WithGranuleSize(blockRows), WithCompressBlockBytes(8<<10), WithCompression(compress.AlgorithmZSTD))
	if err := w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoDScaled, Int64: values, Block: true}); err != nil {
		b.Fatal(err)
	}
	if err := WritePart(b.Context(), files, "frames", w); err != nil {
		b.Fatal(err)
	}
	counter := backendtest.NewSizedByteCounter(files)
	read := backend.Cached(counter, 8<<20)
	if !cached {
		read = uncachedFramePages{Backend: read}
	}
	r, err := OpenPart(b.Context(), read, "frames", NewReadCompressors())
	if err != nil {
		b.Fatal(err)
	}
	metadata, err := r.ColumnBlocks(b.Context(), "ts")
	if err != nil {
		b.Fatal(err)
	}
	var buf []int64
	step, iteration := rows/blockRows/selected, 0
	decode := func(phase int) {
		d := metadata.Clone(b.Context())
		for i := range selected {
			block := i*step + phase
			buf, err = d.DecodeInt64Into(block, buf[:0])
			if err != nil || len(buf) != blockRows || buf[0] != values[block*blockRows] {
				b.Fatalf("block %d: rows=%d err=%v", block, len(buf), err)
			}
		}
	}
	for phase := range step {
		decode(phase)
	}
	counter.Reset()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		phase := 0
		if random {
			phase = iteration % step
		}
		decode(phase)
		iteration++
	}
	b.ReportMetric(float64(counter.Bytes())/float64(b.N), "frame-read-B/op")
}
