package block

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func BenchmarkSparseFrameRead(b *testing.B) {
	const (
		blockRows = 1024
		selected  = 32
	)
	values, codec := sparseFrameValues(b)
	rows := len(values)
	if rows < blockRows*selected {
		b.Fatal("fixture needs at least 32 granules")
	}
	for _, frameBytes := range []int{8 << 10, 16 << 10, 64 << 10} {
		b.Run(strconv.Itoa(frameBytes), func(b *testing.B) {
			files, err := file.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			counter := backendtest.NewSizedByteCounter(files)
			w := NewPartWriter(WithGranuleSize(blockRows), WithCompressBlockBytes(frameBytes),
				WithCompression(compress.AlgorithmZSTD), WithSizingStats())
			if err := w.AddColumn(Column{Name: "ts", Kind: KindInt64, Codec: codec, Int64: values, Block: true}); err != nil {
				b.Fatal(err)
			}
			if err := WritePart(b.Context(), counter, "p", w); err != nil {
				b.Fatal(err)
			}
			r, err := OpenPart(b.Context(), counter, "p", NewReadCompressors())
			if err != nil {
				b.Fatal(err)
			}
			desc, _ := r.ColumnDescByName("ts")
			counter.Reset()
			var buf []int64
			var readBytes int64
			b.SetBytes(selected * blockRows * 8)
			b.ReportAllocs()
			for b.Loop() {
				decoder, err := r.ColumnBlocks(b.Context(), "ts")
				if err != nil {
					b.Fatal(err)
				}
				for i := range selected {
					block := i * (rows / blockRows / selected)
					buf, err = decoder.DecodeInt64Into(block, buf[:0])
					if err != nil || len(buf) != blockRows || buf[0] != values[block*blockRows] {
						b.Fatalf("decode block %d: %d rows, %v", block, len(buf), err)
					}
				}
				readBytes += counter.Bytes()
				counter.Reset()
			}
			b.ReportMetric(float64(readBytes)/float64(b.N), "read-B/op")
			b.ReportMetric(float64(desc.Bytes)/float64(rows), "stored-B/row")
		})
	}
}

func sparseFrameValues(b *testing.B) ([]int64, chunk.Codec) {
	b.Helper()
	if dir := os.Getenv("OTELDB_BENCH_FRAME_PART"); dir != "" {
		files, err := file.New(filepath.Dir(dir))
		if err != nil {
			b.Fatal(err)
		}
		r, err := OpenPart(b.Context(), files, filepath.Base(dir), NewReadCompressors())
		if err != nil {
			b.Fatal(err)
		}
		column, err := r.Column(b.Context(), "ts")
		if err != nil {
			b.Fatal(err)
		}
		values, err := column.Int64(nil)
		if err != nil {
			b.Fatal(err)
		}
		desc, _ := r.ColumnDescByName("ts")
		return values[:min(len(values), 512<<10)/1024*1024], desc.Codec
	}
	values := make([]int64, 512<<10)
	for i := range values {
		values[i] = 1700000000000000000 + int64(i%2)*15000000000 + int64((i/2)%3)*1000000000
	}
	return values, chunk.CodecDoDScaled
}
