package engine

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
)

func BenchmarkDecoderMetadataOpen(b *testing.B) {
	files, err := file.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	counter := backendtest.NewByteCounter(files)
	e := New(Config{Backend: counter, Prefix: "meta-bench", DecodeCacheBytes: 8 << 20})
	series := apiSeries()
	for i := range 256 << 10 {
		if _, err := e.Append(series, int64(i)*15000000, float64(i)); err != nil {
			b.Fatal(err)
		}
	}
	if err := e.Flush(b.Context()); err != nil {
		b.Fatal(err)
	}
	p := e.parts[0]
	for _, cached := range []bool{false, true} {
		b.Run("cached="+strconv.FormatBool(cached), func(b *testing.B) {
			counter.Reset()
			b.ReportAllocs()
			for b.Loop() {
				if !cached {
					d, err := p.reader.ColumnBlocks(b.Context(), colTs)
					if err != nil || d.NumBlocks() == 0 {
						b.Fatalf("open directory: %v", err)
					}
					continue
				}
				d, pin, err := e.partBlockDecoder(b.Context(), p, colTs, colTsID)
				if err != nil || d.NumBlocks() == 0 {
					b.Fatalf("open cached directory: %v", err)
				}
				e.blockCache.release(pin)
			}
			b.ReportMetric(float64(counter.Bytes())/float64(b.N), "directory-read-B/op")
		})
	}
}
