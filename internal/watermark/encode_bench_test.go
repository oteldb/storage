package watermark_test

import (
	"testing"

	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

func BenchmarkWatermarkEncode(b *testing.B) {
	entries := make([]watermark.Entry, 100_000)
	for i := range entries {
		entries[i] = watermark.Entry{ID: signal.SeriesID{Lo: uint64(i)}, Max: int64(i)}
	}
	b.SetBytes(int64(len(watermark.Encode(nil, entries))))
	b.ReportAllocs()
	for b.Loop() {
		_ = watermark.Encode(nil, entries)
	}
}
