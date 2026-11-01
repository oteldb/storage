package engine

import (
	"testing"

	"github.com/oteldb/storage/encoding/chunk"
)

func BenchmarkSeriesStatsEncode(b *testing.B) {
	ids, stats := make([]chunk.U128, 100_000), make([]SeriesAgg, 100_000)
	for i := range ids {
		ids[i] = chunk.U128{Lo: uint64(i)}
		stats[i] = SeriesAgg{Count: float64(i%256 + 1), Sum: float64(i), Min: 0, Max: float64(i)}
	}
	b.SetBytes(int64(len(encodeSeriesStats(ids, stats))))
	b.ReportAllocs()
	for b.Loop() {
		_ = encodeSeriesStats(ids, stats)
	}
}
