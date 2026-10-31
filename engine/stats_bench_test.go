package engine

import (
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkStatsHeadSamples(b *testing.B) {
	for _, count := range []int{1024, 65536} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			e := New(Config{})
			for i := range count {
				s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
					Key: []byte("id"), Value: signal.IntValue(int64(i)),
				})}
				if _, err := e.Append(s, 1, 1); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			var stats Stats
			for b.Loop() {
				stats = e.Stats()
			}
			if stats.HeadSamples != int64(count) {
				b.Fatalf("head samples=%d, want%d", stats.HeadSamples, count)
			}
		})
	}
}
