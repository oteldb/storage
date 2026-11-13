package engine

import (
	"testing"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
)

// BenchmarkKnownSeriesHeadAppend compares first-buffer admission against the same immutable seed.
func BenchmarkKnownSeriesHeadAppend(b *testing.B) {
	for _, compact := range []bool{false, true} {
		name := "registry"
		if compact {
			name = "known"
		}
		b.Run(name, func(b *testing.B) {
			be, err := backendfile.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			cfg := Config{Backend: be, Prefix: "known-bench", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true, KnownSeriesHead: compact}
			const n = 1024
			ids := make([]signal.SeriesID, n)
			ts := make([]int64, n)
			values := make([]float64, n)
			for i := range n {
				ids[i] = benchSeries(i).Hash()
				ts[i] = 10
				values[i] = 1
			}
			seed := New(cfg)
			if _, err := seed.AppendBatch(ids, ts, values, nil, benchSeries, AppendLimits{}); err != nil {
				b.Fatal(err)
			}
			if err := seed.Flush(b.Context()); err != nil {
				b.Fatal(err)
			}
			if err := seed.CloseWAL(); err != nil {
				b.Fatal(err)
			}
			for i := range ts {
				ts[i] = 100
			}
			b.ReportAllocs()
			b.SetBytes(n * SampleBytes)
			for b.Loop() {
				b.StopTimer()
				e := New(cfg)
				if err := e.LoadParts(b.Context()); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				result, err := e.AppendBatch(ids, ts, values, nil, benchSeries, AppendLimits{})
				if err != nil || result.Accepted != n {
					b.Fatalf("append: %+v, %v", result, err)
				}
				b.StopTimer()
				if err := e.CloseWAL(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}
