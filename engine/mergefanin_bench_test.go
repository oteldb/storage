package engine

import (
	"os"
	"testing"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
)

// BenchmarkMergeFanIn measures one maintenance turn; fixture generation and cleanup are excluded.
func BenchmarkMergeFanIn(b *testing.B) {
	const series, parts = 512, 32
	identities := make([]signal.Series, series)
	ids := make([]signal.SeriesID, series)
	ts := make([]int64, series)
	values := make([]float64, series)
	for i := range identities {
		identities[i] = benchSeries(i)
		ids[i] = identities[i].Hash()
		values[i] = float64(i)
	}
	var retired int
	b.SetBytes(parts * series * 16)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		dir := b.TempDir()
		files, err := backendfile.New(dir)
		if err != nil {
			b.Fatal(err)
		}
		e := New(Config{Backend: files, Prefix: "metrics", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true,
			MergeMemoryBytes: 128 << 20, FlushMemoryBytes: 32 << 20, SeriesCacheBytes: 8 << 20})
		for p := range parts {
			for i := range ts {
				ts[i] = int64(p + 100)
			}
			if _, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return identities[i] }, AppendLimits{}); err != nil {
				b.Fatal(err)
			}
			if err := e.Flush(b.Context()); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		err = e.MergeWith(b.Context(), MergeOptions{Force: true})
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		retired += parts - e.PartCount()
		if err := e.CloseWAL(); err != nil {
			b.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(retired)/float64(b.N), "net-parts/turn")
	b.ReportMetric(float64(retired)/b.Elapsed().Seconds(), "net-parts/s")
}
