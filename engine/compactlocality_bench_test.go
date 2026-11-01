package engine

import (
	"testing"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/block"
)

func BenchmarkSeriesLocalCompaction(b *testing.B) {
	for _, mode := range []string{"size", "local"} {
		b.Run(mode, func(b *testing.B) {
			files, err := file.New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			e := New(Config{Backend: files, Prefix: "metrics", AggregateStats: true, MergeMemoryBytes: 512 << 20})
			populateLocalityParts(b, e, 4096)
			selectRun := func() []*part {
				if mode == "local" {
					return pickMergeRun(e.parts, 0, 0)
				}
				bySize, order := eligibleBySize(e.parts, 0)
				selected, _, _ := scanRuns(bySize, 0)
				return inSrcOrder(selected, order)
			}
			selected := selectRun()
			var inputRows int64
			for _, p := range selected {
				inputRows += int64(p.rows())
			}
			if len(selected) != maxMergeParts || inputRows != 4096*maxMergeParts {
				b.Fatal("fixture must compare equal input counts and logical bytes")
			}
			b.SetBytes(inputRows * partRowBytes)
			b.ReportAllocs()
			var bytes, series int64
			for b.Loop() {
				selected = selectRun()
				parts, err := e.compactStream(b.Context(), selected, minInt64, 0,
					rollupPlan{marker: &block.Rollup{}}, MergeOptions{})
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				for _, p := range parts {
					sizes, err := block.PartObjectSizes(b.Context(), files, p.prefix)
					if err != nil {
						b.Fatal(err)
					}
					bytes += sizes.Total
					series += int64(p.index.seriesCount())
					if err := deletePart(b.Context(), files, p.prefix); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(bytes)/float64(b.N), "output-B/op")
			b.ReportMetric(float64(series)/float64(b.N), "output-series/op")
		})
	}
}
