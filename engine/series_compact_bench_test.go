package engine

import (
	"fmt"
	"slices"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/signal"
)

func BenchmarkSeriesRunSeek(b *testing.B) {
	for _, cacheBytes := range []int64{0, 64 << 10} {
		for _, compacted := range []bool{false, true} {
			b.Run(fmt.Sprintf("idcache=%d/compacted=%t", cacheBytes, compacted), func(b *testing.B) {
				files, err := backendfile.New(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				be := backend.Cached(files, 64<<20)
				e := New(Config{Backend: be, Prefix: "series-seek", DiskSeries: true, MetricSeriesChunks: true, SeparateSeries: true, MergeMemoryBytes: 256 << 20})
				e.seriesIDs = seriestable.NewIDCache(cacheBytes)
				b.Cleanup(func() {
					if err := e.CloseWAL(); err != nil {
						b.Error(err)
					}
				})
				const runs, perRun = 64, 256
				roots := make([]bucketindex.SeriesRoot, 0, runs)
				for r := range runs {
					identities := make([]signal.Series, perRun)
					for i := range identities {
						identities[i] = benchSeries(r*perRun + i)
					}
					slices.SortFunc(identities, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
					var builder seriestable.Builder
					for i := range identities {
						s := &identities[i]
						if err := builder.Add(s.Hash(), *s); err != nil {
							b.Fatal(err)
						}
					}
					prefix := fmt.Sprintf("series-seek/s-%03d", r)
					run, err := seriestable.Write(b.Context(), be, prefix, &builder)
					if err != nil {
						b.Fatal(err)
					}
					roots = append(roots, bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: run.Digest()})
					if err := run.Close(); err != nil {
						b.Fatal(err)
					}
				}
				e.mu.Lock()
				if err := e.installSeriesRootsLocked(b.Context(), roots); err != nil {
					b.Fatal(err)
				}
				if err := e.updateIndexLocked(b.Context()); err != nil {
					b.Fatal(err)
				}
				e.mu.Unlock()
				if compacted {
					for len(e.seriesTable.roots) > 1 {
						changed, err := e.compactSeriesRuns(b.Context(), false)
						if err != nil {
							b.Fatal(err)
						}
						if !changed {
							break
						}
					}
				}

				for _, present := range []bool{false, true} {
					b.Run(fmt.Sprintf("present=%t", present), func(b *testing.B) {
						queries := make([]signal.SeriesID, 1024)
						for i := range queries {
							index := runs*perRun + i
							if present {
								index = (i * 17) % (runs * perRun)
							}
							queries[i] = benchSeries(index).Hash()
						}
						b.ReportAllocs()
						for b.Loop() {
							for _, id := range queries {
								found, err := e.seriesTable.lookup(b.Context(), id, func([]byte) error { return nil })
								if err != nil || found != present {
									b.Fatalf("lookup: found=%t want=%t err=%v", found, present, err)
								}
							}
						}
						b.ReportMetric(float64(len(e.seriesTable.roots)), "roots")
						b.ReportMetric(float64(e.seriesIDs.SizeBytes()), "idcache_B")
					})
				}
			})
		}
	}
}
