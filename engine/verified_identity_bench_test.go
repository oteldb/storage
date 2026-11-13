package engine

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/seriestable"
)

// BenchmarkVerifiedIdentities measures canonical validation, excluding input and root generation.
func BenchmarkVerifiedIdentities(b *testing.B) {
	const count = 8192
	input := identityCacheInput(count)
	for _, roots := range []int{1, 32} {
		for _, limit := range []int64{0, 64 << 10, 8 << 20} {
			b.Run(fmt.Sprintf("roots=%d/budget=%d", roots, limit), func(b *testing.B) {
				files, err := backendfile.New(b.TempDir())
				if err != nil {
					b.Fatal(err)
				}
				counter := &cpuFixtureFile{File: files}
				be := backend.Cached(counter, 64<<20)
				ids := seriestable.NewIDCache(8 << 20)
				var handles []*seriesRootHandle
				for i := range roots {
					var builder seriestable.Builder
					for _, ref := range input.refs[i*count/roots : (i+1)*count/roots] {
						if err := builder.AddView(input.view, int(ref)); err != nil {
							b.Fatal(err)
						}
					}
					prefix := fmt.Sprintf("root-%03d", i)
					run, err := seriestable.WriteWithIDCache(b.Context(), be, prefix, &builder, ids)
					if err != nil {
						b.Fatal(err)
					}
					handles = append(handles, &seriesRootHandle{binding: bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: run.Digest()}, run: run})
				}
				snapshot := newSeriesSnapshot(handles)
				b.Cleanup(snapshot.release)
				var cache *verifiedIdentityCache
				if limit > 0 {
					cache = newVerifiedIdentityCache(limit)
				}
				if _, err := snapshot.knownIdentities(b.Context(), input, cache); err != nil {
					b.Fatal(err)
				}
				counter.Reset()
				b.SetBytes(count * 16)
				b.ReportAllocs()
				for b.Loop() {
					known, err := snapshot.knownIdentities(b.Context(), input, cache)
					if err != nil {
						b.Fatal(err)
					}
					for _, found := range known {
						if !found {
							b.Fatal("missing identity")
						}
					}
				}
				b.ReportMetric(float64(counter.Bytes())/float64(b.N), "backend-read-B/op")
				b.ReportMetric(float64(cache.sizeBytes()), "verified-B")
				b.ReportMetric(float64(count)*float64(b.N)/b.Elapsed().Seconds(), "series/s")
			})
		}
	}
}
