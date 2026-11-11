package engine

import (
	"fmt"
	"slices"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/signal"
)

func BenchmarkSharedIdentityMisses(b *testing.B) {
	for _, roots := range []int{1, 16, 64} {
		for _, retain := range []bool{false, true} {
			b.Run(fmt.Sprintf("roots=%d/retained=%t", roots, retain), func(b *testing.B) {
				benchmarkSharedIdentityMisses(b, roots, retain)
			})
		}
	}
}

func benchmarkSharedIdentityMisses(b *testing.B, roots int, retain bool) {
	b.Helper()
	files, err := backendfile.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	counter := &cpuFixtureFile{File: files}
	be := backend.Cached(counter, 64<<20)
	var cache *seriestable.IDCache
	if retain {
		cache = seriestable.NewIDCache(8 << 20)
	}
	handles := make([]*seriesRootHandle, 0, roots)
	const perRun = 2048
	for run := range roots {
		descriptors := make([]signal.Series, perRun)
		for i := range descriptors {
			descriptors[i] = benchSeries(run*perRun + i)
		}
		slices.SortFunc(descriptors, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
		var builder seriestable.Builder
		for i := range descriptors {
			s := &descriptors[i]
			if err := builder.Add(s.Hash(), *s); err != nil {
				b.Fatal(err)
			}
		}
		prefix := fmt.Sprintf("run-%d", run)
		r, err := seriestable.WriteWithIDCache(b.Context(), be, prefix, &builder, cache)
		if err != nil {
			b.Fatal(err)
		}
		handles = append(handles, &seriesRootHandle{binding: bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: r.Digest()}, run: r})
	}
	snapshot := newSeriesSnapshot(handles)
	b.Cleanup(snapshot.release)
	registry := series.New()
	const queries = 4096
	for i := range queries {
		registry.Add(benchSeries(roots*perRun + i))
	}
	refs := make([]uint32, queries)
	for i := range refs {
		refs[i] = uint32(i)
	}
	view := registry.View()
	slices.SortFunc(refs, func(a, b uint32) int { return view.ID(int(a)).Compare(view.ID(int(b))) })
	input := identitySnapshot{view: view, refs: refs}
	counter.Reset()
	b.ReportAllocs()
	for b.Loop() {
		known, err := snapshot.knownIdentities(b.Context(), input)
		if err != nil {
			b.Fatal(err)
		}
		for _, found := range known {
			if found {
				b.Fatal("unexpected retained identity")
			}
		}
	}
	b.ReportMetric(float64(counter.Bytes())/float64(b.N), "backend-read-B/op")
	b.ReportMetric(float64(cache.SizeBytes()), "retained-B")
	b.ReportMetric(float64(roots*queries), "probes/op")
}
