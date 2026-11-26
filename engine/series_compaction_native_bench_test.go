package engine

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/signal"
)

// descriptorBenchRuns writes independently sorted, overlapping descriptor runs to native files.
func descriptorBenchRuns(b *testing.B, runs, perRun, overlap int) (*Engine, *cpuFixtureFile) {
	b.Helper()
	files, err := backendfile.New(b.TempDir())
	require.NoError(b, err)
	counter := &cpuFixtureFile{File: files}
	e := separateCatalogEngine(counter)
	b.Cleanup(func() { require.NoError(b, e.CloseWAL()) })
	roots := make([]bucketindex.SeriesRoot, 0, runs)
	for r := range runs {
		series := make([]signal.Series, perRun)
		for i := range series {
			series[i] = benchSeries(r*(perRun-overlap) + i)
		}
		slices.SortFunc(series, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
		var builder seriestable.Builder
		for i := range series {
			s := &series[i]
			require.NoError(b, builder.Add(s.Hash(), *s))
		}
		prefix := fmt.Sprintf("%s/s-%02d", e.cfg.Prefix, r)
		run, err := seriestable.Write(b.Context(), files, prefix, &builder)
		require.NoError(b, err)
		roots = append(roots, bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: run.Digest()})
		require.NoError(b, run.Close())
	}
	require.NoError(b, e.installSeriesRootsLocked(b.Context(), roots))
	return e, counter
}

func BenchmarkDescriptorRunMerge(b *testing.B) {
	const runs, perRun = 4, 512
	for _, overlap := range []int{0, perRun / 2} {
		b.Run(fmt.Sprintf("overlap=%d", overlap), func(b *testing.B) {
			e, counter := descriptorBenchRuns(b, runs, perRun, overlap)
			counter.Reset()
			want := runs*perRun - (runs-1)*overlap
			b.ReportAllocs()
			for b.Loop() {
				out, err := e.buildSeriesMerge(b.Context(), e.seriesTable.roots, 128<<20)
				b.StopTimer()
				require.NoError(b, err)
				got := 0
				for _, root := range out {
					got += int(root.run.Count())
				}
				e.discardSeriesOutputs(b.Context(), out)
				require.Equal(b, want, got)
				b.StartTimer()
			}
			b.ReportMetric(float64(counter.Bytes())/float64(b.N), "backend-read-B/op")
			b.ReportMetric(runs*perRun, "input-rows/op")
			b.ReportMetric(float64(want), "distinct/op")
		})
	}
}

func descriptorUnproductiveFixture(b *testing.B) (*Engine, *cpuFixtureFile) {
	b.Helper()
	e, counter := descriptorBenchRuns(b, 2, 32768, 0)
	e.cfg.MergeMemoryBytes = 1 << 30
	_, scratch := boundedSeriesSources(e.seriesTable.roots, e.mergeMemoryBudgetBytes())
	e.cfg.MergeMemoryBytes = 2*scratch + 2
	sources, _ := boundedSeriesSources(e.seriesTable.roots, e.mergeMemoryBudgetBytes())
	require.Len(b, sources, 2)
	return e, counter
}

func BenchmarkDescriptorUnproductiveRetry(b *testing.B) {
	e, counter := descriptorUnproductiveFixture(b)
	changed, err := e.compactSeriesRuns(b.Context(), false)
	require.NoError(b, err)
	require.False(b, changed, "fixture must exhaust its output allowance before combining both roots")
	counter.Reset()
	b.ReportAllocs()
	for b.Loop() {
		changed, err := e.compactSeriesRuns(b.Context(), false)
		if err != nil || changed {
			b.Fatalf("retry changed=%t err=%v", changed, err)
		}
	}
	b.ReportMetric(float64(counter.Bytes())/float64(b.N), "backend-read-B/op")
}

// BenchmarkDescriptorFirstAttempt isolates the cost of discovering a genuinely unproductive group.
func BenchmarkDescriptorFirstAttempt(b *testing.B) {
	source, counter := descriptorUnproductiveFixture(b)
	b.ReportAllocs()
	counter.Reset()
	for b.Loop() {
		b.StopTimer()
		trial := separateCatalogEngine(counter)
		trial.cfg.MergeMemoryBytes = source.cfg.MergeMemoryBytes
		source.seriesTable.acquire()
		trial.seriesTable.release()
		trial.seriesTable = source.seriesTable
		b.StartTimer()
		changed, err := trial.compactSeriesRuns(b.Context(), false)
		b.StopTimer()
		require.NoError(b, trial.CloseWAL())
		require.NoError(b, err)
		require.False(b, changed)
		b.StartTimer()
	}
	b.ReportMetric(float64(counter.Bytes())/float64(b.N), "backend-read-B/op")
}
