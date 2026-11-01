package engine

import (
	"fmt"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
)

func BenchmarkPlannedIndexSelection(b *testing.B) {
	const count = 65536
	for _, selected := range []int{4, 32, 512, count} {
		for _, clustered := range []bool{false, true} {
			if selected == count && clustered {
				continue
			}
			shape := "uniform"
			if clustered {
				shape = "clustered"
			}
			for _, cacheBytes := range []int64{64 << 10, 2 << 20} {
				cache := "64KiB-os-warm"
				if cacheBytes == 2<<20 {
					cache = "2MiB-prewarmed"
				}
				b.Run(fmt.Sprintf("selected=%d/%s/%s", selected, shape, cache), func(b *testing.B) {
					benchmarkPagedIndexSelection(b, count, selected, clustered, cacheBytes)
				})
			}
		}
	}
}

type pagedIndexProbe struct {
	id   signal.SeriesID
	want rowRange
}

func benchmarkPagedIndexSelection(b *testing.B, count, selected int, clustered bool, cacheBytes int64) {
	b.Helper()
	const rowsPerSeries = 3
	column := makeSortedSeriesColumn(count, rowsPerSeries)
	files, err := file.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	if err := files.Write(b.Context(), sidxKey("p"), encodeSeriesIndex(column)); err != nil {
		b.Fatal(err)
	}
	counter := backendtest.NewByteCounter(files)
	cached := backend.Cached(counter, cacheBytes)
	index, ok := openPagedIndex(b.Context(), cached, "p", len(column), obs.NewNop().Corruption)
	if !ok || len(index.fences) == 0 || index.view.Load() != nil {
		b.Fatal("fixture must use leaf pages without retaining the full index")
	}
	probes := make([]pagedIndexProbe, selected)
	plan := enginePlan{ids: make([]signal.SeriesID, selected)}
	pt := &part{index: partIndex{paged: index}}
	for i := range probes {
		position := i * count / selected
		if clustered {
			position = count/3 + i
		}
		row := position * rowsPerSeries
		probes[i] = pagedIndexProbe{
			id: signal.SeriesID(column[row]), want: rowRange{start: row, end: row + rowsPerSeries},
		}
		plan.ids[i] = probes[i].id
	}
	lookup := func() {
		ranges, positions, err := plan.matchingRanges(b.Context(), pt, true)
		if err != nil || len(ranges) != len(probes) || len(positions) != len(probes) {
			b.Fatalf("matching ranges=%d, positions=%d, error=%v", len(ranges), len(positions), err)
		}
		for i, probe := range probes {
			if ranges[i] != probe.want || positions[i] != uint32(i) {
				b.Fatalf("lookup=%v at %d, want %v at %d", ranges[i], positions[i], probe.want, i)
			}
		}
	}
	// Opening validated the file, warming the OS cache without populating the leaf cache.
	// The small cache starts empty; 1x measures its first pass, longer runs include reuse/eviction.
	if cacheBytes == 2<<20 {
		lookup()
	}
	counter.Reset()
	b.ReportAllocs()
	for b.Loop() {
		lookup()
	}
	b.ReportMetric(float64(counter.Bytes())/float64(b.N), "index-read-B/op")
	b.ReportMetric(float64(counter.Reads())/float64(b.N), "index-reads/op")
	b.ReportMetric(float64(len(probes)), "lookups/op")
}
