package engine

import (
	"context"
	"slices"
	"testing"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/signal"
)

func BenchmarkPartIDBloomMetadataMiss(b *testing.B) {
	const parts, perPart, probes = 16, 4096, 256
	files, err := backendfile.New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	counter := &cpuFixtureFile{File: files}
	be := backend.Cached(counter, 64<<20)
	e := New(Config{Backend: be, Prefix: "bloom-probes", DiskSeries: true})
	b.Cleanup(func() { _ = e.Close(context.Background()) })
	known := make(map[signal.SeriesID]struct{}, parts*perPart)
	ids, ts, values := make([]signal.SeriesID, perPart), make([]int64, perPart), make([]float64, perPart)
	for generation := range parts {
		for i := range ids {
			ids[i], ts[i] = benchSeries(generation*perPart+i).Hash(), 100
			known[ids[i]] = struct{}{}
		}
		result, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return benchSeries(generation*perPart + i) }, AppendLimits{})
		if err != nil || result.Accepted != perPart {
			b.Fatalf("fixture append: accepted %d, error %v", result.Accepted, err)
		}
		if err := e.Flush(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
	if len(e.parts) != parts {
		b.Fatalf("fixture has %d parts, want %d", len(e.parts), parts)
	}
	filters := make([]*partIDBloom, len(e.parts))
	var filterBytes, falsePositives, eligibleProbes int64
	for i, p := range e.parts {
		if err := p.validateDiskSeriesIDs(b.Context()); err != nil {
			b.Fatal(err)
		}
		filters[i] = p.idBloom.Load()
		filterBytes += filters[i].sizeBytes()
	}
	probeIDs := make([]signal.SeriesID, 0, probes)
	for i := uint64(0); len(probeIDs) < probes; i++ {
		id := signal.SeriesID{Hi: mixDiskBloom(i), Lo: mixDiskBloom(i ^ 0x9e3779b97f4a7c15)}
		if _, found := known[id]; !found {
			probeIDs = append(probeIDs, id)
		}
	}
	slices.SortFunc(probeIDs, signal.SeriesID.Compare)
	for i, filter := range filters {
		for _, id := range probeIDs {
			if !e.parts[i].index.mayContainID(id) {
				continue
			}
			eligibleProbes++
			if filter.mayContain(id) {
				falsePositives++
			}
		}
	}
	run := func() error {
		var reader seriesindex.MetadataReader
		for _, p := range e.parts {
			if err := p.visitDiskMetadata(b.Context(), probeIDs, &reader, func(signal.SeriesID, seriesindex.Metadata) { b.Error("unexpected metadata") }); err != nil {
				return err
			}
		}
		return nil
	}
	for _, filtered := range []bool{false, true} {
		name := "unfiltered"
		if filtered {
			name = "filtered"
		}
		b.Run(name, func(b *testing.B) {
			for i, p := range e.parts {
				filter := filters[i]
				if !filtered {
					filter = nil
				}
				p.idBloom.Store(filter)
			}
			if err := run(); err != nil {
				b.Fatal(err)
			}
			counter.Reset()
			b.ReportAllocs()
			b.SetBytes(probes * 16)
			for b.Loop() {
				if err := run(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(filterBytes), "filter-B")
			b.ReportMetric(float64(counter.Bytes())/float64(b.N), "file-read-B/op")
			if filtered {
				b.ReportMetric(float64(falsePositives)/float64(eligibleProbes), "filter-FP-ratio")
				b.ReportMetric(float64(falsePositives), "possible-probes/op")
			} else {
				b.ReportMetric(float64(eligibleProbes), "possible-probes/op")
			}
		})
	}
}
