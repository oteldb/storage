package engine

import (
	"fmt"
	"math"
	"testing"

	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

// BenchmarkSharedWatermarkBatch isolates ingestion metadata probes on a native-file part.
func BenchmarkSharedWatermarkBatch(b *testing.B) {
	for _, mode := range []string{"point", "window"} {
		b.Run(mode, func(b *testing.B) { benchmarkSharedWatermarkBatch(b, mode) })
	}
}

func benchmarkSharedWatermarkBatch(b *testing.B, mode string) {
	b.Helper()
	options := cpuFixtureOptions{hosts: 10000, generations: 1, points: 1, disk: true, separate: true, sync: true, readCache: true, flushMemoryBytes: 2 << 30}
	fixture := newCPUFixture(b, b.Context(), options)
	fixture.engine.cfg.ResidentHeadBytes = math.MaxInt64
	input := newCPUStorageInput(options, 0)
	if err := input.appendTo(fixture.engine); err != nil {
		b.Fatal(err)
	}
	if err := fixture.engine.Flush(b.Context()); err != nil {
		b.Fatal(err)
	}
	if fixture.engine.PartCount() != 1 {
		b.Fatal("fixture must contain one numeric part")
	}
	p := fixture.engine.parts[0]
	all := make([]signal.SeriesID, 0, 80000)
	iter := p.diskRowIDs(b.Context())
	for iter.Next() {
		all = append(all, iter.At())
	}
	if err := iter.Err(); err != nil {
		b.Fatal(err)
	}
	if len(all) != 80000 {
		b.Fatalf("series=%d", len(all))
	}
	const count = 4096
	ids := make([]signal.SeriesID, count)
	for i := range ids {
		ids[i] = all[i*len(all)/count]
	}
	p.index.dropView()
	p.acquire()
	b.Cleanup(p.release)
	ctx := p.reader.ReaderContext(b.Context())
	if _, err := p.sharedWatermarkReader(ctx); err != nil {
		b.Fatal(err)
	}
	var reader seriesindex.MetadataReader
	var cursor watermark.Cursor
	// Warm the index's range cache for identical ordinal/membership lookup costs.
	for _, id := range ids {
		if _, ok, err := p.index.position(ctx, id); err != nil || !ok {
			b.Fatal(fmt.Sprint(ok, err))
		}
	}
	fixture.counter.Reset()
	total := 0
	visit := func(_ signal.SeriesID, meta seriesindex.Metadata) {
		if meta.Covered != cpuFixtureBase {
			b.Fatal("incorrect covered timestamp")
		}
		total++
	}
	b.ReportAllocs()
	for b.Loop() {
		if mode == "point" {
			for _, id := range ids {
				meta, ok, err := p.sharedSeriesMetadata(ctx, id)
				if err != nil || !ok {
					b.Fatal(fmt.Sprint(ok, err))
				}
				visit(id, meta)
			}
		} else if err := p.visitDiskMetadataWindow(ctx, ids, &reader, &cursor, visit); err != nil {
			b.Fatal(err)
		}
	}
	if total != b.N*count {
		b.Fatal("incomplete metadata batch")
	}
	b.ReportMetric(float64(count), "ids/op")
	b.ReportMetric(float64(fixture.counter.Bytes())/float64(b.N), "file-read-B/op")
}
