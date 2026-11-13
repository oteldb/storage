package engine

import (
	"math"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func BenchmarkDiskSeriesCacheHostWorkingSet(b *testing.B) {
	for _, workload := range []struct {
		name  string
		count int
		bytes int64
	}{
		{name: "10k-hosts-per-head", count: 10000 * 1449 / 8, bytes: (480 << 20) / 8 * 7 / 8},
		{name: "over-budget", count: 100000, bytes: 1 << 20},
	} {
		b.Run(workload.name, func(b *testing.B) {
			c := newDiskSeriesCache(workload.bytes)
			ids := make([]signal.SeriesID, workload.count)
			for i := range ids {
				ids[i] = signal.SeriesID{Hi: mixDiskBloom(uint64(i)), Lo: mixDiskBloom(uint64(i) + 1)}
				c.put(ids[i], 100)
			}
			var hits uint64
			var passes uint64
			b.ReportAllocs()
			for b.Loop() {
				for _, id := range ids {
					if _, hit := c.get(id); hit {
						hits++
					}
					c.put(id, 101)
				}
				passes++
			}
			b.ReportMetric(float64(hits)/float64(passes*uint64(len(ids))), "hit-ratio")
			b.ReportMetric(float64(c.sizeBytes()), "cache-bytes")
		})
	}
}

func BenchmarkDiskSeriesHistoricalWorkingSet(b *testing.B) {
	options := cpuFixtureOptions{hosts: 10000, generations: 4, points: 1, disk: true, separate: true, flushMemoryBytes: 2 << 30}
	f := newCPUFixture(b, b.Context(), options)
	f.engine.cfg.ResidentHeadBytes = math.MaxInt64
	f.engine.cfg.Backend = backend.Cached(f.counter, 1<<20)
	const count = 10000 * len(cpuFixtureModes)
	f.engine.head.disk.cache = newDiskSeriesCache(int64((480 << 20) / 8 * 7 / 8 * count / (10000 * 1449 / 8)))
	for generation := range options.generations {
		f.appendGeneration(b, generation)
		if err := f.engine.Flush(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
	ids := make([]signal.SeriesID, count)
	for i := range ids {
		ids[i] = cpuMetricSeries(i).Hash()
	}
	// Reuse pending admission scratch; no samples are appended or flushed in the timed body.
	f.engine.head.disk.pending = make(map[signal.SeriesID]diskHeadEntry, 4096)
	f.counter.Reset()
	b.ReportAllocs()
	for b.Loop() {
		for first := 0; first < len(ids); first += 4096 {
			clear(f.engine.head.disk.pending)
			f.engine.mu.LockWriter()
			err := f.engine.preloadDiskSeriesMoreLocked(ids[first:min(first+4096, len(ids))])
			f.engine.mu.UnlockWriter()
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportMetric(float64(f.counter.Bytes())/float64(b.N), "file-read-B/op")
	b.ReportMetric(float64(count), "ids/op")
}
