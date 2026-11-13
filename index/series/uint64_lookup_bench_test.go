package series

import (
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

// BenchmarkUint64Resolution keeps full identity checks in both uint64-key alternatives.
// Fixture hashing, registry construction and table construction are outside lookup timing.
func BenchmarkUint64Resolution(b *testing.B) {
	hosts := 10000
	if s := os.Getenv("OTELDB_BENCH_SERIES_HOSTS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 10000 {
			b.Fatal("invalid hosts")
		}
		hosts = n
	}
	repeats := 6
	if s := os.Getenv("OTELDB_BENCH_PACKED_REPEATS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 10 {
			b.Fatal("invalid repeats")
		}
		repeats = n
	}
	fixtureCtx := pprof.WithLabels(b.Context(), pprof.Labels("phase", "fixture"))
	pprof.SetGoroutineLabels(fixtureCtx)
	defer pprof.SetGoroutineLabels(b.Context())
	f := newSeriesOnlyFixture(b, hosts)
	ids := make([]signal.SeriesID, seriesOnlyBatchSize)
	buf := make([]byte, 0, 4096)
	prepare := func(offset int) []signal.Series {
		batch := f.prepareSeriesBatch(offset)
		for i := range batch {
			buf = batch[i].AppendHashInput(buf[:0])
			ids[i] = signal.HashBytes(buf)
		}
		return batch
	}
	ix := NewWithAttributeTemplates(nil, exporterPool("dimensions", "cold"))
	count := f.count()
	var expectedChecksum uint64
	for offset := 0; offset < count; offset += seriesOnlyBatchSize {
		batch := prepare(offset)
		for i := range batch {
			ix.AddEntry(Entry{ID: ids[i], Series: batch[i]})
			expectedChecksum ^= ids[i].Hi ^ ids[i].Lo
		}
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	baselineCopy := make(map[signal.SeriesID]uint32)
	for ref := range ix.rows {
		baselineCopy[ix.rows[ref].id] = uint32(ref)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	baselineBytes := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	runtime.KeepAlive(baselineCopy)
	runtime.GC()
	runtime.ReadMemStats(&before)
	compact := newUint64RowRefs()
	for ref := range ix.rows {
		compact.put(ix.rows[ref].id, uint32(ref), ix.rows)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	compactBytes := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	runtime.ReadMemStats(&before)
	inline := newUint64InlineRefs()
	for ref := range ix.rows {
		inline.put(ix.rows[ref].id, uint32(ref))
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	inlineBytes := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	for repeat := range repeats {
		modes := []string{"uint128", "uint64-row-check", "uint64-inline-check"}
		if repeat%2 != 0 {
			modes = []string{"uint64-inline-check", "uint64-row-check", "uint128"}
		}
		for _, mode := range modes {
			b.Run("hosts="+strconv.Itoa(hosts)+"/repeat="+strconv.Itoa(repeat)+"/"+mode, func(b *testing.B) {
				workCtx := pprof.WithLabels(b.Context(), pprof.Labels("phase", "resolve", "mode", mode))
				b.ReportAllocs()
				for b.Loop() {
					var sum, checksum uint64
					for offset := 0; offset < count; offset += seriesOnlyBatchSize {
						b.StopTimer()
						pprof.SetGoroutineLabels(fixtureCtx)
						batch := prepare(offset)
						pprof.SetGoroutineLabels(workCtx)
						b.StartTimer()
						for i := range batch {
							id := ids[i]
							var ref uint32
							var ok bool
							switch mode {
							case "uint128":
								ref, ok = baselineCopy[id]
							case "uint64-row-check":
								ref, ok = compact.get(id, ix.rows)
							case "uint64-inline-check":
								ref, ok = inline.get(id)
							}
							if !ok {
								b.Fatal("missing identity")
							}
							sum += uint64(ref)
							checksum ^= id.Hi ^ id.Lo
						}
					}
					b.StopTimer()
					pprof.SetGoroutineLabels(fixtureCtx)
					if sum != uint64(count)*uint64(count-1)/2 || checksum != expectedChecksum {
						b.Fatal("incorrect identity or reference")
					}
					packedResolutionSink = checksum
					b.StartTimer()
				}
				var tableBytes int64
				switch mode {
				case "uint128":
					tableBytes = baselineBytes
				case "uint64-row-check":
					tableBytes = compactBytes
				case "uint64-inline-check":
					tableBytes = inlineBytes
				}
				b.ReportMetric(float64(count)*float64(b.N)/b.Elapsed().Seconds(), "series/s")
				b.ReportMetric(float64(tableBytes), "table-heap-B")
				runtime.KeepAlive(baselineCopy)
				runtime.KeepAlive(ix)
				runtime.KeepAlive(compact)
				runtime.KeepAlive(inline)
			})
		}
	}
}
