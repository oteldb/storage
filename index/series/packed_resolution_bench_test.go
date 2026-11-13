package series

import (
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

type packedFixture struct {
	fixture *seriesOnlyFixture
	data    []byte
	keys    [][]byte
	ids     []signal.SeriesID
}

func (p *packedFixture) prepare(offset int) []signal.Series {
	batch := p.fixture.prepareSeriesBatch(offset)
	p.data = p.data[:0]
	p.keys = p.keys[:0]
	p.ids = p.ids[:0]
	for i := range batch {
		start := len(p.data)
		p.data = batch[i].AppendHashInput(p.data)
		key := p.data[start:len(p.data):len(p.data)]
		p.keys = append(p.keys, key)
		p.ids = append(p.ids, signal.HashBytes(key))
	}
	return batch
}

var packedResolutionSink uint64

// BenchmarkPackedResolution compares resolution of the same packed inputs and registered identities.
// Packing, registration and cache warming are excluded. All caches stay live for every mode.
func BenchmarkPackedResolution(b *testing.B) {
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
	p := packedFixture{fixture: newSeriesOnlyFixture(b, hosts), data: make([]byte, 0, 4<<20), keys: make([][]byte, 0, seriesOnlyBatchSize), ids: make([]signal.SeriesID, 0, seriesOnlyBatchSize)}
	ix := NewWithAttributeTemplates(nil, exporterPool("dimensions", "cold"))
	count := p.fixture.count()
	var expectedChecksum uint64
	for offset := 0; offset < count; offset += seriesOnlyBatchSize {
		batch := p.prepare(offset)
		for i := range batch {
			ix.AddEntry(Entry{ID: p.ids[i], Series: batch[i]})
			expectedChecksum ^= p.ids[i].Hi ^ p.ids[i].Lo
		}
	}
	if ix.Len() != count {
		b.Fatal("identity count changed")
	}
	caches := []*packedCache{newPackedCache(256<<20, count), newPackedCache(6<<30, count)}
	for offset := 0; offset < count; offset += seriesOnlyBatchSize {
		p.prepare(offset)
		for i, key := range p.keys {
			for _, c := range caches {
				c.put(key, p.ids[i], uint32(offset+i), 1)
			}
		}
	}
	runtime.GC()
	for repeat := range repeats {
		modes := []string{"id-map", "packed-id-map", "packed-cache-256MiB", "packed-cache-6GiB"}
		if repeat%2 != 0 {
			modes = []string{"packed-cache-6GiB", "packed-cache-256MiB", "packed-id-map", "id-map"}
		}
		for _, mode := range modes {
			b.Run("hosts="+strconv.Itoa(hosts)+"/repeat="+strconv.Itoa(repeat)+"/"+mode, func(b *testing.B) {
				workCtx := pprof.WithLabels(b.Context(), pprof.Labels("phase", "resolve", "mode", mode))
				var c *packedCache
				if mode == "packed-cache-256MiB" {
					c = caches[0]
				}
				if mode == "packed-cache-6GiB" {
					c = caches[1]
				}
				var hits, lookups uint64
				b.ReportAllocs()
				for b.Loop() {
					var sum, checksum uint64
					for offset := 0; offset < count; offset += seriesOnlyBatchSize {
						b.StopTimer()
						pprof.SetGoroutineLabels(fixtureCtx)
						p.prepare(offset)
						pprof.SetGoroutineLabels(workCtx)
						b.StartTimer()
						for i, key := range p.keys {
							var id signal.SeriesID
							var ref uint32
							switch mode {
							case "id-map":
								id = p.ids[i]
								var ok bool
								ref, ok = ix.Ref(id)
								if !ok {
									b.Fatal("missing identity")
								}
							case "packed-id-map":
								id = signal.HashBytes(key)
								var ok bool
								ref, ok = ix.Ref(id)
								if !ok {
									b.Fatal("missing identity")
								}
							default:
								var current, found bool
								id, ref, current, found = c.lookup(key, 1)
								if found && current {
									hits++
								} else {
									id = signal.HashBytes(key)
									var ok bool
									ref, ok = ix.Ref(id)
									if !ok {
										b.Fatal("missing identity")
									}
								}
							}
							lookups++
							sum += uint64(ref)
							checksum ^= id.Hi ^ id.Lo
						}
					}
					b.StopTimer()
					pprof.SetGoroutineLabels(fixtureCtx)
					if sum != uint64(count)*uint64(count-1)/2 || checksum != expectedChecksum {
						b.Fatal("wrong local references")
					}
					packedResolutionSink = checksum
					b.StartTimer()
				}
				b.ReportMetric(float64(count)*float64(b.N)/b.Elapsed().Seconds(), "series/s")
				b.ReportMetric(float64(ix.SizeBytes()), "registry-B")
				if c != nil {
					b.ReportMetric(float64(hits)/float64(lookups), "hit-ratio")
					b.ReportMetric(float64(c.sizeBytes()), "cache-B")
					b.ReportMetric(float64(c.count), "cached-series")
				}
				runtime.KeepAlive(ix)
				runtime.KeepAlive(caches)
			})
		}
	}
}
