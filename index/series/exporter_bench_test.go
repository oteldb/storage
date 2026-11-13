package series

import (
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/oteldb/storage/signal"
)

func exporterPool(mode, state string) *AttributeTemplates {
	budget := int64(8 << 20)
	if state == "full-small-budget" {
		budget = 512 << 10
	}
	switch mode {
	case "instance":
		return NewAttributeTemplates(budget, []byte("instance"))
	case "dimensions":
		return NewAttributeTemplates(budget, []byte("instance"), []byte("cpu"), []byte("mode"), []byte("device"), []byte("mountpoint"), []byte("quantile"), []byte("le"))
	default:
		return nil
	}
}

func trainExporter(pool *AttributeTemplates, input []signal.Series, partial bool) {
	if pool == nil {
		return
	}
	for range 3 {
		ix := NewWithAttributeTemplates(nil, pool)
		for i := range input {
			if !partial || i%2 == 0 {
				ix.Add(input[i])
			}
		}
	}
}

func fillExporterPool(pool *AttributeTemplates) {
	if pool == nil {
		return
	}
	prior := pool.SizeBytes()
	for pattern := range 12000 {
		s := templateSeries(0, 0)
		s.Attributes = append(s.Attributes, signal.KeyValue{Key: []byte("obsolete"), Value: signal.StringValue([]byte(strconv.Itoa(pattern)))})
		warmExporterCandidate(pool, s)
		if pattern%512 == 511 {
			if pool.SizeBytes() == prior {
				return
			}
			prior = pool.SizeBytes()
		}
	}
}

func warmExporterCandidate(pool *AttributeTemplates, s signal.Series) {
	ix := NewWithAttributeTemplates(nil, pool)
	for i := range 512 {
		s.Resource.SchemaURL = []byte(strconv.Itoa(i))
		ix.Add(s)
	}
}

// BenchmarkCapturedExporterIndexes models concurrent registry generations, not engine ingestion.
func BenchmarkCapturedExporterIndexes(b *testing.B) {
	input := exporterInput(b, 64)
	for _, heads := range []int{1, 8} {
		groups := make([][]signal.Series, heads)
		for i := range input {
			head := input[i].Hash().Lo % uint64(heads)
			groups[head] = append(groups[head], input[i])
		}
		for _, state := range []string{"cold", "warm", "partial", "full-small-budget"} {
			for _, mode := range []string{"flat", "instance", "dimensions"} {
				b.Run("heads="+strconv.Itoa(heads)+"/"+state+"/"+mode, func(b *testing.B) {
					pool := exporterPool(mode, state)
					switch state {
					case "warm":
						trainExporter(pool, input, false)
					case "full-small-budget":
						fillExporterPool(pool)
					}
					var indexes []*Index
					b.ReportAllocs()
					for b.Loop() {
						if state == "partial" {
							b.StopTimer()
							pool = exporterPool(mode, state)
							trainExporter(pool, input, true)
							runtime.GC()
							b.StartTimer()
						}
						if state == "cold" {
							pool = exporterPool(mode, state)
						}
						indexes = buildExporterIndexes(groups, pool)
					}
					var charged, templated int64
					for h, ix := range indexes {
						if ix.Len() != len(groups[h]) {
							b.Fatal("identity count changed")
						}
						charged += ix.SizeBytes()
						for _, r := range ix.rows {
							if r.keys&templateFlag != 0 {
								templated++
							}
						}
					}
					b.ReportMetric(float64(charged+pool.SizeBytes()), "charged-B")
					b.ReportMetric(float64(pool.SizeBytes()), "pool-B")
					b.ReportMetric(float64(templated)/float64(len(input)), "templated-ratio")
				})
			}
		}
	}
}

func buildExporterIndexes(groups [][]signal.Series, pool *AttributeTemplates) []*Index {
	indexes := make([]*Index, len(groups))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for h := range groups {
		wg.Go(func() {
			<-start
			ix := NewWithAttributeTemplates(nil, pool)
			for i := range groups[h] {
				ix.Add(groups[h][i])
			}
			indexes[h] = ix
		})
	}
	close(start)
	wg.Wait()
	return indexes
}

// BenchmarkCapturedExporterMemory includes retained pools and indexes; run with -benchtime=1x.
func BenchmarkCapturedExporterMemory(b *testing.B) {
	input := exporterInput(b, 64)
	groups := make([][]signal.Series, 8)
	for i := range input {
		h := input[i].Hash().Lo % 8
		groups[h] = append(groups[h], input[i])
	}
	for _, state := range []string{"cold", "warm"} {
		for _, mode := range []string{"flat", "instance", "dimensions"} {
			b.Run(state+"/"+mode, func(b *testing.B) {
				for b.Loop() {
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					pool := exporterPool(mode, state)
					if state == "warm" {
						trainExporter(pool, input, false)
					}
					indexes := buildExporterIndexes(groups, pool)
					runtime.GC()
					runtime.ReadMemStats(&after)
					b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "heap-delta-B")
					b.ReportMetric(float64(int64(after.HeapObjects)-int64(before.HeapObjects)), "heap-delta-objects")
					b.ReportMetric(float64(pool.SizeBytes()), "pool-B")
					runtime.KeepAlive(indexes)
					runtime.KeepAlive(pool)
					runtime.KeepAlive(groups)
				}
			})
		}
	}
}
