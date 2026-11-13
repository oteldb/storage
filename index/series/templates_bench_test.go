package series

import (
	"bytes"
	"math/rand/v2"
	"runtime"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

func templateBenchInput(order string) []signal.Series {
	const hosts, shapes = 10000, 8
	out := make([]signal.Series, hosts*shapes)
	for i := range out {
		host, shape := i/shapes, i%shapes
		if order == "metric-major" {
			host, shape = i%hosts, i/hosts
		}
		if order == "sparse-1449-shapes" || order == "unique-fixed" {
			host, shape = (i*7919)%hosts, i%1449
			if order == "unique-fixed" {
				shape = i
			}
		}
		s := templateSeries(host, 0)
		for j := range s.Attributes {
			if bytes.Equal(s.Attributes[j].Key, []byte("mode")) {
				s.Attributes[j].Value = signal.StringValue([]byte(strconv.Itoa(shape)))
			}
			if (order == "sparse-1449-shapes" || order == "unique-fixed") && bytes.Equal(s.Attributes[j].Key, []byte("__name__")) {
				s.Attributes[j].Value = signal.StringValue([]byte("node_metric_" + strconv.Itoa(shape)))
			}
		}
		s.Attributes = signal.NewAttributes(append(s.Attributes,
			signal.KeyValue{Key: []byte("__unit__"), Value: signal.StringValue([]byte("s"))},
			signal.KeyValue{Key: []byte("__kind__"), Value: signal.IntValue(1)},
			signal.KeyValue{Key: []byte("__temporality__"), Value: signal.IntValue(2)},
			signal.KeyValue{Key: []byte("__monotonic__"), Value: signal.BoolValue(true)},
		)...)
		out[i] = s
	}
	if order == "shuffled" {
		rand.New(rand.NewPCG(1, 2)).Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	}
	return out
}

func BenchmarkAttributeTemplatesIngest(b *testing.B) {
	for _, order := range []string{"host-major", "metric-major", "shuffled", "sparse-1449-shapes", "unique-fixed"} {
		input := templateBenchInput(order)
		for _, mode := range []string{"flat", "whole-tuple", "instance", "instance-cpu"} {
			b.Run(order+"/"+mode, func(b *testing.B) {
				var pool *AttributeTemplates
				switch mode {
				case "whole-tuple":
					pool = NewAttributeTemplates(8 << 20)
				case "instance":
					pool = NewAttributeTemplates(8<<20, []byte("instance"))
				case "instance-cpu":
					pool = NewAttributeTemplates(8<<20, []byte("instance"), []byte("cpu"))
				}
				// Warm shared templates once, as a preceding resident-head generation would.
				if pool != nil {
					warm := NewWithAttributeTemplates(nil, pool)
					for i := range input {
						warm.Add(input[i])
					}
				}
				var last *Index
				b.ReportAllocs()
				for b.Loop() {
					last = NewWithAttributeTemplates(nil, pool)
					for i := range input {
						last.Add(input[i])
					}
				}
				b.ReportMetric(float64(last.SizeBytes()+pool.SizeBytes()), "charged-B")
				b.ReportMetric(float64(pool.SizeBytes()), "pool-B")
				var templated int
				for _, r := range last.rows {
					if r.keys&templateFlag != 0 {
						templated++
					}
				}
				b.ReportMetric(float64(templated)/float64(len(input)), "templated-ratio")
			})
		}
	}
}

// BenchmarkAttributeTemplatesMemory includes pool and registry memory; run with -benchtime=1x.
func BenchmarkAttributeTemplatesMemory(b *testing.B) {
	for _, order := range []string{"host-major", "sparse-1449-shapes"} {
		input := templateBenchInput(order)
		for _, heads := range []int{1, 8} {
			groups := make([][]signal.Series, heads)
			for i := range input {
				index := int(input[i].Hash().Lo % uint64(heads))
				groups[index] = append(groups[index], input[i])
			}
			for _, enabled := range []bool{false, true} {
				b.Run(order+"/heads="+strconv.Itoa(heads)+"/templates="+strconv.FormatBool(enabled), func(b *testing.B) {
					for b.Loop() {
						runtime.GC()
						var before, after runtime.MemStats
						runtime.ReadMemStats(&before)
						var pool *AttributeTemplates
						if enabled {
							pool = NewAttributeTemplates(8<<20, []byte("instance"))
						}
						indexes := make([]*Index, heads)
						var charged int64
						for i, group := range groups {
							indexes[i] = NewWithAttributeTemplates(nil, pool)
							for j := range group {
								indexes[i].Add(group[j])
							}
							charged += indexes[i].SizeBytes()
						}
						runtime.GC()
						runtime.ReadMemStats(&after)
						b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "heap-delta-B")
						b.ReportMetric(float64(int64(after.HeapObjects)-int64(before.HeapObjects)), "heap-delta-objects")
						b.ReportMetric(float64(charged+pool.SizeBytes()), "charged-B")
						runtime.KeepAlive(indexes)
						runtime.KeepAlive(pool)
						runtime.KeepAlive(groups)
					}
				})
			}
		}
	}
}

func BenchmarkAttributeTemplatesRead(b *testing.B) {
	input := templateBenchInput("host-major")
	var logicalBytes int64
	var encoded []byte
	for i := range input {
		encoded = input[i].AppendHashInput(encoded[:0])
		logicalBytes += int64(len(encoded))
	}
	for _, enabled := range []bool{false, true} {
		var pool *AttributeTemplates
		if enabled {
			pool = NewAttributeTemplates(8<<20, []byte("instance"))
		}
		ix := NewWithAttributeTemplates(nil, pool)
		for i := range input {
			ix.Add(input[i])
		}
		view := ix.View()
		b.Run("templates="+strconv.FormatBool(enabled), func(b *testing.B) {
			b.Run("canonical", func(b *testing.B) {
				buf := make([]byte, 0, 1024)
				b.SetBytes(logicalBytes)
				b.ReportAllocs()
				for b.Loop() {
					for i := range view.Len() {
						buf = view.AppendCanonical(buf[:0], i)
					}
				}
			})
			b.Run("materialize", func(b *testing.B) {
				var attrs signal.Attributes
				b.SetBytes(logicalBytes)
				b.ReportAllocs()
				for b.Loop() {
					for i := range view.Len() {
						attrs = view.At(i, attrs).Attributes
					}
				}
			})
		})
	}
}
