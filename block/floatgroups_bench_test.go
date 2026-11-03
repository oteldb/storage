package block

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func groupBenchmarkValues() ([]float64, []int) {
	r := rand.New(rand.NewPCG(123, 456))
	values := make([]float64, 0, 3840)
	ends := make([]int, 0, 64)
	for series := range 64 {
		for point := range 60 {
			var value float64
			switch series % 4 {
			case 0:
				value = float64(series)
			case 1:
				value = 1e9 + float64(point*13+series)
			case 2:
				value = math.Sin(float64(point%20)) * 1e-3
			default:
				value = r.Float64() * 1e6
			}
			values = append(values, value)
		}
		ends = append(ends, len(values))
	}
	return values, ends
}

func groupBenchmarkPart(tb testing.TB, values []float64, ends []int, grouped bool, alg compress.Algorithm) builtPart {
	tb.Helper()
	w := NewStreamWriter(WithGranuleSize(1024), WithCompressBlockBytes(8<<10), WithCompression(alg), WithSizingStats())
	c := Column{Name: "v", Kind: KindFloat64, AutoCodec: true, Block: true}
	if grouped {
		c.Codec = chunk.CodecFloatGroups
		c.AutoCodec = false
	}
	if err := w.AddColumn(c); err != nil {
		tb.Fatal(err)
	}
	lo := 0
	for _, hi := range ends {
		var err error
		if grouped {
			err = w.AppendFloat64Group(0, values[lo:hi], true)
		} else {
			err = w.AppendFloat64(0, values[lo:hi])
		}
		if err != nil {
			tb.Fatal(err)
		}
		lo = hi
	}
	part, err := w.build()
	if err != nil {
		tb.Fatal(err)
	}
	return part
}

func BenchmarkFloatGroups(b *testing.B) {
	values, ends := groupBenchmarkValues()
	for _, alg := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmZSTD} {
		for _, grouped := range []bool{false, true} {
			name := "legacy"
			if grouped {
				name = "grouped"
			}
			b.Run(alg.String()+"/"+name, func(b *testing.B) {
				part := groupBenchmarkPart(b, values, ends, grouped, alg)
				mem := backend.Memory()
				if err := part.write(b.Context(), mem, "p"); err != nil {
					b.Fatal(err)
				}
				reader, err := OpenPart(b.Context(), mem, "p")
				if err != nil {
					b.Fatal(err)
				}
				col, err := reader.Column(b.Context(), "v")
				if err != nil {
					b.Fatal(err)
				}
				size := len(part.objects[0])
				b.Run("encode", func(b *testing.B) {
					b.SetBytes(int64(len(values)) * 8)
					b.ReportAllocs()
					for b.Loop() {
						groupBenchmarkPart(b, values, ends, grouped, alg)
					}
					b.ReportMetric(float64(size)/float64(len(values)), "stored-B/point")
				})
				b.Run("decode", func(b *testing.B) {
					b.SetBytes(int64(len(values)) * 8)
					b.ReportAllocs()
					var dst []float64
					for b.Loop() {
						dst, err = col.Float64(dst)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}
