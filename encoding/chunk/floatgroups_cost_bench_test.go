package chunk

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/oteldb/storage/encoding/compress"
)

func floatCostValues(name string, rows int) []float64 {
	values := make([]float64, rows)
	random := rand.New(rand.NewPCG(789, 123))
	for i := range values {
		switch name {
		case "milli":
			values[i] = float64(i+1) / 1000
		case "random":
			values[i] = random.Float64() * 1e6
		case "constant":
			values[i] = 1
		case "exceptional":
			values[i] = []float64{math.Inf(1), math.Float64frombits(0x7ff8000000000042), math.Float64frombits(1 << 63), 0, float64(i)}[i%5]
		}
	}
	return values
}

func BenchmarkFloatGroupFramedCost(b *testing.B) {
	for _, algorithm := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmLZ4, compress.AlgorithmZSTD} {
		for _, name := range []string{"milli", "random", "constant", "exceptional"} {
			for _, rows := range []int{16, 64} {
				if name == "constant" && (algorithm != compress.AlgorithmNone || rows != 16) {
					continue
				}
				b.Run(fmt.Sprintf("%s/%s/rows=%d", algorithm, name, rows), func(b *testing.B) {
					values := floatCostValues(name, rows)
					compressor := compress.NewCompressor(algorithm, compress.LevelDefault)
					var compressed, output []byte
					encoder := &FloatGroupEncoder{}
					cost := func(data []byte) int { compressed = compressor.Compress(compressed[:0], data); return len(compressed) }
					b.SetBytes(int64(rows) * 8)
					b.ReportAllocs()
					for b.Loop() {
						var err error
						output, err = encoder.EncodeGroupWithCost(output[:0], values, cost)
						if err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(len(output))/float64(rows), "encoded-B/point")
					b.ReportMetric(float64(cost(output))/float64(rows), "stored-B/point")
				})
			}
		}
	}
}
