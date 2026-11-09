package chunk

import (
	"math"
	"math/rand/v2"
	"testing"
)

var decimalNormalizeMantissa int64
var decimalNormalizeExponent int

func decimalNormalizeInputs(name string) []float64 {
	values := make([]float64, 1024)
	random := rand.New(rand.NewPCG(123, 456))
	for i := range values {
		switch name {
		case "milli":
			values[i] = float64(i+1) / 1000
		case "centi":
			values[i] = float64(i+1) / 100
		case "precise":
			values[i] = math.Nextafter(float64(i+1)/1000, math.Inf(1))
		case "integer":
			values[i] = float64(i + 1)
		case "integer_zeros":
			values[i] = float64((i + 1) * 1000)
		case "random":
			values[i] = random.Float64() * 1e6
		case "tiny":
			values[i] = float64(i+1) * 1e-12
		}
	}
	return values
}

func BenchmarkFloatToDecimalInputs(b *testing.B) {
	for _, name := range []string{"milli", "centi", "precise", "integer", "integer_zeros", "random", "tiny"} {
		b.Run(name, func(b *testing.B) {
			values := decimalNormalizeInputs(name)
			b.SetBytes(int64(len(values)) * 8)
			b.ReportAllocs()
			for b.Loop() {
				var mantissa int64
				exponent := 0
				for _, value := range values {
					v, e := floatToDecimal(value)
					mantissa += v
					exponent += e
				}
				decimalNormalizeMantissa, decimalNormalizeExponent = mantissa, exponent
			}
		})
	}
}
