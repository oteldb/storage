package chunk

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/encoding/compress"
)

// Frozen framed-cost selection before candidate reuse.
func referenceFloatGroupCost(e *FloatGroupEncoder, dst []byte, values []float64, cost func([]byte) int) ([]byte, error) {
	start := len(dst)
	var ends []int
	if len(values) > 0 {
		ends = []int{len(values)}
	}
	out, err := e.Encode(dst, values, ends)
	if err != nil {
		return dst, err
	}
	if len(values) == 0 || cost == nil {
		return out, nil
	}
	_, rest, err := floatGroupHeader(out[start:])
	if err != nil {
		return dst, err
	}
	group, err := readFloatGroup(rest)
	if err != nil {
		return dst, err
	}
	if group.codec == CodecNone {
		return out, nil
	}
	best := cost(out[start:])
	if group.codec != CodecGorilla {
		e.gorilla = EncodeFloats(e.gorilla[:0], values)
		e.candidate = singleFloatGroup(e.candidate[:0], len(values), CodecGorilla, e.gorilla)
		if n := cost(e.candidate); n < best {
			out = append(out[:start], e.candidate...)
			best = n
		}
	}
	if group.codec != CodecDecimal {
		e.decimal = encodeFloatsDecimal(e.decimal[:0], values, 64, &e.decimalWork)
		var used int
		e.decoded, used, err = DecodeFloatsDecimal(e.decoded[:0], e.decimal)
		if err == nil && used == len(e.decimal) && equalFloatBits(values, e.decoded) {
			e.candidate = singleFloatGroup(e.candidate[:0], len(values), CodecDecimal, e.decimal)
			if n := cost(e.candidate); n < best {
				out = append(out[:start], e.candidate...)
			}
		}
	}
	return out, nil
}

func TestFloatGroupCostSelectionMatchesReference(t *testing.T) {
	t.Parallel()
	for _, algorithm := range []compress.Algorithm{compress.AlgorithmNone, compress.AlgorithmLZ4, compress.AlgorithmZSTD} {
		t.Run(algorithm.String(), func(t *testing.T) {
			t.Parallel()
			actual, reference := &FloatGroupEncoder{}, &FloatGroupEncoder{}
			newCompressor := compress.NewCompressor(algorithm, compress.LevelDefault)
			oldCompressor := compress.NewCompressor(algorithm, compress.LevelDefault)
			var newCompressed, oldCompressed, newOutput, oldOutput []byte
			for _, values := range [][]float64{
				nil, {1}, floatCostValues("milli", 16), floatCostValues("random", 64),
				floatCostValues("exceptional", 16), floatCostValues("constant", 64),
				{math.Float64frombits(1 << 63), 0},
				{math.Float64frombits(0x7ff0000000000042), math.Float64frombits(0x7ff0000000000042)},
				floatCostValues("milli", 64), {},
			} {
				newCalls, oldCalls := 0, 0
				costNew := func(data []byte) int {
					newCalls++
					newCompressed = newCompressor.Compress(newCompressed[:0], data)
					return len(newCompressed)
				}
				costOld := func(data []byte) int {
					oldCalls++
					oldCompressed = oldCompressor.Compress(oldCompressed[:0], data)
					return len(oldCompressed)
				}
				prefix := []byte{0xa5, 0x5a}
				var err error
				newOutput, err = actual.EncodeGroupWithCost(append(newOutput[:0], prefix...), values, costNew)
				require.NoError(t, err)
				oldOutput, err = referenceFloatGroupCost(reference, append(oldOutput[:0], prefix...), values, costOld)
				require.NoError(t, err)
				require.Equal(t, oldOutput, newOutput)
				require.Equal(t, oldCalls, newCalls)
				decoded, used, err := DecodeFloatGroups(nil, newOutput[len(prefix):])
				require.NoError(t, err)
				require.Equal(t, len(newOutput)-len(prefix), used)
				require.True(t, equalFloatBits(values, decoded))
			}
		})
	}
}

func FuzzFloatGroupCostReference(f *testing.F) {
	f.Add([]byte{})
	f.Add(binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, math.Float64bits(0.001)), math.Float64bits(0.002)))
	f.Add(binary.BigEndian.AppendUint64(nil, 1<<63))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 512 {
			t.Skip()
		}
		values := make([]float64, len(data)/8)
		for i := range values {
			values[i] = math.Float64frombits(binary.BigEndian.Uint64(data[i*8:]))
		}
		cost := func(data []byte) int { return (len(data) + 15) / 16 }
		got, err := (&FloatGroupEncoder{}).EncodeGroupWithCost([]byte{0xa5}, values, cost)
		if err != nil {
			t.Fatal(err)
		}
		want, err := referenceFloatGroupCost(&FloatGroupEncoder{}, []byte{0xa5}, values, cost)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("cost selection changed: got%x want%x", got, want)
		}
	})
}
