package chunk

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateFloatGroupsRows(t *testing.T) {
	t.Parallel()
	for _, values := range [][]float64{
		nil,
		{1, 1, 1},
		{0.001, 0.002, 0.003},
		{math.Pi, math.E, math.SmallestNonzeroFloat64},
		{0, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff8000000004321)},
	} {
		var enc FloatGroupEncoder
		var ends []int
		if len(values) > 0 {
			ends = []int{len(values)}
		}
		data, err := enc.Encode(nil, values, ends)
		require.NoError(t, err)
		require.NoError(t, ValidateFloatGroupsRows(data, len(values)))
		var visited []float64
		require.NoError(t, VisitFloatGroupsRows(data, len(values), func(v float64) { visited = append(visited, v) }))
		floatBitsEqual(t, values, visited)
		require.Error(t, ValidateFloatGroupsRows(data, len(values)+1))
		for i := range data {
			require.Error(t, ValidateFloatGroupsRows(data[:i], len(values)))
		}
		require.Error(t, ValidateFloatGroupsRows(append(data, 0), len(values)))
	}
	for _, tc := range []struct {
		name  string
		codec Codec
		data  []byte
	}{
		{"gorilla missing row", CodecGorilla, append([]byte{2}, make([]byte, 8)...)},
		{"gorilla invalid reuse", CodecGorilla, append(append([]byte{2}, make([]byte, 8)...), 0x80)},
		{"decimal missing row", CodecDecimal, []byte{2, 64, 0, 0}},
		{"decimal missing exponent", CodecDecimal, []byte{2, 64}},
		{"gorilla trailing bytes", CodecGorilla, append(EncodeFloats(nil, []float64{1, 2}), 0)},
		{"decimal trailing bytes", CodecDecimal, append(EncodeFloatsDecimal(nil, []float64{1, 2}, 64), 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := singleFloatGroup(nil, 2, tc.codec, tc.data)
			require.Error(t, ValidateFloatGroupsRows(data, 2))
			_, _, err := DecodeFloatGroupsRows(nil, data, 2)
			require.Error(t, err)
		})
	}
}

func TestValidateTimestampsAdaptiveRows(t *testing.T) {
	t.Parallel()
	for _, values := range [][]int64{
		{1},
		{1, 16, 31},
		{1, 4, 9, 17},
		{7, 7},
		{7, 6, 5},
		{math.MinInt64, 0, math.MaxInt64},
		{math.MaxInt64, math.MinInt64},
		{math.MinInt64, math.MaxInt64 - 1, math.MaxInt64 - 3},
	} {
		data, err := EncodeTimestampsAdaptive(nil, values)
		require.NoError(t, err)
		lo, hi, ordered, err := ValidateTimestampsAdaptiveRows(data, len(values))
		require.NoError(t, err)
		wantLo, wantHi, wantOrdered := timestampBounds(values)
		require.Equal(t, wantLo, lo)
		require.Equal(t, wantHi, hi)
		require.Equal(t, wantOrdered, ordered)
		_, _, _, err = ValidateTimestampsAdaptiveRows(data, len(values)+1)
		require.Error(t, err)
		for i := range data {
			_, _, _, err = ValidateTimestampsAdaptiveRows(data[:i], len(values))
			require.Error(t, err)
		}
		_, _, _, err = ValidateTimestampsAdaptiveRows(append(data, 0), len(values))
		require.Error(t, err)
	}
	for _, bad := range [][]byte{nil, {2, 1}, {1, 0}, {0, 0}} {
		_, _, _, err := ValidateTimestampsAdaptiveRows(bad, 1)
		require.Error(t, err)
	}
}

func timestampBounds(values []int64) (lo, hi int64, ordered bool) {
	lo, hi, ordered = values[0], values[0], true
	for i, v := range values {
		lo, hi = min(lo, v), max(hi, v)
		if i > 0 {
			ordered = ordered && values[i-1] < v
		}
	}
	return lo, hi, ordered
}

func FuzzValidateEncodedGranules(f *testing.F) {
	var enc FloatGroupEncoder
	values := []float64{math.Pi, math.Copysign(0, -1)}
	floats, _ := enc.Encode(nil, values, []int{len(values)})
	timestamps, _ := EncodeTimestampsAdaptive(nil, []int64{1, 4})
	f.Add(floats, timestamps, uint8(1))
	timestamps, _ = EncodeTimestampsAdaptive(nil, []int64{1, 4, 9})
	f.Add(singleFloatGroup(nil, 3, CodecDecimal, EncodeFloatsDecimal(nil, []float64{1, 2, 3}, 64)), timestamps, uint8(2))
	f.Add([]byte{1, 2, 1, 2, 2, 1, 2}, []byte{1, 0}, uint8(1))
	f.Fuzz(func(t *testing.T, floats, timestamps []byte, size uint8) {
		if len(floats)+len(timestamps) > 8192 {
			t.Skip()
		}
		rows := int(size%64) + 1
		err := ValidateFloatGroupsRows(floats, rows)
		_, _, decodedErr := DecodeFloatGroupsRows(nil, floats, rows)
		require.Equal(t, decodedErr != nil, err != nil)
		var visited []float64
		visitErr := VisitFloatGroupsRows(floats, rows, func(v float64) { visited = append(visited, v) })
		require.Equal(t, decodedErr != nil, visitErr != nil)
		if visitErr == nil {
			decoded, _, decodeErr := DecodeFloatGroupsRows(nil, floats, rows)
			require.NoError(t, decodeErr)
			floatBitsEqual(t, decoded, visited)
		}
		lo, hi, ordered, err := ValidateTimestampsAdaptiveRows(timestamps, rows)
		decoded, _, decodedErr := DecodeTimestampsAdaptiveRows(nil, timestamps, rows)
		require.Equal(t, decodedErr != nil, err != nil)
		if err == nil {
			wantLo, wantHi, wantOrdered := timestampBounds(decoded)
			require.Equal(t, wantLo, lo)
			require.Equal(t, wantHi, hi)
			require.Equal(t, wantOrdered, ordered)
		}
	})
}

func BenchmarkValidateEncodedFloats(b *testing.B) {
	var enc FloatGroupEncoder
	values := make([]float64, 16)
	for i := range values {
		values[i] = float64(i) / 1000
	}
	data, err := enc.Encode(nil, values, []int{len(values)})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	for b.Loop() {
		if err := ValidateFloatGroupsRows(data, len(values)); err != nil {
			b.Fatal(err)
		}
	}
}

func TestValidateRegularTimestampOverflow(t *testing.T) {
	t.Parallel()
	data := []byte{0}
	data = binary.AppendUvarint(data, 5)
	data = binary.AppendVarint(data, math.MinInt64)
	data = binary.AppendVarint(data, math.MaxInt64)
	values, _, err := DecodeTimestampsAdaptive(nil, data)
	require.NoError(t, err)
	lo, hi, ordered, err := ValidateTimestampsAdaptiveRows(data, len(values))
	require.NoError(t, err)
	wantLo, wantHi, wantOrdered := timestampBounds(values)
	require.Equal(t, wantLo, lo)
	require.Equal(t, wantHi, hi)
	require.Equal(t, wantOrdered, ordered)
}
