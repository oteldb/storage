package chunk

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func floatBitsEqual(tb testing.TB, want, got []float64) {
	tb.Helper()
	require.Len(tb, got, len(want))
	for i, v := range want {
		require.Equal(tb, math.Float64bits(v), math.Float64bits(got[i]), "row %d", i)
	}
}

func TestFloatGroups(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		values []float64
		ends   []int
	}{
		{name: "empty"},
		{"constants", []float64{1, 1, 1, 2, 2}, []int{3, 5}},
		{"integers", []float64{1, 2, 3, 1e9, 1e9 + 1, 1e9 + 2}, []int{3, 6}},
		{"special", []float64{0, math.Copysign(0, -1), math.Inf(1), math.Inf(-1), math.Float64frombits(0x7ff0000000000002), math.Float64frombits(0x7ff8000000004321), math.SmallestNonzeroFloat64, math.MaxFloat64, math.Pi}, []int{2, 4, 6, 9}},
		{"constant_nan", []float64{math.Float64frombits(0x7ff0000000000002), math.Float64frombits(0x7ff0000000000002)}, []int{2}},
		{"fractional", []float64{math.Pi, math.E, 0.1, 0.2, 0.3}, []int{2, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var enc FloatGroupEncoder
			data, err := enc.Encode([]byte{9}, tc.values, tc.ends)
			require.NoError(t, err)
			require.Equal(t, byte(9), data[0])
			data = data[1:]
			if tc.name == "fractional" {
				require.Positive(t, enc.ResidentBytes())
			}
			got, n, err := DecodeFloatGroups(make([]float64, 5, 50), data)
			require.NoError(t, err)
			require.Equal(t, len(data), n)
			floatBitsEqual(t, tc.values, got)
			checked, _, err := DecodeFloatGroupsRows(nil, data, len(tc.values))
			require.NoError(t, err)
			floatBitsEqual(t, tc.values, checked)
			cursor, err := NewFloatDecoder(CodecFloatGroups, data)
			require.NoError(t, err)
			require.Equal(t, len(tc.values), cursor.Len())
			for i, want := range tc.values {
				v, err := cursor.Next()
				require.NoError(t, err)
				require.Equal(t, math.Float64bits(want), math.Float64bits(v))
				require.Equal(t, i+1, cursor.Pos())
			}
			_, err = cursor.Next()
			require.Error(t, err)
			_, _, err = DecodeFloatGroupsRows(nil, data, len(tc.values)+1)
			require.Error(t, err)
			for j := range data {
				_, _, err = DecodeFloatGroups(nil, data[:j])
				require.Error(t, err, "truncated %d", j)
			}
		})
	}
}

func TestFloatGroupsRejectMalformed(t *testing.T) {
	t.Parallel()
	var enc FloatGroupEncoder
	for _, ends := range [][]int{nil, {0}, {2}, {1, 1}, {-1}, {3}} {
		_, err := enc.Encode(nil, []float64{1}, ends)
		require.Error(t, err)
	}
	data, err := enc.Encode(nil, []float64{1, 1}, []int{2})
	require.NoError(t, err)
	bad := [][]byte{oversizedFloatGroupHeader(), nil, {2}, append(append([]byte{}, data...), 0), {1, 2, 0}, {1, 2, 3}, {1, 1, 1, 255, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0}, {1, 1, 1, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0}, {1, 1, 1, 0, 1, 7, 0, 0, 0, 0, 0, 0, 0}, {1, 1, 1, 2, 1, 1, 2}}
	for _, b := range bad {
		_, _, err = DecodeFloatGroupsRows(nil, b, 1)
		require.Error(t, err)
		_, _, err = DecodeFloatGroups(nil, b)
		require.Error(t, err)
		_, err = NewFloatGroupsDecoder(b)
		require.Error(t, err)
	}
	// Framing is valid, but the nested Gorilla payload is truncated.
	malformed := []byte{1, 2, 1, 2, 2, 1, 2}
	_, _, err = DecodeFloatGroups(nil, malformed)
	require.Error(t, err)
	d, err := NewFloatGroupsDecoder(malformed)
	require.NoError(t, err)
	_, err = d.Next()
	require.Error(t, err)
	_, err = d.Next()
	require.Error(t, err)
}

func TestFloatGroupsGolden(t *testing.T) {
	t.Parallel()
	var enc FloatGroupEncoder
	data, err := enc.Encode(nil, []float64{1, math.Copysign(0, -1)}, []int{1, 2})
	require.NoError(t, err)
	require.Equal(t, "0102020001083ff00000000000000001088000000000000000", hex.EncodeToString(data))
}

func FuzzFloatGroups(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, uint8(1))
	f.Add([]byte{2, 0, 0, 0, 0, 0, 240, 127}, uint8(3))
	f.Fuzz(func(t *testing.T, raw []byte, width uint8) {
		if len(raw) > 8192 {
			t.Skip()
		}
		var values []float64
		var ends []int
		for len(raw) >= 8 {
			values = append(values, math.Float64frombits(binary.LittleEndian.Uint64(raw)))
			raw = raw[8:]
		}
		step := int(width) + 1
		for i := step; i < len(values); i += step {
			ends = append(ends, i)
		}
		if len(values) > 0 {
			ends = append(ends, len(values))
		}
		var enc FloatGroupEncoder
		data, err := enc.Encode(nil, values, ends)
		require.NoError(t, err)
		got, _, err := DecodeFloatGroups(nil, data)
		require.NoError(t, err)
		floatBitsEqual(t, values, got)
	})
}

func FuzzDecodeFloatGroups(f *testing.F) {
	var enc FloatGroupEncoder
	data, _ := enc.Encode(nil, []float64{1, 2, 3, 0}, []int{3, 4})
	f.Add(data)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8192 {
			t.Skip()
		}
		_, _, _ = DecodeFloatGroups(nil, data)
		d, err := NewFloatGroupsDecoder(data)
		if err != nil {
			return
		}
		for range d.Len() {
			if _, err := d.Next(); err != nil {
				break
			}
		}
	})
}

func oversizedFloatGroupHeader() []byte {
	return append([]byte{1}, binary.AppendUvarint(nil, MaxFloatGroupRows+1)...)
}

//nolint:paralleltest // AllocsPerRun temporarily changes GOMAXPROCS.
func TestFloatGroupScratchReuse(t *testing.T) {
	values := make([]float64, 180)
	for i := range values {
		switch i / 60 {
		case 0:
			values[i] = 42
		case 1:
			values[i] = float64(i)
		default:
			values[i] = math.Sin(float64(i))
		}
	}
	ends := []int{60, 120, 180}
	var enc FloatGroupEncoder
	data, err := enc.Encode(nil, values, ends)
	require.NoError(t, err)
	dst, _, err := DecodeFloatGroups(nil, data)
	require.NoError(t, err)
	allocations := testing.AllocsPerRun(100, func() {
		data, err = enc.Encode(data[:0], values, ends)
		if err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocations, "warmed encoder reuses candidate and conversion scratch")
	allocations = testing.AllocsPerRun(100, func() {
		dst, _, err = DecodeFloatGroups(dst, data)
		if err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocations, "bulk decoding reuses output and stack reader state")
	floatBitsEqual(t, values, dst)
}
