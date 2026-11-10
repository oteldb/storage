package chunk

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:paralleltest,tparallel // Subtests exercise consecutive resets on one decoder.
func TestFloatGroupsDecoderReset(t *testing.T) {
	t.Parallel()
	var encoder FloatGroupEncoder
	var decoder FloatGroupsDecoder
	for _, tc := range []struct {
		name   string
		values []float64
	}{
		{"decimal", []float64{1, 2, 3, 4}},
		{"constant", []float64{7, 7}},
		{"special", []float64{math.Copysign(0, -1), math.Inf(1), math.Float64frombits(0x7ff8000000004321)}},
		{"empty", nil},
		{"decimal_again", []float64{3, 4, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ends []int
			if len(tc.values) > 0 {
				ends = []int{len(tc.values)}
			}
			data, err := encoder.Encode(nil, tc.values, ends)
			require.NoError(t, err)
			capacity := cap(decoder.values)
			var previous *float64
			if capacity > 0 {
				previous = &decoder.values[:capacity][0]
			}
			require.NoError(t, decoder.Reset(data))
			require.Zero(t, decoder.Pos())
			require.Equal(t, len(tc.values), decoder.Len())
			for _, want := range tc.values {
				got, err := decoder.Next()
				require.NoError(t, err)
				require.Equal(t, math.Float64bits(want), math.Float64bits(got))
			}
			_, err = decoder.Next()
			require.Error(t, err)
			if capacity >= len(tc.values) && previous != nil {
				require.Same(t, previous, &decoder.values[:cap(decoder.values)][0])
			}
			require.Error(t, decoder.Reset(nil))
			require.Zero(t, decoder.Len())
			_, err = decoder.Next()
			require.Error(t, err)
		})
	}
	// A valid frame can fail later inside its nested codec payload.
	require.NoError(t, decoder.Reset([]byte{1, 2, 1, 2, 2, 1, 2}))
	_, err := decoder.Next()
	require.Error(t, err)
	data, err := encoder.Encode(nil, []float64{3, 4}, []int{2})
	require.NoError(t, err)
	require.NoError(t, decoder.Reset(data))
	got, err := decoder.Next()
	require.NoError(t, err)
	require.Equal(t, math.Float64bits(3), math.Float64bits(got))
	// Reset also discards a partially consumed group.
	require.NoError(t, decoder.Reset(data))
	got, err = decoder.Next()
	require.NoError(t, err)
	require.Equal(t, math.Float64bits(3), math.Float64bits(got))
	old := data
	data, err = encoder.Encode(nil, []float64{7, 8}, []int{2})
	require.NoError(t, err)
	require.NoError(t, decoder.Reset(data))
	clear(old)
	for _, want := range []float64{7, 8} {
		got, err := decoder.Next()
		require.NoError(t, err)
		require.Equal(t, math.Float64bits(want), math.Float64bits(got))
	}
}

func TestAdaptiveTimestampCursorReset(t *testing.T) {
	t.Parallel()
	var cursor adaptiveTimestampCursor
	for _, values := range [][]int64{{1, 4, 9, 17}, {1, 16, 31}, nil, {math.MinInt64, 0, math.MaxInt64}, {7, 7}, {1, 2, 4}} {
		data, err := EncodeTimestampsAdaptive(nil, values)
		require.NoError(t, err)
		previous := cursor.fallback
		require.NoError(t, cursor.Reset(data))
		require.Zero(t, cursor.Pos())
		if previous != nil {
			require.Same(t, previous, cursor.fallback)
		}
		for _, want := range values {
			got, err := cursor.Next()
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
		_, err = cursor.Next()
		require.Error(t, err)
		require.Error(t, cursor.Reset(nil))
		require.Zero(t, cursor.Len())
		_, err = cursor.Next()
		require.Error(t, err)
	}
	fallback, err := EncodeTimestampsAdaptive(nil, []int64{1, 4, 9, 17})
	require.NoError(t, err)
	require.NoError(t, cursor.Reset(append(fallback, 0)))
	for range 3 {
		_, err = cursor.Next()
		require.NoError(t, err)
	}
	_, err = cursor.Next()
	require.Error(t, err)
	require.NoError(t, cursor.Reset(fallback))
	got, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
	require.NoError(t, cursor.Reset(fallback))
	got, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
	old := fallback
	fallback, err = EncodeTimestampsAdaptive(nil, []int64{2, 5, 10, 18})
	require.NoError(t, err)
	require.NoError(t, cursor.Reset(fallback))
	clear(old)
	for _, want := range []int64{2, 5, 10, 18} {
		got, err := cursor.Next()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func FuzzMetricCursorReset(f *testing.F) {
	var encoder FloatGroupEncoder
	floats, _ := encoder.Encode(nil, []float64{1, math.Copysign(0, -1), math.Pi}, []int{3})
	regular, _ := EncodeTimestampsAdaptive(nil, []int64{1, 16, 31})
	fallback, _ := EncodeTimestampsAdaptive(nil, []int64{1, 4, 9, 17})
	f.Add(floats, regular)
	f.Add([]byte{1, 2, 1, 2, 2, 1, 2}, fallback)
	f.Add([]byte{}, []byte{})
	f.Fuzz(func(t *testing.T, floatData, timestampData []byte) {
		if len(floatData)+len(timestampData) > 8192 {
			t.Skip()
		}
		var reusedFloat FloatGroupsDecoder
		require.NoError(t, reusedFloat.Reset(floats))
		_, err := reusedFloat.Next()
		require.NoError(t, err)
		err = reusedFloat.Reset(floatData)
		freshFloat, freshErr := NewFloatGroupsDecoder(floatData)
		require.Equal(t, freshErr != nil, err != nil)
		if err == nil {
			require.Equal(t, freshFloat.Len(), reusedFloat.Len())
			for range min(reusedFloat.Len(), 4096) {
				want, wantErr := freshFloat.Next()
				got, gotErr := reusedFloat.Next()
				require.Equal(t, wantErr != nil, gotErr != nil)
				if wantErr != nil {
					break
				}
				require.Equal(t, math.Float64bits(want), math.Float64bits(got))
			}
		}
		require.NoError(t, reusedFloat.Reset(floats))
		got, err := reusedFloat.Next()
		require.NoError(t, err)
		require.Equal(t, math.Float64bits(1), math.Float64bits(got))

		var reusedTimestamp adaptiveTimestampCursor
		require.NoError(t, reusedTimestamp.Reset(fallback))
		_, err = reusedTimestamp.Next()
		require.NoError(t, err)
		err = reusedTimestamp.Reset(timestampData)
		freshTimestamp, freshErr := NewAdaptiveTimestampCursor(timestampData)
		require.Equal(t, freshErr != nil, err != nil)
		if err == nil {
			require.Equal(t, freshTimestamp.Len(), reusedTimestamp.Len())
			for range min(reusedTimestamp.Len(), 4096) {
				want, wantErr := freshTimestamp.Next()
				got, gotErr := reusedTimestamp.Next()
				require.Equal(t, wantErr != nil, gotErr != nil)
				if wantErr != nil {
					break
				}
				require.Equal(t, want, got)
			}
		}
		require.NoError(t, reusedTimestamp.Reset(regular))
		v, err := reusedTimestamp.Next()
		require.NoError(t, err)
		require.Equal(t, int64(1), v)
	})
}
