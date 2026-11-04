package chunk

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdaptiveTimestamps(t *testing.T) {
	t.Parallel()
	for _, values := range [][]int64{nil, {1}, {1, 2}, {3, 3, 3}, {1, 16, 31, 46}, {1, 4, 9, 17}, {math.MaxInt64, math.MinInt64}, {math.MinInt64, 0, math.MaxInt64}} {
		data, err := EncodeTimestampsAdaptive(nil, values)
		require.NoError(t, err)
		got, n, err := DecodeTimestampsAdaptiveRows(nil, data, len(values))
		require.NoError(t, err)
		require.Equal(t, len(data), n)
		require.Equal(t, values, got)
		cursor, err := NewTsCursor(CodecAdaptiveTimestamps, data)
		require.NoError(t, err)
		require.Equal(t, len(values), cursor.Len())
		for i, v := range values {
			got, err := cursor.Next()
			require.NoError(t, err)
			require.Equal(t, v, got)
			require.Equal(t, i+1, cursor.Pos())
		}
		_, err = cursor.Next()
		require.Error(t, err)
		_, _, err = DecodeTimestampsAdaptiveRows(nil, data, len(values)+1)
		require.Error(t, err)
		for i := range data {
			_, _, err = DecodeTimestampsAdaptive(nil, data[:i])
			require.Error(t, err)
		}
		_, _, err = DecodeTimestampsAdaptive(nil, append(data, 0))
		require.Error(t, err)
	}
	data, err := EncodeTimestampsAdaptive(nil, []int64{1, 16, 31})
	require.NoError(t, err)
	require.Equal(t, "0003021e", hex.EncodeToString(data))
	for _, bad := range [][]byte{nil, {2, 0}, {0, 128}, {0, 1}, {0, 1, 0}, {0, 0, 0}, {1, 0}, {1, 255, 255}} {
		_, _, err = DecodeTimestampsAdaptive(nil, bad)
		require.Error(t, err)
		_, err = NewAdaptiveTimestampCursor(bad)
		require.Error(t, err)
	}
}

func FuzzAdaptiveTimestamps(f *testing.F) {
	f.Add([]byte{1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 8192 {
			t.Skip()
		}
		var values []int64
		for len(raw) >= 8 {
			values = append(values, int64(binary.LittleEndian.Uint64(raw)))
			raw = raw[8:]
		}
		data, err := EncodeTimestampsAdaptive(nil, values)
		require.NoError(t, err)
		got, _, err := DecodeTimestampsAdaptive(nil, data)
		require.NoError(t, err)
		require.Equal(t, values, got)
		c, err := NewAdaptiveTimestampCursor(data)
		require.NoError(t, err)
		for _, v := range values {
			got, err := c.Next()
			require.NoError(t, err)
			require.Equal(t, v, got)
		}
	})
}

func TestAdaptiveTimestampCursorRejectsTrailingFallback(t *testing.T) {
	t.Parallel()
	data, err := EncodeTimestampsAdaptive(nil, []int64{1, 4, 9, 17})
	require.NoError(t, err)
	require.Equal(t, byte(1), data[0])
	data = append(data, 0)
	cursor, err := NewAdaptiveTimestampCursor(data)
	require.NoError(t, err)
	for range 3 {
		_, err = cursor.Next()
		require.NoError(t, err)
	}
	_, err = cursor.Next()
	require.Error(t, err)
	_, err = cursor.Next()
	require.Error(t, err)
}

func FuzzDecodeAdaptiveTimestamps(f *testing.F) {
	data, _ := EncodeTimestampsAdaptive(nil, []int64{1, 4, 9, 17})
	f.Add(data)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8192 {
			t.Skip()
		}
		_, _, _ = DecodeTimestampsAdaptive(nil, data)
		cursor, err := NewAdaptiveTimestampCursor(data)
		if err != nil {
			return
		}
		for range min(cursor.Len(), 2048) {
			if _, err = cursor.Next(); err != nil {
				break
			}
		}
	})
}
