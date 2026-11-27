package wal

import (
	"bytes"
	"encoding/hex"
	"math"
	"strconv"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

const packedTestTimestamp = int64(1_700_000_000_000_000_000)

func TestPackedReferencesRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		mode packedTimestampMode
		ts   []int64
	}{
		{"shared timestamp", packedSame, []int64{packedTestTimestamp, packedTestTimestamp, packedTestTimestamp}},
		{"signed deltas", packedDelta, []int64{packedTestTimestamp, packedTestTimestamp - 100, packedTestTimestamp + 100}},
		{"extreme modular deltas", packedDelta, []int64{math.MinInt64, math.MaxInt64, math.MinInt64}},
		{"raw", packedRaw, []int64{packedTestTimestamp, -packedTestTimestamp, math.MinInt64, math.MaxInt64}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			values := []uint64{0x7ff0000000000002, 1 << 63, math.MaxUint64, 1}
			ids := []signal.SeriesID{{Hi: 1, Lo: 2}, {Hi: 2, Lo: 2}, {Hi: 3, Lo: 2}, {Hi: 4, Lo: 2}}[:len(tc.ts)]
			var buf bytes.Buffer
			packed, err := NewWriter(&buf).WritePackedReferences(ids, func(i int) (int64, float64) {
				return tc.ts[i], math.Float64frombits(values[i])
			})
			require.NoError(t, err)
			require.True(t, packed)
			typ, payload, consumed, err := readFrame(buf.Bytes())
			require.NoError(t, err)
			require.Equal(t, recordPackedReferences, typ)
			require.Equal(t, byte(tc.mode), payload[0])
			require.Equal(t, buf.Len(), consumed)
			n := 0
			require.NoError(t, Replay(buf.Bytes(), Handlers{
				OnPackedReference: func(id signal.SeriesID, ts int64, value float64) error {
					require.Less(t, n, len(ids))
					require.Equal(t, ids[n], id)
					require.Equal(t, tc.ts[n], ts)
					require.Equal(t, values[n], math.Float64bits(value))
					n++
					return nil
				},
			}))
			require.Equal(t, len(ids), n)
		})
	}
}

func TestPackedReferencesEncoding(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	packed, err := NewWriter(&buf).WritePackedReferences(
		[]signal.SeriesID{{Hi: 1, Lo: 2}, {Hi: 3, Lo: 4}},
		func(i int) (int64, float64) {
			return packedTestTimestamp, []float64{1, math.Float64frombits(1 << 63)}[i]
		},
	)
	require.NoError(t, err)
	require.True(t, packed)
	require.Equal(t,
		"3b080002000000000000000100000000000000023ff000000000000000000000000000030000000000000004800000000000000017979cfe362a0000ff49e3a9",
		hex.EncodeToString(buf.Bytes()))
	require.ErrorIs(t, Replay(buf.Bytes(), Handlers{}), ErrCorrupt)
}

func TestPackedReferencesFallback(t *testing.T) {
	t.Parallel()
	for _, n := range []int{0, 1, 2, MaxPackedReferences + 1} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			packed, err := NewWriter(&buf).WritePackedReferences(make([]signal.SeriesID, n), func(int) (int64, float64) {
				return 0, 1
			})
			require.NoError(t, err)
			require.False(t, packed)
			require.Zero(t, buf.Len())
		})
	}
}

func TestPackedReferencesSalvage(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	packed, err := NewWriter(&buf).WritePackedReferences(
		[]signal.SeriesID{{Hi: 1}, {Hi: 2}, {Hi: 3}},
		func(i int) (int64, float64) { return packedTestTimestamp + int64(i), 42 },
	)
	require.NoError(t, err)
	require.True(t, packed)
	good := buf.Bytes()
	_, payload, _, err := readFrame(good)
	require.NoError(t, err)

	n := 0
	h := Handlers{OnPackedReference: func(signal.SeriesID, int64, float64) error {
		n++
		return nil
	}}
	for cut := 1; cut < len(good); cut++ {
		require.ErrorIs(t, Replay(good[:cut], h), ErrCorrupt)
		require.Zero(t, n)
	}

	for i := range payload {
		invalid := appendFrame(nil, recordPackedReferences, payload[:i])
		damaged := 0
		n = 0
		_, err := walk(append(invalid, good...), &h, func(_, _ int, err error) error {
			require.ErrorIs(t, err, ErrCorrupt)
			require.Zero(t, n)
			damaged++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 1, damaged)
		require.Equal(t, 3, n)
	}

	failed := errors.New("missing descriptor")
	h.OnPackedReference = func(signal.SeriesID, int64, float64) error { return failed }
	_, err = walk(good, &h, func(_, _ int, _ error) error {
		t.Fatal("descriptor failures stop recovery")
		return nil
	})
	require.ErrorIs(t, err, failed)
}

func FuzzPackedReferences(f *testing.F) {
	ids := []signal.SeriesID{{Hi: 1}, {Hi: 2}, {Hi: 3}}
	for _, timestamps := range [][3]int64{
		{packedTestTimestamp, packedTestTimestamp, packedTestTimestamp},
		{packedTestTimestamp, packedTestTimestamp - 10, packedTestTimestamp + 10},
		{packedTestTimestamp, -packedTestTimestamp, math.MaxInt64},
	} {
		var buf bytes.Buffer
		packed, err := NewWriter(&buf).WritePackedReferences(ids, func(i int) (int64, float64) {
			return timestamps[i], 42
		})
		if !packed || err != nil {
			f.Fatalf("build corpus: %t %v", packed, err)
		}
		_, payload, _, err := readFrame(buf.Bytes())
		if err != nil {
			f.Fatal(err)
		}
		f.Add(payload)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		n := 0
		err := dispatchPackedReferences(data, Handlers{
			OnPackedReference: func(signal.SeriesID, int64, float64) error { n++; return nil },
		})
		if err != nil && n != 0 {
			t.Fatal("malformed block dispatched rows")
		}
		if err == nil && (n < 2 || n > MaxPackedReferences) {
			t.Fatalf("invalid accepted row count %d", n)
		}
	})
}
