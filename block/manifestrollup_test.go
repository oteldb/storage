package block

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/bitstream"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

func sampleRollup() *Rollup {
	return &Rollup{Tiers: []RollupTier{
		{Before: 7200, Interval: 300, Agg: 4},
		{Before: -5, Interval: 60, Agg: 0},
	}}
}

func markedManifest() Manifest {
	m := sampleManifest()
	m.DiskBytes, m.RawBytes = 4096, 65536
	m.Rollup = sampleRollup()

	return m
}

// withCRC appends a valid CRC to body, so decoding reaches the field parsing.
func withCRC(body []byte) []byte {
	return binary.BigEndian.AppendUint32(append([]byte(nil), body...), crc32.Checksum(body, castagnoli))
}

// TestManifestGoldenRollup pins the marker's bytes: a known layout, and a known empty one, after RawBytes.
func TestManifestGoldenRollup(t *testing.T) {
	t.Parallel()

	m := Manifest{
		Version: 1, RowCount: 2, MinTime: 100, MaxTime: 200, GranuleSize: 8192,
		Columns: []ColumnDesc{{
			Name: "ts", Kind: KindInt64, Codec: chunk.CodecDoD,
			Compress: compress.AlgorithmNone, MinInt64: 100, MaxInt64: 200,
		}},
		Rollup: &Rollup{Tiers: []RollupTier{{Before: 150, Interval: 10, Agg: 5}}},
	}

	for _, tc := range []struct {
		name   string
		rollup *Rollup
		golden string
	}{
		{"layout", m.Rollup, "4f54504d0102c801900380400102747300010000c8019003000001ac020a0560dea8e6"},
		{"raw", &Rollup{}, "4f54504d0102c801900380400102747300010000c8019003000000980b61e6"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := m
			m.Rollup = tc.rollup
			assert.Equal(t, tc.golden, hex.EncodeToString(m.Encode(nil)))

			raw, err := hex.DecodeString(tc.golden)
			require.NoError(t, err)
			got, err := DecodeManifest(raw)
			require.NoError(t, err)
			assert.Equal(t, m, got)
		})
	}
}

// TestManifestUnmarkedIsUnknown pins that every manifest written without the marker (the goldens of
// each earlier layout, and the committed v2 part) decodes as unknown, never as raw.
func TestManifestUnmarkedIsUnknown(t *testing.T) {
	t.Parallel()

	for _, golden := range []string{
		"4f54504d0102c801900380400102747300010000c80190030000d19bb2b9",
		"4f54504d0102c801900380400102747300010000c80190032cc1aa66",
		"4f54504d0102c801900380400102747300010000c8019003001536e671",
	} {
		raw, err := hex.DecodeString(golden)
		require.NoError(t, err)
		got, err := DecodeManifest(raw)
		require.NoError(t, err)
		assert.Nil(t, got.Rollup, golden)
	}

	got, err := DecodeManifest(v3Manifest().Encode(nil))
	require.NoError(t, err)
	assert.Nil(t, got.Rollup, "v3")

	b := backend.Memory()
	loadV2Fixture(t, b)

	r, err := OpenPart(context.Background(), b, "p")
	require.NoError(t, err)
	assert.Nil(t, r.Manifest().Rollup, "v2 fixture")
}

// TestManifestRollupTruncationSweep cuts the body inside the marker: each prefix is a valid manifest
// whose marker reads as unknown, with every earlier field intact.
func TestManifestRollupTruncationSweep(t *testing.T) {
	t.Parallel()

	unmarked := markedManifest()
	unmarked.Rollup = nil
	start := len(unmarked.Encode(nil)) - 4

	full := markedManifest().Encode(nil)
	body := full[:len(full)-4]
	require.Greater(t, len(body), start)

	want := checkedColumns(unmarked)

	for n := start; n < len(body); n++ {
		got, err := DecodeManifest(withCRC(body[:n]))
		require.NoErrorf(t, err, "prefix len %d", n)
		require.Equalf(t, want, got, "prefix len %d", n)
	}

	got, err := DecodeManifest(full)
	require.NoError(t, err)
	assert.Equal(t, checkedColumns(markedManifest()), got)
}

// TestManifestMalformedRollupIsUnknown covers markers no writer produces: each decodes as unknown
// rather than failing the part.
func TestManifestMalformedRollupIsUnknown(t *testing.T) {
	t.Parallel()

	enc := sampleManifest().Encode(nil)
	prefix := enc[:len(enc)-4]

	marker := func(n uint64, tiers func(w *bitstream.Writer)) []byte {
		w := bitstream.NewWriter(append([]byte(nil), prefix...))
		w.WriteUvarint(n)

		if tiers != nil {
			tiers(w)
		}

		w.PadToByte()

		return withCRC(w.Bytes())
	}
	tier := func(before int64, interval uint64, agg byte) func(w *bitstream.Writer) {
		return func(w *bitstream.Writer) {
			w.WriteVarint(before)
			w.WriteUvarint(interval)
			_ = w.WriteByte(agg)
		}
	}

	for _, tc := range []struct {
		name string
		src  []byte
	}{
		{"count past the body", marker(1<<62, nil)},
		{"count past the tiers", marker(2, tier(10, 5, 0))},
		{"zero interval", marker(1, tier(10, 0, 0))},
		{"interval past MaxInt64", marker(1, tier(10, math.MaxInt64+1, 0))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := DecodeManifest(tc.src)
			require.NoError(t, err)
			assert.Equal(t, checkedColumns(sampleManifest()), got)
		})
	}
}

// TestWithRollup checks both writers stamp the marker, and that without it the manifest has none.
func TestWithRollup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ts := []int64{1, 2, 3}

	for _, tc := range []struct {
		name string
		opts []PartOption
		want *Rollup
	}{
		{"unmarked", nil, nil},
		{"raw", []PartOption{WithRollup(Rollup{})}, &Rollup{}},
		{"layout", []PartOption{WithRollup(*sampleRollup())}, sampleRollup()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := backend.Memory()

			pw := NewPartWriter(tc.opts...)
			require.NoError(t, pw.AddColumn(Column{Name: "ts", Kind: KindInt64, Int64: ts, Block: true}))
			require.NoError(t, WritePart(ctx, b, "batch", pw))

			sw := NewStreamWriter(tc.opts...)
			require.NoError(t, sw.AddColumn(Column{Name: "ts", Kind: KindInt64, Block: true}))
			require.NoError(t, sw.AppendInt64(0, ts))
			require.NoError(t, WriteStreamPart(ctx, b, "stream", sw))

			for _, prefix := range []string{"batch", "stream"} {
				r, err := OpenPart(ctx, b, prefix)
				require.NoError(t, err)
				assert.Equal(t, tc.want, r.Manifest().Rollup, prefix)
			}
		})
	}
}

// FuzzManifestRollupRoundTrip checks Encode∘Decode is the identity for every known marker.
func FuzzManifestRollupRoundTrip(f *testing.F) {
	f.Add(uint8(0), int64(0), int64(1), uint8(0))
	f.Add(uint8(1), int64(7200), int64(60), uint8(4))
	f.Add(uint8(3), int64(-1), int64(math.MaxInt64), uint8(255))
	f.Add(uint8(4), int64(math.MinInt64), int64(3600), uint8(6))

	f.Fuzz(func(t *testing.T, n uint8, before, interval int64, agg uint8) {
		if interval <= 0 {
			return
		}

		r := &Rollup{}
		for i := range int64(n % 16) {
			r.Tiers = append(r.Tiers, RollupTier{
				Before:   before ^ i<<40,
				Interval: max(interval>>i, 1),
				Agg:      agg + uint8(i),
			})
		}

		m := sampleManifest()
		m.Rollup = r

		got, err := DecodeManifest(m.Encode(nil))
		require.NoError(t, err)
		assert.Equal(t, checkedColumns(m), got)
	})
}
