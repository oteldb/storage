package block

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/bitstream"
)

func TestBindingTrailerPreservesLegacyRollupDecoder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		rollup *Rollup
	}{
		{"unknown", nil}, {"raw", &Rollup{}}, {"downsampled", &Rollup{Tiers: []RollupTier{{Before: 100, Interval: 10, Agg: 1}, {Before: 200, Interval: 20, Agg: 2}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := Manifest{Version: 2, RowCount: 1, Rollup: tc.rollup, BindingNonce: [16]byte{1, 2, 3}}
			raw := m.Encode(nil)
			prefix := m
			prefix.Rollup = nil
			prefix.BindingNonce = [16]byte{}
			offset := len(prefix.Encode(nil)) - 4
			got := legacyBindingRollup(bitstream.NewReader(raw[offset : len(raw)-4]))
			require.Equal(t, tc.rollup, got)
		})
	}
}

// Frozen rollup decoder from before the binding trailer existed.
func legacyBindingRollup(r *bitstream.Reader) *Rollup {
	n, err := r.ReadUvarint()
	if err != nil || n > uint64(r.Remaining()/3) {
		return nil
	}
	if n == 0 {
		return &Rollup{}
	}
	tiers := make([]RollupTier, n)
	for i := range tiers {
		before, err := r.ReadVarint()
		if err != nil {
			return nil
		}
		interval, err := r.ReadUvarint()
		if err != nil || interval == 0 || interval > math.MaxInt64 {
			return nil
		}
		agg, err := r.ReadByte()
		if err != nil {
			return nil
		}
		tiers[i] = RollupTier{Before: before, Interval: int64(interval), Agg: agg}
	}
	return &Rollup{Tiers: tiers}
}

func TestBindingNonceSurvivesUnknownTrailer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		rollup *Rollup
	}{
		{"unknown", nil}, {"raw", &Rollup{}}, {"downsampled", &Rollup{Tiers: []RollupTier{{Before: 100, Interval: 10, Agg: 1}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := Manifest{Version: 2, RowCount: 1, Rollup: tc.rollup, BindingNonce: [16]byte{1, 2, 3}}
			raw := m.Encode(nil)
			raw = append(raw[:len(raw)-4], 0xff)
			raw = binary.BigEndian.AppendUint32(raw, crc32.Checksum(raw, castagnoli))
			got, err := DecodeManifest(raw)
			require.NoError(t, err)
			require.Equal(t, m.BindingNonce, got.BindingNonce)
			require.Equal(t, m.Rollup, got.Rollup)
			b := backend.Memory()
			require.NoError(t, b.Write(t.Context(), manifestKey("p"), raw))
			digest, err := BindManifest(t.Context(), b, "p")
			require.NoError(t, err)
			require.Equal(t, sha256.Sum256(raw), digest)
			after, err := b.Read(t.Context(), manifestKey("p"))
			require.NoError(t, err)
			require.Equal(t, raw, after, "rebinding must preserve unknown trailing fields without another nonce")
		})
	}
}
