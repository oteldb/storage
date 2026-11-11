package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeriesReference(t *testing.T) {
	t.Parallel()
	digest := [32]byte{1, 2, 3}
	for _, tc := range []struct {
		name   string
		change func([]byte) []byte
		valid  bool
	}{
		{"valid", func(b []byte) []byte { return b }, true},
		{"truncated", func(b []byte) []byte { return b[:39] }, false},
		{"extra", func(b []byte) []byte { return append(b, 0) }, false},
		{"magic", func(b []byte) []byte { b[0] ^= 1; return b }, false},
		{"digest", func(b []byte) []byte { b[4] ^= 1; return b }, false},
		{"checksum", func(b []byte) []byte { b[39] ^= 1; return b }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.valid, validSeriesReference(tc.change(encodeSeriesReference(digest)), digest))
		})
	}
	require.False(t, validSeriesReference(encodeSeriesReference(digest), [32]byte{5}))
}

func FuzzSeriesReference(f *testing.F) {
	digest := [32]byte{1, 2, 3}
	f.Add(encodeSeriesReference(digest))
	f.Add([]byte{})
	f.Add([]byte("SREF"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if validSeriesReference(raw, digest) {
			require.Equal(t, encodeSeriesReference(digest), raw)
		}
	})
}
