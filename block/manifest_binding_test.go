package block

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

func TestManifestBindingRoundTrip(t *testing.T) {
	t.Parallel()
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown", true: "known"}[known], func(t *testing.T) {
			t.Parallel()
			m := Manifest{Version: 2, RowCount: 1, BindingNonce: [16]byte{1, 2, 3}}
			if known {
				m.Rollup = &Rollup{}
			}
			data := m.Encode(nil)
			got, err := DecodeManifest(data)
			require.NoError(t, err)
			require.Equal(t, m.BindingNonce, got.BindingNonce)
			require.Equal(t, m.Rollup, got.Rollup)
			require.Equal(t, data, got.Encode(nil))
		})
	}
}

func TestBindManifestPreservesDataAndRename(t *testing.T) {
	t.Parallel()
	b := backend.Memory()
	w, data := samplePartWriter(t)
	require.NoError(t, WritePart(t.Context(), b, "a", w))
	before, err := OpenPart(t.Context(), b, "a")
	require.NoError(t, err)
	digest, err := BindManifest(t.Context(), b, "a")
	require.NoError(t, err)
	digest2, err := BindManifest(t.Context(), b, "a")
	require.NoError(t, err)
	require.Equal(t, digest, digest2, "binding is idempotent before publication")
	raw, err := b.Read(t.Context(), manifestKey("a"))
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(raw), digest)
	after, err := OpenPart(t.Context(), b, "a")
	require.NoError(t, err)
	require.Equal(t, digest, after.ManifestDigest())
	require.NotEqual(t, before.ManifestDigest(), digest)
	col, err := after.Column(t.Context(), "value")
	require.NoError(t, err)
	values, err := col.Float64(nil)
	require.NoError(t, err)
	require.Equal(t, data.value, values)
	require.NoError(t, b.Write(t.Context(), manifestKey("renamed"), raw))
	renamed, err := OpenPart(t.Context(), b, "renamed")
	require.NoError(t, err)
	require.Equal(t, digest, renamed.ManifestDigest(), "binding excludes object path")
	got, err := ManifestDigest(t.Context(), b, "a")
	require.NoError(t, err)
	require.Equal(t, digest, got)
}

func TestBindIdenticalManifestsDistinctParts(t *testing.T) {
	t.Parallel()
	b := backend.Memory()
	for _, prefix := range []string{"a", "b"} {
		w, _ := samplePartWriter(t)
		require.NoError(t, WritePart(t.Context(), b, prefix, w))
	}
	a, err := BindManifest(t.Context(), b, "a")
	require.NoError(t, err)
	other, err := BindManifest(t.Context(), b, "b")
	require.NoError(t, err)
	require.NotEqual(t, a, other)
}
