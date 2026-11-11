package recordengine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

func TestSeparateSeriesCatalogRejectedBeforeMutation(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"load", "readonly", "rebase", "reset"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			be := backend.Memory()
			e := New(Config{Backend: be, Prefix: "tenant/metrics", Schema: NewSchema()})
			ix := &bucketindex.Index{SeriesRoots: []bucketindex.SeriesRoot{{Prefix: "tenant/metrics/s-root", ManifestDigest: [32]byte{1}}}}
			_, err := ix.Save(t.Context(), be, e.indexKey(), "")
			require.NoError(t, err)
			require.NoError(t, be.Write(t.Context(), "tenant/metrics/s-root/manifest", []byte("metadata")))
			before, err := be.Read(t.Context(), e.indexKey())
			require.NoError(t, err)
			keys, err := be.List(t.Context(), "")
			require.NoError(t, err)
			head := e.head
			switch operation {
			case "load":
				err = e.LoadParts(t.Context())
			case "readonly":
				err = e.LoadPartsReadOnly(t.Context())
			case "rebase":
				e.mu.Lock()
				err = e.adoptIndexLocked(t.Context())
				e.mu.Unlock()
			case "reset":
				err = e.Reset(t.Context())
			}
			require.ErrorContains(t, err, "separate series catalogs are unsupported")
			require.Same(t, head, e.head)
			require.Empty(t, e.indexVersion)
			after, err := be.Read(t.Context(), e.indexKey())
			require.NoError(t, err)
			require.Equal(t, before, after)
			afterKeys, err := be.List(t.Context(), "")
			require.NoError(t, err)
			require.ElementsMatch(t, keys, afterKeys)
		})
	}
}
