package backend_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
)

type writerMemoryBackend struct {
	backend.Backend

	bytes int64
	known bool
}

func (b writerMemoryBackend) WriterMemory(string) (int64, bool) { return b.bytes, b.known }

func TestWriterMemory(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		bytes int64
		known bool
		want  bool
	}{
		{"known", 4096, true, true},
		{"zero", 0, true, true},
		{"unknown", 4096, false, false},
		{"negative", -1, true, false},
		{"saturated", math.MaxInt64, true, true},
		{"key saturated", math.MaxInt64 - 48, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			inner := writerMemoryBackend{Backend: backend.Memory(), bytes: tt.bytes, known: tt.known}
			for i, b := range []backend.Backend{inner, backend.Cached(inner, 4096)} {
				n, known := backend.WriterMemory(b, "part/identity")
				require.Equal(t, tt.want, known)
				if known {
					if i == 0 || tt.bytes == math.MaxInt64 {
						require.Equal(t, tt.bytes, n)
					} else {
						require.Greater(t, n, tt.bytes, "the cache wrapper owns invalidation metadata")
					}
				} else {
					require.Zero(t, n)
				}
			}
		})
	}
	n, known := backend.WriterMemory(backend.Memory(), "part/identity")
	require.False(t, known)
	require.Zero(t, n)
}
