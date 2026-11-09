package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/internal/readerpool"
)

func TestCachedMeteredReadersKeepOperationGenerations(t *testing.T) {
	t.Parallel()
	for _, order := range []string{"cache-outside", "meter-outside"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			f, err := file.New(t.TempDir())
			require.NoError(t, err)
			o, _ := obstest.New(t)
			b := backend.Cached(instrumentBackend(f, o.Backend), 1<<20)
			if order == "meter-outside" {
				b = instrumentBackend(backend.Cached(f, 1<<20), o.Backend)
			}
			ctx := t.Context()
			require.NoError(t, b.Write(ctx, "part/c/0", []byte("original")))
			scope := readerpool.NewScope()
			defer func() { require.NoError(t, scope.Close()) }()
			scope.Acquire()
			defer scope.Release()
			old := scope.Context(ctx)
			v, err := backend.ReadView(old, b, "part/c/0")
			require.NoError(t, err)
			require.Equal(t, "original", string(v))
			require.NoError(t, b.Write(ctx, "part/c/0", []byte("replaced")))
			fresh := scope.Context(ctx)
			v, err = backend.ReadView(fresh, b, "part/c/0")
			require.NoError(t, err)
			require.Equal(t, "replaced", string(v))
			for _, read := range []func() ([]byte, error){
				func() ([]byte, error) { return backend.ReadAt(old, b, "part/c/0", 0, 8) },
				func() ([]byte, error) { return backend.ReadViewAt(old, b, "part/c/0", 0, 8) },
				func() ([]byte, error) { return backend.ReadAtAppend(old, b, nil, "part/c/0", 0, 8) },
				func() ([]byte, error) { return backend.ReadPageView(old, b, "part/c/0", 0, 8) },
			} {
				v, err = read()
				require.NoError(t, err)
				require.Equal(t, "original", string(v))
			}
		})
	}
}
