package backend_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/readerpool"
)

type pageDecodeFunc func([]byte) ([]byte, error)

func (f pageDecodeFunc) DecodePage(p []byte) ([]byte, error) { return f(p) }

func TestDecodedPageCache(t *testing.T) {
	t.Parallel()
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "bare", true: "cached"}[cached], func(t *testing.T) {
			t.Parallel()
			b := backend.Memory()
			if cached {
				b = backend.Cached(b, 4096)
			}
			require.NoError(t, b.Write(t.Context(), "part", []byte("abcd")))
			var decodes atomic.Int64
			decode := pageDecodeFunc(func(p []byte) ([]byte, error) { decodes.Add(1); return bytes.ToUpper(p), nil })
			for range 2 {
				v, err := backend.ReadDecodedPage(t.Context(), b, "part", 1, 2, "upper", decode)
				require.NoError(t, err)
				require.Equal(t, "BC", string(v))
			}
			if cached {
				require.EqualValues(t, 1, decodes.Load())
			} else {
				require.EqualValues(t, 2, decodes.Load())
			}
			// Raw pages and different decoders cannot collide with the decoded representation.
			v, err := backend.ReadPageView(t.Context(), b, "part", 1, 2)
			require.NoError(t, err)
			require.Equal(t, "bc", string(v))
			v, err = backend.ReadDecodedPage(t.Context(), b, "part", 1, 2, "lower", pageDecodeFunc(func(p []byte) ([]byte, error) { return bytes.Clone(p), nil }))
			require.NoError(t, err)
			require.Equal(t, "bc", string(v))
			require.NoError(t, b.Write(t.Context(), "part", []byte("wxyz")))
			v, err = backend.ReadDecodedPage(t.Context(), b, "part", 1, 2, "upper", decode)
			require.NoError(t, err)
			require.Equal(t, "XY", string(v))
			require.NoError(t, b.Delete(t.Context(), "part"))
			_, err = backend.ReadDecodedPage(t.Context(), b, "part", 1, 2, "upper", decode)
			require.ErrorIs(t, err, backend.ErrNotExist)
		})
	}
}

func TestDecodedPageConcurrentMissAndFailures(t *testing.T) {
	t.Parallel()
	b := backend.Cached(backend.Memory(), 4096)
	require.NoError(t, b.Write(t.Context(), "part", []byte("abcd")))
	var decodes atomic.Int64
	failure := errors.New("decode failed")
	decode := pageDecodeFunc(func(_ []byte) ([]byte, error) { decodes.Add(1); return nil, failure })
	for range 2 {
		_, err := backend.ReadDecodedPage(t.Context(), b, "part", 0, 4, "upper", decode)
		require.ErrorIs(t, err, failure)
	}
	require.EqualValues(t, 2, decodes.Load())
	decodes.Store(0)
	decode = pageDecodeFunc(func(p []byte) ([]byte, error) { decodes.Add(1); return bytes.ToUpper(p), nil })
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			v, err := backend.ReadDecodedPage(t.Context(), b, "part", 0, 4, "upper", decode)
			if err != nil {
				t.Error(err)
				return
			}
			if string(v) != "ABCD" {
				t.Errorf("got %q", v)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, decodes.Load())
	// Decoded bytes share the same bounded cache as packed pages.
	big := pageDecodeFunc(func([]byte) ([]byte, error) { return make([]byte, 8192), nil })
	_, err := backend.ReadDecodedPage(t.Context(), b, "part", 0, 4, "big", big)
	require.NoError(t, err)
	require.LessOrEqual(t, b.(interface{ Stats() backend.CacheStats }).Stats().Bytes, int64(4096))
}

func TestDecodedPageOperationGenerations(t *testing.T) {
	t.Parallel()
	f, err := file.New(t.TempDir())
	require.NoError(t, err)
	b := backend.Cached(f, 1<<20)
	require.NoError(t, b.Write(t.Context(), "part", []byte("original")))
	scope := readerpool.NewScope()
	t.Cleanup(func() { require.NoError(t, scope.Close()) })
	scope.Acquire()
	defer scope.Release()
	old := scope.Context(t.Context())
	decode := pageDecodeFunc(func(p []byte) ([]byte, error) { return bytes.ToUpper(p), nil })
	read := func(ctx context.Context, want string) {
		t.Helper()
		v, err := backend.ReadDecodedPage(ctx, b, "part", 0, 8, "upper", decode)
		require.NoError(t, err)
		require.Equal(t, want, string(v))
	}
	read(old, "ORIGINAL")
	require.NoError(t, b.Write(t.Context(), "part", []byte("replaced")))
	fresh := scope.Context(t.Context())
	read(fresh, "REPLACED")
	read(old, "ORIGINAL")
	read(fresh, "REPLACED")
	require.NoError(t, f.Write(t.Context(), "part", []byte("restored")))
	latest := scope.Context(t.Context())
	read(latest, "RESTORED")
	read(old, "ORIGINAL")
	read(fresh, "REPLACED")
}
