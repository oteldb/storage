package backend_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/faultbackend"
)

type rangeWithoutSize struct {
	backend.Backend

	reads atomic.Int64
}

func (b *rangeWithoutSize) Read(ctx context.Context, key string) ([]byte, error) {
	b.reads.Add(1)
	return b.Backend.Read(ctx, key)
}

func (b *rangeWithoutSize) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return backend.ReadAt(ctx, b.Backend, key, off, n)
}

type fallbackSizer struct{ backend.Backend }

func (b fallbackSizer) Size(ctx context.Context, key string) (int64, error) {
	return backend.SizeOf(ctx, b.Backend, key)
}

func TestNativeSizeDoesNotInferCapabilities(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"range-only", "fallback-sizer", "cached", "counter", "fault"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			inner := &rangeWithoutSize{Backend: backend.Memory()}
			require.NoError(t, inner.Write(ctx, "p/obj", []byte("payload")))
			var wrapped backend.Backend = inner
			switch name {
			case "fallback-sizer":
				wrapped = fallbackSizer{inner}
			case "cached":
				wrapped = backend.Cached(inner, 1<<20)
			case "counter":
				wrapped = backendtest.NewSizedByteCounter(inner)
			case "fault":
				wrapped = faultbackend.Wrap(inner)
			}
			n, known, err := backend.NativeSizeOf(ctx, wrapped, "p/obj")
			require.NoError(t, err)
			assert.Zero(t, n)
			assert.False(t, known)
			assert.Zero(t, inner.reads.Load(), "size admission must not materialize a value")
			_, err = backend.SizeOf(ctx, wrapped, "p/obj")
			require.NoError(t, err)
			assert.EqualValues(t, 1, inner.reads.Load(), "ordinary SizeOf retains its legacy read fallback")
		})
	}
}

func TestNativeSizeMemoryAndForwarding(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"plain", "cached", "counter", "fault", "streaming"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			inner := backend.Memory()
			require.NoError(t, inner.Write(t.Context(), "p/obj", []byte("payload")))
			require.NoError(t, inner.Write(t.Context(), "p/empty", nil))
			wrapped := inner
			switch name {
			case "cached":
				wrapped = backend.Cached(inner, 1<<20)
			case "counter":
				wrapped = backendtest.NewByteCounter(inner)
			case "fault":
				wrapped = faultbackend.Wrap(inner)
			case "streaming":
				wrapped = backendtest.NewStreamingMemory()
				require.NoError(t, wrapped.Write(t.Context(), "p/obj", []byte("payload")))
				require.NoError(t, wrapped.Write(t.Context(), "p/empty", nil))
			}
			for _, object := range []struct {
				key  string
				size int64
			}{{"p/obj", 7}, {"p/empty", 0}} {
				n, known, err := backend.NativeSizeOf(t.Context(), wrapped, object.key)
				require.NoError(t, err)
				assert.True(t, known)
				assert.Equal(t, object.size, n)
			}
			_, _, err := backend.NativeSizeOf(t.Context(), wrapped, "absent")
			require.ErrorIs(t, err, backend.ErrNotExist)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, _, err = backend.NativeSizeOf(ctx, wrapped, "p/obj")
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

type keyedNativeSizer struct{ backend.Backend }

var errNativeSize = errors.New("native metadata unavailable")

func (b keyedNativeSizer) NativeSize(_ context.Context, key string) (int64, bool, error) {
	if key == "known" {
		return 42, true, nil
	}
	if key == "error" {
		return 0, true, errNativeSize
	}
	return 0, false, nil
}

func TestNativeSizePerKeyAndErrors(t *testing.T) {
	t.Parallel()
	wrapped := backend.Cached(keyedNativeSizer{Backend: backend.Memory()}, 1<<20)
	n, known, err := backend.NativeSizeOf(t.Context(), wrapped, "known")
	require.NoError(t, err)
	assert.True(t, known)
	assert.EqualValues(t, 42, n)
	require.NoError(t, wrapped.Write(t.Context(), "unknown", []byte("cached")))
	_, known, err = backend.NativeSizeOf(t.Context(), wrapped, "unknown")
	require.NoError(t, err)
	assert.False(t, known, "a resident cache entry must not invent an inner metadata capability")
	_, _, err = backend.NativeSizeOf(t.Context(), wrapped, "error")
	require.ErrorIs(t, err, errNativeSize)
}
