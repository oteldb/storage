package storage

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs"
)

type nativeRangeNoSizer struct {
	backend.Backend

	reads atomic.Int64
}

func (b *nativeRangeNoSizer) Read(ctx context.Context, key string) ([]byte, error) {
	b.reads.Add(1)
	return b.Backend.Read(ctx, key)
}

func (b *nativeRangeNoSizer) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return backend.ReadAt(ctx, b.Backend, key, off, n)
}

func TestInstrumentedNativeSizeDoesNotRead(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"instrumented", "cached/instrumented", "instrumented/cached"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			inner := &nativeRangeNoSizer{Backend: backend.Memory()}
			require.NoError(t, inner.Write(t.Context(), "p/sidx", []byte("payload")))
			wrapped := instrumentBackend(inner, obs.NewNop().Backend)
			switch name {
			case "cached/instrumented":
				wrapped = backend.Cached(wrapped, 1<<20)
			case "instrumented/cached":
				wrapped = instrumentBackend(backend.Cached(inner, 1<<20), obs.NewNop().Backend)
			}
			require.True(t, backend.RangesNatively(t.Context(), wrapped, "p/sidx"))
			_, exposesSizer := wrapped.(backend.Sizer)
			require.True(t, exposesSizer)
			_, known, err := backend.NativeSizeOf(t.Context(), wrapped, "p/sidx")
			require.NoError(t, err)
			assert.False(t, known)
			assert.Zero(t, inner.reads.Load(), "cache and instrumentation must not materialize an unsized ranged object")
			_, err = backend.SizeOf(t.Context(), wrapped, "p/sidx")
			require.NoError(t, err)
			assert.EqualValues(t, 1, inner.reads.Load(), "legacy Size still has its explicit whole-read fallback")
		})
	}
}
