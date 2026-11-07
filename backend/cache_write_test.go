package backend_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
)

func TestCacheWritesDoNotPopulate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		write func(context.Context, backend.Backend, []byte) error
	}{
		{"write", func(ctx context.Context, b backend.Backend, data []byte) error { return b.Write(ctx, "k", data) }},
		{"deferred", func(ctx context.Context, b backend.Backend, data []byte) error {
			return backend.WriteDeferred(ctx, b, "k", data)
		}},
		{"put-if-absent", func(ctx context.Context, b backend.Backend, data []byte) error {
			_, err := b.PutIfAbsent(ctx, "k", data)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			inner := newCounting()
			c := backend.Cached(inner, 1<<20)
			stats := c.(interface{ Stats() backend.CacheStats })
			data := []byte("old")
			require.NoError(t, tc.write(ctx, c, data))
			assert.Zero(t, stats.Stats().Items)
			data[0] = 'X'
			old, err := backend.ReadView(ctx, c, "k")
			require.NoError(t, err)
			require.Equal(t, []byte("old"), old)
			assert.Equal(t, 1, stats.Stats().Items)
			// Leave a stale cached generation to exercise successful conditional insertion too.
			require.NoError(t, inner.Delete(ctx, "k"))
			data = []byte("new")
			require.NoError(t, tc.write(ctx, c, data))
			assert.Zero(t, stats.Stats().Items)
			data[0] = 'Y'
			assert.Equal(t, []byte("old"), old)
			for range 2 {
				got, err := backend.ReadView(ctx, c, "k")
				require.NoError(t, err)
				assert.Equal(t, []byte("new"), got)
			}
			assert.Equal(t, int64(2), inner.reads.Load())
		})
	}
}

func TestCacheUncommittedWritesKeepResidentValue(t *testing.T) {
	t.Parallel()
	for _, kind := range []faultbackend.Kind{faultbackend.Write, faultbackend.PutIfAbsent} {
		t.Run(kind.String(), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			inner := faultbackend.Wrap(backend.Memory())
			c := backend.Cached(inner, 1<<20)
			require.NoError(t, c.Write(ctx, "k", []byte("old")))
			old, err := backend.ReadView(ctx, c, "k")
			require.NoError(t, err)
			inner.Add(faultbackend.Rule{Kind: kind, Err: backend.ErrNoSpace})
			if kind == faultbackend.Write {
				require.ErrorIs(t, c.Write(ctx, "k", []byte("new")), backend.ErrNoSpace)
				require.ErrorIs(t, backend.WriteDeferred(ctx, c, "k", []byte("new")), backend.ErrNoSpace)
			} else {
				won, err := c.PutIfAbsent(ctx, "k", []byte("new"))
				require.ErrorIs(t, err, backend.ErrNoSpace)
				assert.False(t, won)
			}
			got, err := backend.ReadView(ctx, c, "k")
			require.NoError(t, err)
			assert.Same(t, &old[0], &got[0])
		})
	}
}

func TestCacheLostPutKeepsResidentValue(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	c := backend.Cached(backend.Memory(), 1<<20)
	require.NoError(t, c.Write(ctx, "k", []byte("old")))
	old, err := backend.ReadView(ctx, c, "k")
	require.NoError(t, err)
	won, err := c.PutIfAbsent(ctx, "k", []byte("new"))
	require.NoError(t, err)
	assert.False(t, won)
	got, err := backend.ReadView(ctx, c, "k")
	require.NoError(t, err)
	assert.Same(t, &old[0], &got[0])
}
