package backend_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

//nolint:paralleltest // Mutations exercise one cache lifecycle in order.
func TestPageCacheCoherence(t *testing.T) {
	ctx := t.Context()
	inner := backend.Memory()
	require.NoError(t, inner.Write(ctx, "part", bytes.Repeat([]byte{1}, 8192)))
	counter := backendtest.NewByteCounter(inner)
	cached := backend.Cached(counter, 4096)
	old, err := backend.ReadPageView(ctx, cached, "part", 100, 1024)
	require.NoError(t, err)
	assert.EqualValues(t, 1024, counter.Bytes())
	_, err = backend.ReadPageView(ctx, cached, "part", 100, 1024)
	require.NoError(t, err)
	assert.EqualValues(t, 1024, counter.Bytes())

	for _, tt := range []struct {
		name  string
		write func([]byte) error
	}{
		{name: "uncached", write: func(v []byte) error { return backend.WriteUncached(ctx, cached, "part", v) }},
		{name: "deferred", write: func(v []byte) error { return backend.WriteUncachedDeferred(ctx, cached, "part", v) }},
		{name: "cached", write: func(v []byte) error { return cached.Write(ctx, "part", v) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := bytes.Repeat([]byte(tt.name), 8192)
			require.NoError(t, tt.write(data))
			got, err := backend.ReadPageView(ctx, cached, "part", 100, 1024)
			require.NoError(t, err)
			assert.Equal(t, data[100:1124], got)
		})
	}
	assert.Equal(t, bytes.Repeat([]byte{1}, 1024), old)
	require.NoError(t, cached.Delete(ctx, "part"))
	_, err = backend.ReadPageView(ctx, cached, "part", 100, 1024)
	require.ErrorIs(t, err, backend.ErrNotExist)
	require.NoError(t, cached.Write(ctx, "part", bytes.Repeat([]byte{9}, 8192)))
	got, err := backend.ReadPageView(ctx, cached, "part", 100, 1024)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{9}, 1024), got)
}

func TestPageCacheConcurrentMiss(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	inner := backend.Memory()
	require.NoError(t, inner.Write(ctx, "part", bytes.Repeat([]byte{1}, 8192)))
	counter := backendtest.NewByteCounter(inner)
	cached := backend.Cached(counter, 4096)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			<-start
			got, err := backend.ReadPageView(ctx, cached, "part", 0, 1024)
			if err != nil {
				t.Error(err)
				return
			}
			assert.Len(t, got, 1024)
		})
	}
	close(start)
	wg.Wait()
	assert.EqualValues(t, 1, counter.Reads())
}
