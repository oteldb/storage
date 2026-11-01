package block

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
)

func TestFrameSourceCachePolicy(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		window int64
		reads  int64
	}{
		{name: "point frames shared across decoders", reads: 1},
		{name: "scan windows remain uncached", window: 2048, reads: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			inner := backend.Memory()
			require.NoError(t, inner.Write(t.Context(), "column", bytes.Repeat([]byte{1}, 8192)))
			counter := backendtest.NewByteCounter(inner)
			cached := backend.Cached(counter, 4096)
			for range 2 {
				source := &frameSource{ctx: t.Context(), b: cached, key: "column", window: tt.window,
					frameOff: []int32{0, 1024, 2048}}
				frame, err := source.frame(0, 0, 1024)
				require.NoError(t, err)
				require.Equal(t, bytes.Repeat([]byte{1}, 1024), frame)
			}
			require.Equal(t, tt.reads, counter.Reads())
		})
	}
}

func TestFrameSourceCacheInvalidationAndShortRead(t *testing.T) {
	t.Parallel()
	inner := backend.Memory()
	require.NoError(t, inner.Write(t.Context(), "column", bytes.Repeat([]byte{1}, 8192)))
	cached := backend.Cached(inner, 4096)
	source := &frameSource{ctx: t.Context(), b: cached, key: "column"}
	old, err := source.frame(0, 0, 1024)
	require.NoError(t, err)
	require.NoError(t, backend.WriteUncached(t.Context(), cached, "column", bytes.Repeat([]byte{2}, 8192)))
	fresh, err := source.frame(0, 0, 1024)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{2}, 1024), fresh)
	require.Equal(t, bytes.Repeat([]byte{1}, 1024), old)
	_, err = source.frame(0, 8000, 1024)
	require.ErrorIs(t, err, ErrCorrupt)
}
