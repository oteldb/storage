package file_test

import (
	"context"
	"io"
	"io/fs"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
)

func TestObjectReader(t *testing.T) {
	t.Parallel()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, b.Write(t.Context(), "object", []byte("abcdef")))
	r, supported, err := backend.OpenReader(t.Context(), backend.Cached(b, 1<<20), "object")
	require.NoError(t, err)
	require.True(t, supported)
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	require.Equal(t, int64(6), r.Size())
	_, err = r.Seek(0, io.SeekStart)
	require.NoError(t, err)
	buf := make([]byte, 2)
	_, err = r.ReadAt(buf, 4)
	require.NoError(t, err)
	require.Equal(t, "ef", string(buf))
	_, err = io.ReadFull(r, buf)
	require.NoError(t, err)
	require.Equal(t, "ab", string(buf), "ReadAt does not move the streaming position")
	require.NoError(t, b.Write(t.Context(), "object", []byte("new")))
	_, err = r.ReadAt(buf, 4)
	require.NoError(t, err)
	require.Equal(t, "ef", string(buf), "the old opened generation survives replacement")
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
	_, err = r.ReadAt(buf, 0)
	require.ErrorIs(t, err, fs.ErrClosed)
}

func TestObjectReaderOpenFailures(t *testing.T) {
	t.Parallel()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	_, supported, err := backend.OpenReader(t.Context(), b, "missing")
	require.True(t, supported)
	require.ErrorIs(t, err, backend.ErrNotExist)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = b.OpenReader(ctx, "missing")
	require.ErrorIs(t, err, context.Canceled)
	_, supported, err = backend.OpenReader(t.Context(), backend.Memory(), "missing")
	require.False(t, supported)
	require.NoError(t, err)
}

func TestObjectReaderSeek(t *testing.T) {
	t.Parallel()
	b, err := file.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, b.Write(t.Context(), "object", []byte("abcdef")))
	for _, tt := range []struct {
		name    string
		off     int64
		whence  int
		pos     int64
		want    string
		wantErr bool
	}{
		{"start", 1, io.SeekStart, 1, "bc", false},
		{"current", 1, io.SeekCurrent, 4, "ef", false},
		{"end", -2, io.SeekEnd, 4, "ef", false},
		{"negative", -7, io.SeekEnd, 0, "", true},
		{"overflow", math.MaxInt64, io.SeekEnd, 0, "", true},
		{"bad-whence", 0, -1, 0, "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, _, err := b.OpenReader(t.Context(), "object")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, r.Close()) })
			if tt.whence == io.SeekCurrent {
				_, err := r.Seek(3, io.SeekStart)
				require.NoError(t, err)
			}
			pos, err := r.Seek(tt.off, tt.whence)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.pos, pos)
			buf := make([]byte, 2)
			_, err = io.ReadFull(r, buf)
			require.NoError(t, err)
			require.Equal(t, tt.want, string(buf))
		})
	}
}
