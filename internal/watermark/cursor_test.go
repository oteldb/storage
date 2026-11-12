package watermark

import (
	"context"
	"strconv"
	"testing"

	"github.com/go-faster/errors"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/signal"
)

func TestCursor(t *testing.T) {
	t.Parallel()
	for _, count := range []int{0, 1, cursorEntries, cursorEntries + 1, 4097} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			entries := make([]Entry, count)
			for i := range entries {
				entries[i] = Entry{ID: signal.SeriesID{Hi: uint64(i + 1)}, Max: int64(i) - 7}
			}
			be := backendtest.NewByteCounter(backend.Memory())
			require.NoError(t, be.Write(t.Context(), "smax", Encode(nil, entries)))
			r, err := OpenReader(t.Context(), be, "smax", count)
			require.NoError(t, err)
			be.Reset()
			cursor := r.Cursor(t.Context())
			for i, want := range entries {
				got, err := cursor.At(i)
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
			require.EqualValues(t, (count+cursorEntries-1)/cursorEntries, be.Reads(), "one ranged read per window")
			require.EqualValues(t, count*entryWidth, be.Bytes())
			require.LessOrEqual(t, cap(cursor.data), 64<<10)
			for _, i := range []int{count / 2, 0, count - 1} {
				if i < 0 || i >= count {
					continue
				}
				got, err := cursor.At(i)
				require.NoError(t, err)
				require.Equal(t, entries[i], got)
			}
			_, err = cursor.At(-1)
			require.ErrorIs(t, err, ErrCorrupt)
			_, err = cursor.At(count)
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

type failingCursorBackend struct {
	*backendtest.ByteCounter

	fail bool
}

func (b *failingCursorBackend) ReadAtAppend(ctx context.Context, dst []byte, key string, off, n int64) ([]byte, error) {
	if b.fail {
		b.fail = false
		dst = append(dst, byte(255))
		dst[0] = 255
		return nil, errors.New("failed window read")
	}
	return backend.ReadAtAppend(ctx, b.ByteCounter, dst, key, off, n)
}

func TestCursorReadFailureInvalidatesWindow(t *testing.T) {
	t.Parallel()
	entries := make([]Entry, cursorEntries+1)
	for i := range entries {
		entries[i] = Entry{ID: signal.SeriesID{Hi: uint64(i + 1)}, Max: int64(i)}
	}
	be := &failingCursorBackend{ByteCounter: backendtest.NewByteCounter(backend.Memory())}
	require.NoError(t, be.Write(t.Context(), "smax", Encode(nil, entries)))
	reader, err := OpenReader(t.Context(), be, "smax", len(entries))
	require.NoError(t, err)
	cursor := reader.Cursor(t.Context())
	got, err := cursor.At(0)
	require.NoError(t, err)
	require.Equal(t, entries[0], got)
	be.fail = true
	_, err = cursor.At(cursorEntries)
	require.Error(t, err)
	got, err = cursor.At(0)
	require.NoError(t, err)
	require.Equal(t, entries[0], got)
}

func TestCursorResetChangesOwner(t *testing.T) {
	t.Parallel()
	be := backendtest.NewByteCounter(backend.Memory())
	readers := make([]*Reader, 2)
	for i, key := range []string{"first", "second"} {
		require.NoError(t, be.Write(t.Context(), key, Encode(nil, []Entry{{ID: signal.SeriesID{Hi: 1}, Max: int64(i + 1)}})))
		reader, err := OpenReader(t.Context(), be, key, 1)
		require.NoError(t, err)
		readers[i] = reader
	}
	be.Reset()
	var cursor Cursor
	cursor.Reset(t.Context(), readers[0])
	first, err := cursor.At(0)
	require.NoError(t, err)
	require.EqualValues(t, 1, first.Max)
	capacity := cap(cursor.data)
	cursor.Reset(t.Context(), readers[1])
	second, err := cursor.At(0)
	require.NoError(t, err)
	require.EqualValues(t, 2, second.Max)
	require.Equal(t, capacity, cap(cursor.data))
	require.EqualValues(t, 2, be.Reads())
}
