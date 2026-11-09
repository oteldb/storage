package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
)

type countRangeReader struct {
	backend.Backend

	reads int
}

func (b *countRangeReader) NativeSize(ctx context.Context, key string) (int64, bool, error) {
	return backend.NativeSizeOf(ctx, b.Backend, key)
}
func (b *countRangeReader) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	b.reads++
	return backend.ReadAt(ctx, b.Backend, key, off, n)
}

func TestDiskCountPricesNativeRangeFallbackCopies(t *testing.T) {
	t.Parallel()
	b := &countRangeReader{Backend: backend.Memory()}
	_, appends := any(b).(backend.ReadAppender)
	require.False(t, appends)
	_, views := any(b).(backend.ViewerAt)
	require.False(t, views)
	e := New(Config{Backend: b, Prefix: "count-range-copy", DiskSeries: true})
	t.Cleanup(func() { require.NoError(t, e.Close(context.Background())) })
	const count = 1024
	for i := range count {
		accepted, err := e.Append(benchSeries(i), 10, 1)
		require.NoError(t, err)
		require.True(t, accepted)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.parts, 1)
	request := fetch.Request{Start: 0, End: 20}
	small := readbudget.New(30 << 10)
	b.reads = 0
	_, err := e.Count(readbudget.With(t.Context(), small), request)
	require.ErrorIs(t, err, readbudget.ErrExceeded)
	require.Zero(t, b.reads, "range result, destination and growth must be admitted before any payload read")
	require.EqualValues(t, 30<<10, small.Remaining())
	require.Zero(t, e.parts[0].refs.Load())
	large := readbudget.New(4 << 20)
	got, err := e.Count(readbudget.With(t.Context(), large), request)
	require.NoError(t, err)
	require.Equal(t, count, got)
	require.Positive(t, b.reads)
	require.EqualValues(t, 4<<20, large.Remaining())
	require.Zero(t, e.parts[0].refs.Load())
}
