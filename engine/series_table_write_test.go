package engine

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

func TestSharedSeriesFlushAdmission(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	refused := errors.New("metadata workspace refused")
	reject := true
	e := New(Config{
		Backend: be, Prefix: "metadata-admission", DiskSeries: true, MetricSeriesChunks: true, SeparateSeries: true, FlushMemoryBytes: 8 << 20,
		FlushAdmission: func(_ context.Context, bytes int64) (func(), error) {
			if reject && bytes > 16<<20 {
				return nil, refused
			}
			return func() {}, nil
		},
	})
	ok, err := e.Append(benchSeries(1), 100, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.ErrorIs(t, e.Flush(t.Context()), refused)
	require.Nil(t, e.seriesTable, "admission refusal must precede metadata publication")
	require.Zero(t, e.PartCount())
	reject = false
	require.NoError(t, e.Flush(t.Context()))
	require.Equal(t, 1, e.PartCount())
	require.NoError(t, e.Close(t.Context()))
}

func TestSharedSeriesRootOrderingAfterReopen(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "root-order", DiskSeries: true, MetricSeriesChunks: true, SeparateSeries: true})
	future := catalogRoot(t, be, "root-order/s-ZZZZZZZZZZZZZZZZZZZZZZZZZZ", 1)
	ix := &bucketindex.Index{SeriesRoots: []bucketindex.SeriesRoot{future}}
	_, err := ix.Save(t.Context(), be, e.indexKey(), "")
	require.NoError(t, err)
	require.NoError(t, e.LoadParts(t.Context()))
	ok, err := e.Append(benchSeries(2), 100, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, e.Flush(t.Context()))
	loaded, err := bucketindex.Load(t.Context(), be, e.indexKey())
	require.NoError(t, err)
	require.Len(t, loaded.SeriesRoots, 2)
	require.Equal(t, future, loaded.SeriesRoots[1])
	require.NoError(t, e.Close(t.Context()))
}
