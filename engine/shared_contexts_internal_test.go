package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/series"
)

func TestEngineSharedContextsReset(t *testing.T) {
	t.Parallel()
	pool := series.NewContexts(1 << 20)
	be := backend.Memory()
	a := New(Config{Backend: be, Prefix: "a", DiskSeries: true, OOOWindow: 10, SeriesContexts: pool})
	b := New(Config{Backend: be, Prefix: "b", DiskSeries: true, OOOWindow: 10, SeriesContexts: pool})
	t.Cleanup(func() { require.NoError(t, a.CloseWAL()); require.NoError(t, b.CloseWAL()) })
	s := benchSeries(0)
	ok, err := a.Append(s, 100, 1)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = b.Append(s, 200, 2)
	require.NoError(t, err)
	require.True(t, ok)
	av, _ := a.head.series.Get(s.Hash())
	bv, _ := b.head.series.Get(s.Hash())
	require.Same(t, &av.Resource.Attributes[0], &bv.Resource.Attributes[0])
	require.NoError(t, a.Flush(t.Context()))
	require.Zero(t, a.head.series.Len())
	require.NoError(t, a.Reset(t.Context()))
	require.Zero(t, a.Stats().Series)
	ok, err = a.Append(s, 50, 3)
	require.NoError(t, err)
	require.True(t, ok, "shared contexts cannot inherit another head's admission watermark")
	av, _ = a.head.series.Get(s.Hash())
	require.Same(t, &av.Resource.Attributes[0], &bv.Resource.Attributes[0])
	require.EqualValues(t, 1, b.Stats().HeadSamples)
}
