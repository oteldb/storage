package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

func TestSeparateSeriesLifecycle(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	cfg := engine.Config{Backend: be, Prefix: "separated", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true}
	e := engine.New(cfg)
	s := mkSeries("job", "api")
	other := mkSeries("job", "web")
	for _, ts := range []int64{100, 200, 300} {
		mustAppend(t, e, s, ts, float64(ts))
		if ts == 200 {
			mustAppend(t, e, other, ts, 2)
		}
		require.NoError(t, e.Flush(t.Context()))
	}
	ix, err := bucketindex.Load(t.Context(), be, "separated/"+bucketindex.Object)
	require.NoError(t, err)
	require.Len(t, ix.SeriesRoots, 2, "only previously unseen descriptors create runs")
	for _, part := range ix.Entries {
		for _, suffix := range []string{"/identity", "/series-index"} {
			_, err := be.Read(t.Context(), part.Prefix+suffix)
			require.ErrorIs(t, err, backend.ErrNotExist)
		}
	}
	req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}
	got := fetchAll(t, e, req)
	require.Len(t, got, 1)
	require.Equal(t, []int64{100, 200, 300}, got[0].Timestamps)
	require.NoError(t, e.Merge(t.Context(), 200))
	require.NoError(t, e.Close(t.Context()))
	recovered := engine.New(cfg)
	require.NoError(t, recovered.LoadParts(t.Context()))
	defer func() { require.NoError(t, recovered.Close(t.Context())) }()
	got = fetchAll(t, recovered, req)
	require.Len(t, got, 1)
	require.Equal(t, []int64{200, 300}, got[0].Timestamps)
	require.Equal(t, 2, recovered.SeriesCount())
	// Retaining descriptors does not retain active cardinality after all numeric rows expire.
	require.NoError(t, recovered.Merge(t.Context(), 1001))
	_, err = recovered.PruneIdentities(t.Context())
	require.NoError(t, err)
	require.Zero(t, recovered.SeriesCount())
	require.Empty(t, fetchAll(t, recovered, req))
	require.NoError(t, recovered.Close(t.Context()))
	again := engine.New(cfg)
	require.NoError(t, again.LoadParts(t.Context()))
	require.Zero(t, again.SeriesCount())
	mustAppend(t, again, s, 1100, 7)
	require.NoError(t, again.Flush(t.Context()))
	ix, err = bucketindex.Load(t.Context(), be, "separated/"+bucketindex.Object)
	require.NoError(t, err)
	require.Len(t, ix.SeriesRoots, 1, "compacted descriptors are reused after numeric retention")
	require.NoError(t, again.Close(t.Context()))
}

func TestSeparateSeriesMissingRootFencesLoad(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	cfg := engine.Config{Backend: be, Prefix: "missing-root", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true}
	e := engine.New(cfg)
	mustAppend(t, e, mkSeries("job", "api"), 100, 1)
	require.NoError(t, e.Close(t.Context()))
	ix, err := bucketindex.Load(t.Context(), be, "missing-root/"+bucketindex.Object)
	require.NoError(t, err)
	require.Len(t, ix.SeriesRoots, 1)
	require.NoError(t, be.Delete(t.Context(), ix.SeriesRoots[0].Prefix+"/series-index"))
	before, err := be.Read(t.Context(), "missing-root/"+bucketindex.Object)
	require.NoError(t, err)
	recovered := engine.New(cfg)
	require.Error(t, recovered.LoadParts(t.Context()))
	after, err := be.Read(t.Context(), "missing-root/"+bucketindex.Object)
	require.NoError(t, err)
	require.Equal(t, before, after, "missing metadata cannot be converted into a sample repair hole")
}
