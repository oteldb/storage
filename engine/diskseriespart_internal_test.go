package engine

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func bindTestSeriesIndex(tb testing.TB, b backend.Backend, p *part, builder *seriesindex.Builder) {
	tb.Helper()
	digest, err := block.BindManifest(tb.Context(), b, p.prefix)
	require.NoError(tb, err)
	r, err := block.OpenPart(tb.Context(), b, p.prefix)
	require.NoError(tb, err)
	require.NoError(tb, p.reader.Close())
	p.reader = r
	builder.BindParent(digest)
	require.NoError(tb, builder.WriteObject(tb.Context(), b, seriesIndexKey(p.prefix)))
	p.metadata, p.metadataChecked = nil, false
}

func TestDiskSeriesPartPublicationAndLegacyRead(t *testing.T) {
	t.Parallel()
	b := backend.Memory()
	e := New(Config{Backend: b, Prefix: "disk-index", DiskSeries: true, AggregateStats: true})
	src := []signal.Series{benchSeries(1), benchSeries(2)}
	slices.SortFunc(src, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	for _, s := range src {
		for i := range 3 {
			accepted, err := e.Append(s, int64(i+1), float64(i+1))
			require.NoError(t, err)
			require.True(t, accepted)
		}
	}
	require.NoError(t, e.Flush(t.Context()))
	require.NotEmpty(t, e.parts)
	for _, p := range e.parts {
		idx, err := p.seriesIndex(t.Context())
		require.NoError(t, err)
		require.NotNil(t, idx)
		require.EqualValues(t, p.index.seriesCount(), idx.Count())
		for _, s := range src {
			got, meta, ok, err := idx.Lookup(t.Context(), s.Hash())
			require.NoError(t, err)
			if !ok {
				continue
			}
			require.True(t, got.Equal(s))
			require.EqualValues(t, 3, meta.Covered)
			require.True(t, meta.HasStats)
			require.Equal(t, seriesindex.Stats{Count: 3, Sum: 6, Min: 1, Max: 3}, meta.Stats)
		}
	}
	old := New(Config{Backend: b, Prefix: "disk-index"})
	require.NoError(t, old.LoadParts(t.Context()))
	it, err := old.Fetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	defer it.Close()
	rows, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, rows, 2, "the legacy identity object remains readable")
	for _, row := range rows {
		require.Equal(t, []float64{1, 2, 3}, row.Values)
	}
}

func TestDiskSeriesMergeWritesIndex(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "merge-index", DiskSeries: true})
	s := benchSeries(1)
	for i := range 2 {
		ok, err := e.Append(s, int64(i+1), float64(i))
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, e.Flush(t.Context()))
	}
	require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true}))
	for _, p := range e.parts {
		idx, err := p.seriesIndex(t.Context())
		require.NoError(t, err)
		require.NotNil(t, idx)
		_, meta, found, err := idx.Lookup(t.Context(), s.Hash())
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, 2, meta.Covered)
	}
}
