package seriesindex_test

import (
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestResolveCachesBroadExactPosting(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const count = 4096
	b := backend.CachedWithPostings(backend.Memory(), 64<<20, 4<<20)
	type entry struct {
		id     signal.SeriesID
		series signal.Series
	}
	entries := make([]entry, 0, count)
	for i := range count {
		s := signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("node_exporter"))},
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue(strconv.AppendInt(nil, int64(i), 10))},
		)}
		entries = append(entries, entry{id: s.Hash(), series: s})
	}
	slices.SortFunc(entries, func(a, b entry) int { return a.id.Compare(b.id) })
	builder := &seriesindex.Builder{}
	for _, e := range entries {
		require.NoError(t, builder.Add(e.id, e.series, seriesindex.Metadata{}))
	}
	require.NoError(t, builder.WriteObject(ctx, b, "index"))
	idx, err := seriesindex.Open(ctx, b, "index")
	require.NoError(t, err)

	matchers := []fetch.Matcher{{
		Name: []byte("job"),
		Spec: &fetch.EqualMatcher{Name: "job", Value: "node_exporter"},
		Match: func(v signal.Value) bool {
			return string(v.Str()) == "node_exporter"
		},
	}}
	first, err := idx.Resolve(ctx, matchers)
	require.NoError(t, err)
	require.True(t, first.Next())
	second, err := idx.Resolve(ctx, matchers)
	require.NoError(t, err)
	for i := range count {
		if i != 0 {
			require.True(t, first.Next())
		}
		require.True(t, second.Next())
		require.Equal(t, signal.SeriesID{Lo: uint64(i)}, first.At())
		require.Equal(t, signal.SeriesID{Lo: uint64(i)}, second.At())
	}
	require.False(t, first.Next())
	require.False(t, second.Next())
	require.NoError(t, first.Err())
	require.NoError(t, second.Err())

	stats := b.(interface {
		PostingStats() backend.PostingCacheStats
	}).PostingStats()
	require.EqualValues(t, 1, stats.Builds)
	require.EqualValues(t, 1, stats.Hits)
	require.Equal(t, stats.ChunksVisited, stats.ChunksAvoided)
}
