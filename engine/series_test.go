package engine_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// hostsOf projects enumerated identities to their sorted "host" label values, the discriminator the
// series tests use.
func hostsOf(t *testing.T, series []signal.Series) []string {
	t.Helper()

	out := make([]string, 0, len(series))
	for i := range series {
		v, ok := series[i].Attributes.Get([]byte("host"))
		require.True(t, ok, "identity must carry the host label")
		out = append(out, string(v.Str()))
	}

	slices.Sort(out)

	if len(out) == 0 {
		return nil
	}

	return out
}

// fetchHosts is hostsOf over a full Fetch+drain — the reference the enumeration must agree with.
func fetchHosts(ctx context.Context, t *testing.T, e *engine.Engine, r fetch.Request) []string {
	t.Helper()

	it, err := e.Fetch(ctx, r)
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)

	series := make([]signal.Series, 0, len(batches))
	for _, b := range batches {
		series = append(series, b.Series)
	}

	return hostsOf(t, series)
}

// TestSeriesEnumeration verifies Engine.Series returns the identities a full Fetch+drain would
// yield — across head, parts, head ∪ part unions, and windows that exclude everything — the contract
// the PromQL label endpoints rely on to skip sample materialization. The part-overlap-granular
// window (see TestSeriesPartGranularWindow) makes it a superset, never a subset.
func TestSeriesEnumeration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/series"})

	a := mkSeries("__name__", "node_x", "host", "a")
	b := mkSeries("__name__", "node_x", "host", "b")
	c := mkSeries("__name__", "node_y", "host", "c")

	mustAppend(t, e, a, 10, 1)
	mustAppend(t, e, b, 15, 2)
	mustAppend(t, e, c, 10, 9)
	require.NoError(t, e.Flush(ctx))

	mustAppend(t, e, a, 30, 3) // head, after the flush
	mustAppend(t, e, b, 35, 4)

	matcher := []fetch.Matcher{eqMatcher("__name__", "node_x")}

	for _, tt := range []struct {
		name string
		req  fetch.Request
		want []string
	}{
		{"head and part", fetch.Request{Start: 0, End: 100, Matchers: matcher}, []string{"a", "b"}},
		{"part only", fetch.Request{Start: 0, End: 20, Matchers: matcher}, []string{"a", "b"}},
		// Part-granular: the part [10,15] overlaps [12,20], so both of its matched series are
		// listed even though only b has a sample inside the window (see TestSeriesPartGranularWindow).
		{"part edge", fetch.Request{Start: 12, End: 20, Matchers: matcher}, []string{"a", "b"}},
		{"head only", fetch.Request{Start: 31, End: 100, Matchers: matcher}, []string{"b"}},
		{"empty window", fetch.Request{Start: 1000, End: 2000, Matchers: matcher}, nil},
		{"no matchers", fetch.Request{Start: 0, End: 100}, []string{"a", "b", "c"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := e.Series(ctx, tt.req)
			require.NoError(t, err)
			assert.Equal(t, tt.want, hostsOf(t, got))
			assert.Subset(t, hostsOf(t, got), fetchHosts(ctx, t, e, tt.req),
				"Series must be a superset of a full Fetch+drain, never dropping a series")
		})
	}
}

// TestSeriesAcrossParts exercises the multi-part regimes the enumeration inherits from the count
// plan — pruned, fully covered (index-only), and window-edge (timestamp decode) — and pins that a
// series living in two parts is returned once.
func TestSeriesAcrossParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/series-parts"})

	a := mkSeries("__name__", "node_x", "host", "a")
	b := mkSeries("__name__", "node_x", "host", "b")
	c := mkSeries("__name__", "node_x", "host", "c")
	d := mkSeries("__name__", "node_x", "host", "d")

	// part1 [100,110]: a, b. part2 [200,210]: a (again), c. part3 [300,310]: b (again), d.
	mustAppend(t, e, a, 100, 1)
	mustAppend(t, e, b, 110, 2)
	require.NoError(t, e.Flush(ctx))

	mustAppend(t, e, a, 200, 3)
	mustAppend(t, e, c, 210, 4)
	require.NoError(t, e.Flush(ctx))

	mustAppend(t, e, b, 300, 5)
	mustAppend(t, e, d, 310, 6)
	require.NoError(t, e.Flush(ctx))

	require.Equal(t, 3, e.PartCount())

	matcher := []fetch.Matcher{eqMatcher("__name__", "node_x")}

	all := fetch.Request{Start: 0, End: 1000, Matchers: matcher}
	got, err := e.Series(ctx, all)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "d"}, hostsOf(t, got), "a and b span two parts, listed once")

	// part1 partial (a@100 excluded, b@110 kept), part2 fully covered, part3 time-pruned.
	mixed := fetch.Request{Start: 105, End: 250, Matchers: matcher}
	gotMixed, err := e.Series(ctx, mixed)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, hostsOf(t, gotMixed))
	assert.Subset(t, hostsOf(t, gotMixed), fetchHosts(ctx, t, e, mixed))

	gap, err := e.Series(ctx, fetch.Request{Start: 120, End: 190, Matchers: matcher})
	require.NoError(t, err)
	assert.Empty(t, gap, "no series has a sample in the inter-part gap")
}

// TestSeriesPartGranularWindow pins the enumeration's window semantics against Count's: enumeration
// reads the series index only, so a series living in a part that merely *overlaps* the window is
// listed (Prometheus' own label endpoints are block-granular the same way), while Count pays a
// timestamp decode at the window edge to answer exactly.
func TestSeriesPartGranularWindow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "t/series-granular"})

	a := mkSeries("__name__", "node_x", "host", "a")
	b := mkSeries("__name__", "node_x", "host", "b")

	mustAppend(t, e, a, 100, 1)
	mustAppend(t, e, b, 200, 2)
	require.NoError(t, e.Flush(ctx)) // one part [100,200] holding both

	matcher := []fetch.Matcher{eqMatcher("__name__", "node_x")}
	req := fetch.Request{Start: 150, End: 250, Matchers: matcher} // only b has a sample inside

	got, err := e.Series(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, hostsOf(t, got), "part-granular: the overlapping part contributes both")

	n, err := e.Count(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "Count stays exact — only b has a sample in the window")

	// A part disjoint from the window is still pruned outright, so the filter is not a no-op.
	none, err := e.Series(ctx, fetch.Request{Start: 300, End: 400, Matchers: matcher})
	require.NoError(t, err)
	assert.Empty(t, none)
}

// TestSeriesDoesNotDecode pins the point of the enumeration seam: listing identities over a
// window-edge part reads no column at all, so a later value-needing Fetch on the same part must
// still return real values (no partial decode-cache entry may be served to it).
func TestSeriesDoesNotDecode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := engine.New(engine.Config{
		Backend: backend.Memory(), Prefix: "t/series-prune", DecodeCacheBytes: 1 << 20,
	})

	s := mkSeries("__name__", "node_x", "host", "a")
	mustAppend(t, e, s, 10, 1)
	mustAppend(t, e, s, 20, 2)
	mustAppend(t, e, s, 30, 3)
	require.NoError(t, e.Flush(ctx))

	matcher := []fetch.Matcher{eqMatcher("__name__", "node_x")}

	got, err := e.Series(ctx, fetch.Request{Start: 15, End: 100, Matchers: matcher})
	require.NoError(t, err)
	require.Len(t, got, 1)

	batches := fetchAll(t, e, fetch.Request{Start: 0, End: 100, Matchers: matcher})
	require.Len(t, batches, 1)
	assert.Equal(t, []float64{1, 2, 3}, batches[0].Values, "value column must survive the index-only enumeration")
}
