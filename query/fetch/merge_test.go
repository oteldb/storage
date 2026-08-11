package fetch_test

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type fakeFetcher struct{ batches []*fetch.Batch }

func (f fakeFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	return fetch.NewSliceIterator(f.batches), nil
}

func batch(id uint64, pairs ...[2]int64) *fetch.Batch {
	b := &fetch.Batch{ID: signal.SeriesID{Lo: id}}
	for _, p := range pairs {
		b.Timestamps = append(b.Timestamps, p[0])
		b.Values = append(b.Values, float64(p[1]))
	}

	return b
}

func drain(t *testing.T, f fetch.Fetcher) []*fetch.Batch {
	t.Helper()
	it, err := f.Fetch(context.Background(), fetch.Request{})
	require.NoError(t, err)
	out, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	return out
}

func TestMergeEmptyAndSingle(t *testing.T) {
	t.Parallel()

	assert.Empty(t, drain(t, fetch.Merge()), "no children ⇒ empty")

	f := fakeFetcher{batches: []*fetch.Batch{batch(1, [2]int64{10, 1})}}
	got := drain(t, fetch.Merge(f))
	require.Len(t, got, 1, "single child is a pass-through")
	assert.Equal(t, signal.SeriesID{Lo: 1}, got[0].ID)
}

func TestMergeDisjointSeries(t *testing.T) {
	t.Parallel()

	a := fakeFetcher{batches: []*fetch.Batch{batch(1, [2]int64{10, 1})}}
	b := fakeFetcher{batches: []*fetch.Batch{batch(2, [2]int64{10, 2})}}

	got := drain(t, fetch.Merge(a, b))
	require.Len(t, got, 2, "distinct ids stay separate")
	assert.Equal(t, signal.SeriesID{Lo: 1}, got[0].ID)
	assert.Equal(t, signal.SeriesID{Lo: 2}, got[1].ID)
}

func TestMergeSameSeriesAcrossChildren(t *testing.T) {
	t.Parallel()

	// The same series id in two children: samples are unioned and timestamp-ordered.
	a := fakeFetcher{batches: []*fetch.Batch{batch(7, [2]int64{10, 1}, [2]int64{30, 3})}}
	b := fakeFetcher{batches: []*fetch.Batch{batch(7, [2]int64{20, 2}, [2]int64{40, 4})}}

	got := drain(t, fetch.Merge(a, b))
	require.Len(t, got, 1, "same id federates into one series")
	assert.Equal(t, []int64{10, 20, 30, 40}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2, 3, 4}, got[0].Values)
}

func TestMergeDuplicateTimestampLaterChildWins(t *testing.T) {
	t.Parallel()

	a := fakeFetcher{batches: []*fetch.Batch{batch(7, [2]int64{10, 1})}}
	b := fakeFetcher{batches: []*fetch.Batch{batch(7, [2]int64{10, 99})}}

	got := drain(t, fetch.Merge(a, b))
	require.Len(t, got, 1)
	assert.Equal(t, []int64{10}, got[0].Timestamps)
	assert.Equal(t, []float64{99}, got[0].Values, "later child wins the duplicate timestamp")
}

func TestMergeDoesNotMutateChildBatches(t *testing.T) {
	t.Parallel()

	shared := batch(7, [2]int64{10, 1})
	a := fakeFetcher{batches: []*fetch.Batch{shared}}
	b := fakeFetcher{batches: []*fetch.Batch{batch(7, [2]int64{20, 2})}}

	_ = drain(t, fetch.Merge(a, b))
	assert.Equal(t, []int64{10}, shared.Timestamps, "the child's batch is cloned, not appended to")
}

// idSeries builds a distinct, content-addressed identity ("host"=name) and a batch carrying it, so
// a fan-out test exercises the real id contract (Batch.ID == Series.Hash()).
func idSeries(name string) (signal.Series, *fetch.Batch) {
	s := signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(name))},
	)}

	return s, &fetch.Batch{ID: s.Hash(), Series: s, Timestamps: []int64{10}, Values: []float64{1}}
}

// listingFetcher answers the enumeration capability from a fixed identity set; its Fetch fails the
// test, so a fan-out that reaches for samples on a capable child is caught.
type listingFetcher struct {
	t      *testing.T
	series []signal.Series
}

func (f listingFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	f.t.Error("Fetch must not be called on a child that lists series")

	return fetch.NewSliceIterator(nil), nil
}

func (f listingFetcher) Series(context.Context, fetch.Request) ([]signal.Series, error) {
	return f.series, nil
}

func hostsOf(t *testing.T, series []signal.Series) []string {
	t.Helper()

	out := make([]string, 0, len(series))
	for i := range series {
		v, ok := series[i].Attributes.Get([]byte("host"))
		require.True(t, ok)
		out = append(out, string(v.Str()))
	}

	slices.Sort(out)

	return out
}

// TestMergeSeriesFanOut covers the fan-out's enumeration capability: children are unioned and
// deduplicated by identity (unlike counting, enumeration composes), a child without the capability
// still contributes through a plain fetch, and the whole fan-out is discoverable as a
// [fetch.SeriesLister].
func TestMergeSeriesFanOut(t *testing.T) {
	t.Parallel()

	a, ab := idSeries("a")
	b, bb := idSeries("b")
	c, _ := idSeries("c")

	// Child 1 lists {a, b}; child 2 lists {b, c} (b is the cross-child duplicate); child 3 has no
	// capability and contributes {a, b} through its batches.
	m := fetch.Merge(
		listingFetcher{t: t, series: []signal.Series{a, b}},
		listingFetcher{t: t, series: []signal.Series{b, c}},
		fakeFetcher{batches: []*fetch.Batch{ab, bb}},
	)

	lister := fetch.SeriesListerOf(m)
	require.NotNil(t, lister, "the fan-out exposes the enumeration capability")

	got, err := lister.Series(context.Background(), fetch.Request{})
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, hostsOf(t, got), "union, deduplicated by identity")

	// The output is one ascending id stream, so a fan-out is itself a legal child of another.
	for i := 1; i < len(got); i++ {
		assert.Negative(t, got[i-1].Hash().Compare(got[i].Hash()), "ids ascend")
	}
}

// TestMergeSeriesSortsUnorderedChild pins the defensive half of the sort-merge: a child that
// returns identities out of order (a gather that concatenated its shards, say) is sorted rather
// than mis-merged into duplicates.
func TestMergeSeriesSortsUnorderedChild(t *testing.T) {
	t.Parallel()

	names := []string{"a", "b", "c", "d"}

	series := make([]signal.Series, 0, len(names))
	for _, name := range names {
		s, _ := idSeries(name)
		series = append(series, s)
	}

	// Sorted ascending, then reversed — whatever the hashes are, the child is now descending.
	sorted := fetch.SortSeries(slices.Clone(series))
	reversed := slices.Clone(sorted)
	slices.Reverse(reversed)

	m := fetch.Merge(
		listingFetcher{t: t, series: reversed},
		listingFetcher{t: t, series: []signal.Series{sorted[0], sorted[2]}}, // duplicates of the first child
	)

	got, err := fetch.SeriesListerOf(m).Series(context.Background(), fetch.Request{})
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "d"}, hostsOf(t, got), "each series once, despite the unordered child")
}

// labelingFetcher answers the label-metadata capability from fixed sets.
type labelingFetcher struct {
	fakeFetcher

	names  []string
	values []string
}

func (f labelingFetcher) LabelNames(context.Context, fetch.Request) ([]string, error) {
	return f.names, nil
}

func (f labelingFetcher) LabelValues(context.Context, fetch.Request, []byte) ([]string, error) {
	return f.values, nil
}

// TestMergeLabelsFanOut covers the fan-out's label-metadata capability: the children's sets are
// unioned and deduplicated, and a fan-out with even one incapable child reports the call
// unsupported rather than answering from a subset.
func TestMergeLabelsFanOut(t *testing.T) {
	t.Parallel()

	capable := fetch.Merge(
		labelingFetcher{names: []string{"__name__", "route"}, values: []string{"/a", "/b"}},
		labelingFetcher{names: []string{"route", "zone"}, values: []string{"/b", "/c"}},
	)

	lister := fetch.LabelListerOf(capable)
	require.NotNil(t, lister)

	names, err := lister.LabelNames(context.Background(), fetch.Request{})
	require.NoError(t, err)
	assert.Equal(t, []string{"__name__", "route", "zone"}, names)

	values, err := lister.LabelValues(context.Background(), fetch.Request{}, []byte("route"))
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/b", "/c"}, values)

	// One incapable child ⇒ the whole fan-out opts out, so the caller falls back to a path that
	// sees every child instead of silently losing its labels.
	partial := fetch.Merge(
		labelingFetcher{names: []string{"route"}},
		fakeFetcher{batches: []*fetch.Batch{batch(1, [2]int64{10, 1})}},
	)

	_, err = fetch.LabelListerOf(partial).LabelNames(context.Background(), fetch.Request{})
	require.ErrorIs(t, err, fetch.ErrLabelsUnsupported)
}
