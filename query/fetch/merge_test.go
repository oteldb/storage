package fetch_test

import (
	"context"
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
