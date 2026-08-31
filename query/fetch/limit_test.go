package fetch_test

import (
	"context"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestBatchSize(t *testing.T) {
	t.Parallel()

	assert.Zero(t, (*fetch.Batch)(nil).Size(), "a nil batch costs nothing")
	assert.Zero(t, (&fetch.Batch{}).Size(), "an empty batch costs nothing")

	samples := batch(1, [2]int64{10, 1}, [2]int64{20, 2})
	// Two timestamps and two values, eight bytes each.
	assert.Equal(t, int64(32), samples.Size())

	withCols := &fetch.Batch{
		Timestamps: []int64{1},
		Columns: []fetch.NamedColumn{
			{Name: "body", Bytes: [][]byte{[]byte("hello")}},
		},
	}
	// 8 (timestamp) + 4 (column name) + 5 (payload) + 24 (slice header).
	assert.Equal(t, int64(41), withCols.Size(),
		"a byte value is charged its header as well as its payload")
}

func TestLimitBytesRejectsPastBudget(t *testing.T) {
	t.Parallel()

	// Three batches of 32 bytes each; the budget admits two.
	f := fakeFetcher{batches: []*fetch.Batch{
		batch(1, [2]int64{10, 1}, [2]int64{20, 2}),
		batch(2, [2]int64{30, 3}, [2]int64{40, 4}),
		batch(3, [2]int64{50, 5}, [2]int64{60, 6}),
	}}

	it, err := fetch.LimitBytes(f, 64).Fetch(context.Background(), fetch.Request{})
	require.NoError(t, err)

	got, err := fetch.Drain(context.Background(), it)
	require.ErrorIs(t, err, fetch.ErrTooLarge)
	assert.Len(t, got, 2, "everything read before the budget ran out is still returned")
	assert.Contains(t, err.Error(), "limit 64", "the error names the budget it broke")
}

func TestLimitBytesAdmitsUnderBudget(t *testing.T) {
	t.Parallel()

	f := fakeFetcher{batches: []*fetch.Batch{
		batch(1, [2]int64{10, 1}, [2]int64{20, 2}),
		batch(2, [2]int64{30, 3}, [2]int64{40, 4}),
	}}

	assert.Len(t, drain(t, fetch.LimitBytes(f, 1<<20)), 2, "a query inside its budget is untouched")
}

func TestLimitBytesDisabled(t *testing.T) {
	t.Parallel()

	f := fakeFetcher{batches: []*fetch.Batch{batch(1, [2]int64{10, 1})}}

	for _, max := range []int64{0, -1} {
		assert.Equal(t, fetch.Fetcher(f), fetch.LimitBytes(f, max),
			"a non-positive budget installs no limiter at all")
	}
}

// countingFetcher is a [fetch.Fetcher] that also answers the count() pushdown, so the test can check
// the limit wrapper does not hide an optional capability from [fetch.CounterOf].
type countingFetcher struct{ fakeFetcher }

func (countingFetcher) Count(context.Context, fetch.Request) (int, error) { return 7, nil }

func TestLimitBytesPreservesCapabilities(t *testing.T) {
	t.Parallel()

	inner := countingFetcher{fakeFetcher{batches: []*fetch.Batch{batch(1, [2]int64{10, 1})}}}

	c := fetch.CounterOf(fetch.LimitBytes(inner, 1<<20))
	require.NotNil(t, c, "the limit layer forwards Unwrap so the count pushdown still resolves")

	n, err := c.Count(context.Background(), fetch.Request{})
	require.NoError(t, err)
	assert.Equal(t, 7, n)
}

func TestLimitBytesReleasesRejectedBatch(t *testing.T) {
	t.Parallel()

	var released bool

	over := &fetch.Batch{ID: signal.SeriesID{Lo: 1}, Timestamps: []int64{1, 2, 3, 4}}
	over.SetRelease(func(*fetch.Batch) { released = true })

	it, err := fetch.LimitBytes(fakeFetcher{batches: []*fetch.Batch{over}}, 8).
		Fetch(context.Background(), fetch.Request{})
	require.NoError(t, err)

	_, err = it.Next(context.Background())
	require.ErrorIs(t, err, fetch.ErrTooLarge)
	assert.True(t, released, "the caller never sees the rejected batch, so its buffers go back")
}

func TestLimitBytesPropagatesInnerError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")
	f := errFetcher{err: sentinel}

	it, err := fetch.LimitBytes(f, 1<<20).Fetch(context.Background(), fetch.Request{})
	require.NoError(t, err)

	_, err = it.Next(context.Background())
	assert.ErrorIs(t, err, sentinel, "a producer fault is not reported as a budget overrun")
}

type errFetcher struct{ err error }

func (f errFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	return errIterator(f), nil
}

type errIterator struct{ err error }

func (it errIterator) Next(context.Context) (*fetch.Batch, error) { return nil, it.err }
func (it errIterator) Close() error                               { return nil }
