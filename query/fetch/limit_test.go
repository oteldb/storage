package fetch_test

import (
	"context"
	"io"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

// budgeted returns a context carrying a budget of n bytes, plus the budget itself.
func budgeted(n int64) (context.Context, *readbudget.Budget) {
	b := readbudget.New(n)

	return readbudget.With(context.Background(), b), b
}

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

func TestBudgetedRejectsPastBudget(t *testing.T) {
	t.Parallel()

	// Three batches of 32 bytes each; the budget admits two.
	f := fakeFetcher{batches: []*fetch.Batch{
		batch(1, [2]int64{10, 1}, [2]int64{20, 2}),
		batch(2, [2]int64{30, 3}, [2]int64{40, 4}),
		batch(3, [2]int64{50, 5}, [2]int64{60, 6}),
	}}

	ctx, _ := budgeted(64)

	it, err := fetch.Budgeted(f).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	got, err := fetch.Drain(ctx, it)
	require.ErrorIs(t, err, fetch.ErrTooLarge)
	assert.Len(t, got, 2, "everything admitted before the budget ran out is still returned")
}

// The reason to account by hold-and-release rather than by bytes read: a consumer that releases as it
// goes never holds more than one batch, so an arbitrarily long stream fits a small budget. The same
// fetch drained into memory would not.
func TestBudgetedStreamingConsumerIsNotCharged(t *testing.T) {
	t.Parallel()

	batches := make([]*fetch.Batch, 0, 100)

	for i := range 100 {
		batches = append(batches, batch(uint64(i), [2]int64{int64(i), 1}, [2]int64{int64(i) + 1, 2}))
	}

	ctx, budget := budgeted(64) // room for two batches at a time, out of a hundred

	it, err := fetch.Budgeted(fakeFetcher{batches: batches}).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	var n int

	for {
		b, err := it.Next(ctx)
		if err != nil {
			require.ErrorIs(t, err, io.EOF, "the stream ends cleanly, never over budget")

			break
		}

		n++

		b.Release()
	}

	assert.Equal(t, 100, n, "every batch was delivered")
	assert.Equal(t, int64(64), budget.Remaining(), "a released stream ends holding nothing")
}

func TestBudgetedAdmitsUnderBudget(t *testing.T) {
	t.Parallel()

	f := fakeFetcher{batches: []*fetch.Batch{
		batch(1, [2]int64{10, 1}, [2]int64{20, 2}),
		batch(2, [2]int64{30, 3}, [2]int64{40, 4}),
	}}

	ctx, _ := budgeted(1 << 20)

	it, err := fetch.Budgeted(f).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	assert.Len(t, got, 2, "a query inside its budget is untouched")
}

func TestBudgetedWithoutBudgetIsPassthrough(t *testing.T) {
	t.Parallel()

	f := fakeFetcher{batches: []*fetch.Batch{batch(1, [2]int64{10, 1})}}

	// No budget on the context at all: the wrapper must not invent one.
	assert.Len(t, drain(t, fetch.Budgeted(f)), 1)
}

// Close returns what a consumer never released, so one query's leftovers do not permanently shrink
// the next query's room.
func TestBudgetedCloseReleasesOutstanding(t *testing.T) {
	t.Parallel()

	ctx, budget := budgeted(1 << 20)
	f := fakeFetcher{batches: []*fetch.Batch{
		batch(1, [2]int64{10, 1}, [2]int64{20, 2}),
		batch(2, [2]int64{30, 3}, [2]int64{40, 4}),
	}}

	it, err := fetch.Budgeted(f).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	_, err = fetch.Drain(ctx, it) // Drain accumulates and releases nothing
	require.NoError(t, err)

	assert.Equal(t, int64(1<<20), budget.Remaining(), "Close hands the whole reservation back")
}

// A batch released after Close must not credit the budget a second time — that would let a later
// query overdraw by however much the first one leaked.
func TestBudgetedReleaseAfterCloseDoesNotDoubleCredit(t *testing.T) {
	t.Parallel()

	ctx, budget := budgeted(1 << 20)
	f := fakeFetcher{batches: []*fetch.Batch{batch(1, [2]int64{10, 1}, [2]int64{20, 2})}}

	it, err := fetch.Budgeted(f).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	got, err := it.Next(ctx)
	require.NoError(t, err)
	require.NoError(t, it.Close())

	got.Release()

	assert.Equal(t, int64(1<<20), budget.Remaining(), "the reservation is returned exactly once")
}

// countingFetcher is a [fetch.Fetcher] that also answers the count() pushdown, so the test can check
// the budget wrapper does not hide an optional capability from [fetch.CounterOf].
type countingFetcher struct{ fakeFetcher }

func (countingFetcher) Count(context.Context, fetch.Request) (int, error) { return 7, nil }

func TestBudgetedPreservesCapabilities(t *testing.T) {
	t.Parallel()

	inner := countingFetcher{fakeFetcher{batches: []*fetch.Batch{batch(1, [2]int64{10, 1})}}}

	c := fetch.CounterOf(fetch.Budgeted(inner))
	require.NotNil(t, c, "the budget layer forwards Unwrap so the count pushdown still resolves")

	n, err := c.Count(context.Background(), fetch.Request{})
	require.NoError(t, err)
	assert.Equal(t, 7, n)
}

func TestBudgetedReleasesRejectedBatch(t *testing.T) {
	t.Parallel()

	var released bool

	over := &fetch.Batch{ID: signal.SeriesID{Lo: 1}, Timestamps: []int64{1, 2, 3, 4}}
	over.SetRelease(func(*fetch.Batch) { released = true })

	ctx, _ := budgeted(8)

	it, err := fetch.Budgeted(fakeFetcher{batches: []*fetch.Batch{over}}).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	_, err = it.Next(ctx)
	require.ErrorIs(t, err, fetch.ErrTooLarge)
	assert.True(t, released, "the caller never sees the rejected batch, so its buffers go back")
}

// The producer's own release hook must survive being wrapped, or a budgeted read stops recycling
// buffers and quietly costs more than it saves.
func TestBudgetedPreservesProducerReleaseHook(t *testing.T) {
	t.Parallel()

	var recycled bool

	b := &fetch.Batch{ID: signal.SeriesID{Lo: 1}, Timestamps: []int64{1, 2}}
	b.SetRelease(func(*fetch.Batch) { recycled = true })

	ctx, budget := budgeted(1 << 20)

	it, err := fetch.Budgeted(fakeFetcher{batches: []*fetch.Batch{b}}).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	got, err := it.Next(ctx)
	require.NoError(t, err)

	got.Release()

	assert.True(t, recycled, "the producer's pool hook still runs")
	assert.Equal(t, int64(1<<20), budget.Remaining(), "and the reservation came back too")
}

func TestBudgetedPropagatesInnerError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")

	ctx, _ := budgeted(1 << 20)

	it, err := fetch.Budgeted(errFetcher{err: sentinel}).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	_, err = it.Next(ctx)
	assert.ErrorIs(t, err, sentinel, "a producer fault is not reported as a budget overrun")
}

type errFetcher struct{ err error }

func (f errFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	return errIterator(f), nil
}

type errIterator struct{ err error }

func (it errIterator) Next(context.Context) (*fetch.Batch, error) { return nil, it.err }
func (it errIterator) Close() error                               { return nil }
