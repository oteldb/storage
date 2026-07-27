package fetch_test

import (
	"context"
	"io"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// lazyIter is a slice iterator that counts how many batches it has produced and whether it was
// closed — the instrumentation a streaming merge is asserted against.
type lazyIter struct {
	batches  []*fetch.Batch
	i        int
	produced int
	closed   int
	failAt   int // when > 0, Next fails on the failAt'th call
}

func (it *lazyIter) Next(context.Context) (*fetch.Batch, error) {
	if it.failAt > 0 && it.produced+1 == it.failAt {
		return nil, errors.New("child failed")
	}

	if it.i >= len(it.batches) {
		return nil, io.EOF
	}

	b := it.batches[it.i]
	it.i++
	it.produced++

	return b, nil
}

func (it *lazyIter) Close() error {
	it.closed++

	return nil
}

// lazyFetcher hands out one lazyIter, kept for inspection after the merge.
type lazyFetcher struct{ it *lazyIter }

func (f *lazyFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	return f.it, nil
}

// series builds a child's id-sorted batches, one sample each, for the given ids.
func series(ids ...uint64) []*fetch.Batch {
	out := make([]*fetch.Batch, 0, len(ids))
	for _, id := range ids {
		out = append(out, batch(id, [2]int64{int64(id) * 10, int64(id)}))
	}

	return out
}

func ids(batches []*fetch.Batch) []uint64 {
	out := make([]uint64, 0, len(batches))
	for _, b := range batches {
		out = append(out, b.ID.Lo)
	}

	return out
}

// TestMergeStreamsAheadByChildrenOnly is the streaming property: with k children, the merge has
// never pulled more than one batch per child beyond what the consumer has taken, so peak resident
// is O(children) — not O(matched series).
func TestMergeStreamsAheadByChildrenOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const perChild = 64

	children := make([]*lazyIter, 3)
	fetchers := make([]fetch.Fetcher, len(children))

	for c := range children {
		list := make([]uint64, 0, perChild)
		for i := range perChild { // disjoint, ascending ids per child
			list = append(list, uint64(c+1)+uint64(i)*10)
		}

		children[c] = &lazyIter{batches: series(list...)}
		fetchers[c] = &lazyFetcher{it: children[c]}
	}

	it, err := fetch.Merge(fetchers...).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	consumed := 0

	for {
		b, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		require.NotNil(t, b)

		consumed++

		produced := 0
		for _, c := range children {
			produced += c.produced
		}

		assert.LessOrEqualf(t, produced, consumed+len(children),
			"after %d batches consumed the merge must be at most one batch per child ahead", consumed)
	}

	assert.Equal(t, perChild*len(children), consumed, "every series is delivered")
	require.NoError(t, it.Close())

	for i, c := range children {
		assert.Equalf(t, 1, c.closed, "child %d closed exactly once", i)
	}
}

// TestMergeEmitsSortedIDs pins the output order: ascending series id, since the merge is a k-way
// merge over id-sorted children.
func TestMergeEmitsSortedIDs(t *testing.T) {
	t.Parallel()

	a := fakeFetcher{batches: series(1, 5, 9)}
	b := fakeFetcher{batches: series(2, 5, 7)}

	got := drain(t, fetch.Merge(a, b))
	assert.Equal(t, []uint64{1, 2, 5, 7, 9}, ids(got))
}

// TestMergePassesSingleSourceBatchThrough proves a series only one child carries is not copied: the
// consumer gets the child's own batch, so its release hook (and any columns) survive the merge.
func TestMergePassesSingleSourceBatchThrough(t *testing.T) {
	t.Parallel()

	unique := batch(3, [2]int64{10, 1})
	unique.Columns = []fetch.NamedColumn{{Name: "body", Bytes: [][]byte{[]byte("hello")}}}

	released := 0
	unique.SetRelease(func(*fetch.Batch) { released++ })

	a := fakeFetcher{batches: []*fetch.Batch{unique}}
	b := fakeFetcher{batches: series(4)}

	got := drain(t, fetch.Merge(a, b))
	require.Len(t, got, 2)
	assert.Same(t, unique, got[0], "single-source series is passed through, not cloned")
	assert.Zero(t, released, "a passed-through batch is the consumer's to release")

	got[0].Release()
	assert.Equal(t, 1, released, "the child's hook survives the merge")
}

// TestMergeReleasesCombinedChildBatches checks the other half of the lifecycle: a series carried by
// several children is cloned, so the merge releases the children's buffers itself.
func TestMergeReleasesCombinedChildBatches(t *testing.T) {
	t.Parallel()

	left, right := batch(7, [2]int64{10, 1}), batch(7, [2]int64{20, 2})

	releases := 0
	left.SetRelease(func(*fetch.Batch) { releases++ })
	right.SetRelease(func(*fetch.Batch) { releases++ })

	got := drain(t, fetch.Merge(
		fakeFetcher{batches: []*fetch.Batch{left}},
		fakeFetcher{batches: []*fetch.Batch{right}},
	))

	require.Len(t, got, 1)
	assert.Equal(t, []int64{10, 20}, got[0].Timestamps)
	assert.Equal(t, 2, releases, "both contributors' buffers are recycled once cloned")
}

// TestMergeChildErrorIsStickyAndCloses checks that a child's iteration error surfaces from Next,
// stays surfaced (the merge cursor is no longer trustworthy), and that Close still reaches every
// child.
func TestMergeChildErrorIsStickyAndCloses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	bad := &lazyIter{batches: series(1, 2), failAt: 2}
	good := &lazyIter{batches: series(3, 4)}

	it, err := fetch.Merge(&lazyFetcher{it: bad}, &lazyFetcher{it: good}).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	b, err := it.Next(ctx) // id 1 from the failing child, still fine
	require.NoError(t, err)
	assert.Equal(t, signal.SeriesID{Lo: 1}, b.ID)

	_, err = it.Next(ctx)
	require.Error(t, err)
	_, again := it.Next(ctx)
	require.Error(t, again, "the error is sticky")

	require.NoError(t, it.Close())
	assert.Equal(t, 1, bad.closed)
	assert.Equal(t, 1, good.closed, "a failed merge still closes its healthy children")
}

// TestMergeCloseReleasesPendingBatches covers abandoning a merge mid-iteration: the batches still
// held in the children's pending slots are released, not leaked.
func TestMergeCloseReleasesPendingBatches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	pending := batch(9, [2]int64{10, 1})

	released := 0
	pending.SetRelease(func(*fetch.Batch) { released++ })

	it, err := fetch.Merge(
		fakeFetcher{batches: series(1)},
		fakeFetcher{batches: []*fetch.Batch{pending}},
	).Fetch(ctx, fetch.Request{})
	require.NoError(t, err)

	first, err := it.Next(ctx) // id 1; id 9 stays pending in the second child's slot
	require.NoError(t, err)
	assert.Equal(t, signal.SeriesID{Lo: 1}, first.ID)

	require.NoError(t, it.Close())
	assert.Equal(t, 1, released, "the pending batch's buffers are recycled on Close")
	require.NoError(t, it.Close(), "Close is idempotent")
	assert.Equal(t, 1, released)
}

// TestDrainClosesIterator pins the contract streaming producers depend on: draining an iterator
// ends it, so a caller that only drains never leaks the producer's pinned resources.
func TestDrainClosesIterator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	child := &lazyIter{batches: series(1, 2)}

	got, err := fetch.Drain(ctx, child)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, 1, child.closed, "Drain closes what it drained")
}
