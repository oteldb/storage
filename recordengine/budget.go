package recordengine

import (
	"context"
	"sync"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
)

// The record engine's read admission, mirroring the metric engine's decode budget: estimate the
// footprint from part metadata *before* reading, reserve it, and release when the caller closes the
// iterator.
//
// The charge has to happen inside planning, not in a decorator around the fetcher and not merely
// before the parts are read. [Engine.Fetch] reads every part, accumulates every survivor and
// materializes every output batch before it returns an iterator at all, so anything wrapping that
// iterator only sees bytes that are already resident. And [Engine.planFetch] pre-sizes each stream's
// accumulator to the rows it will hold, so the largest allocation of the fetch happens during
// planning — earlier still than the first backend read.

// bytesPerRow is the average decoded bytes one accumulated row costs, taken over the parts this
// plan will read.
//
// [part.sizeBytes] is the part's *decoded* footprint as recorded in its manifest, not its compressed
// size, so this needs no guessed expansion factor. Records are variable-width, so it is an average:
// a request whose matched rows are unusually wide is under-counted and one whose rows are narrow is
// over-counted. With no live parts (a head-only read) it falls back to the same per-row constant the
// manifest fallback uses.
func (p *fetchPlan) bytesPerRow() int64 {
	var bytes, rows int64

	for _, part := range p.liveParts {
		bytes += part.sizeBytes()
		rows += part.rows()
	}

	if rows <= 0 {
		return recordRowBytes
	}

	return max(bytes/rows, 1)
}

// admit reserves the footprint of the rows this plan is about to accumulate, returning the release to
// run when the caller is done with the result. It reports an error wrapping [readbudget.ErrExceeded]
// when the read would hold more than the query is allowed.
//
// It must be called before the per-stream accumulators are allocated, not merely before the parts
// are read: [Engine.planFetch] pre-sizes each accumulator to the rows it will hold, so by the time
// planning returns, the largest allocation of the whole fetch has already happened. Admitting after
// that would refuse the query having already committed the memory it was refusing it to save.
//
// A read with no budget on its context is unbounded, and the release is a no-op.
func (p *fetchPlan) admit(ctx context.Context, rows int64) (release func(), _ error) {
	budget := readbudget.From(ctx)
	if budget == nil {
		return func() {}, nil
	}

	n := rows * p.bytesPerRow()
	if err := budget.Reserve(n); err != nil {
		return nil, err
	}

	var once sync.Once

	return func() { once.Do(func() { budget.Release(n) }) }, nil
}

// budgetedIterator returns the plan's reservation when the consumer closes.
//
// The reservation is taken once, up front, so there is no per-batch bookkeeping to get wrong: no
// hook to chain, no outstanding total to reconcile against Close, and no window in which a batch and
// a Close race to release the same bytes.
type budgetedIterator struct {
	fetch.Iterator

	release func()
}

func (it *budgetedIterator) Close() error {
	it.release()

	return it.Iterator.Close()
}
