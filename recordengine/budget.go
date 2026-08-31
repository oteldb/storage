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
// The charge has to happen here rather than in a decorator around the fetcher. [Engine.Fetch] reads
// every part, accumulates every survivor and materializes every output batch before it returns an
// iterator at all, so anything wrapping that iterator only sees batches that are already resident —
// it would reject a query after the allocation it was meant to prevent.

// estimateBytes estimates the decoded bytes this plan will hold once it has read its parts.
//
// It is the record twin of the metric engine's decode estimate, and it is honest for the same
// reason: [part.sizeBytes] is the part's *decoded* footprint as recorded in its manifest, not its
// compressed size, so scaling it by the share of rows this request touches needs no guessed
// expansion factor. Records are variable-width, so the per-part average is the best the metadata
// supports — a part whose matched rows are unusually wide is under-counted and one whose rows are
// narrow is over-counted.
//
// It counts whole stream ranges rather than the window's slice of them, so it over-estimates a
// request that touches part of a stream's run. That is the safe direction for admission, and the
// alternative — resolving granules per stream — costs the backend reads the estimate exists to
// avoid.
func (p *fetchPlan) estimateBytes() int64 {
	var (
		total   int64
		matched []streamRange
	)

	for _, part := range p.liveParts {
		rows := part.rows()
		if rows <= 0 {
			continue
		}

		matched = part.heldStreams(matched[:0], p.sortedIDs)

		var n int64
		for _, sr := range matched {
			n += int64(sr.end - sr.start)
		}

		if n <= 0 {
			continue
		}

		total += n * part.sizeBytes() / rows
	}

	return total
}

// admit reserves the plan's estimated footprint against the query's budget, returning the release to
// run when the caller is done with the result. It reports an error wrapping
// [readbudget.ErrExceeded] when the read would hold more than the query is allowed, before any part
// is read.
//
// A read with no budget on its context is unbounded, and the release is a no-op.
func (p *fetchPlan) admit(ctx context.Context) (release func(), _ error) {
	budget := readbudget.From(ctx)
	if budget == nil {
		return func() {}, nil
	}

	n := p.estimateBytes()
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
