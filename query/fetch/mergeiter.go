package fetch

import (
	"context"
	"errors"
	"io"
	"slices"

	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/signal"
)

// mergeIter is the streaming k-way merge over the fan-out's children. It holds at most one pending
// batch per child (`cur`) and, per Next, emits the smallest [signal.SeriesID] among them: the
// children are each id-sorted, so the smallest pending id is globally next and every child carrying
// that id has it pending right now.
//
// A child's pending slot is refilled at the *start* of the following Next, not when it is consumed,
// so a consumer holding the batch we just returned is never racing a producer that reuses buffers,
// and the resident set stays k batches plus the consumer's one. Refills fan out concurrently, so a
// step that advances several children overlaps their backend reads as the old drain-everything
// merge did.
type mergeIter struct {
	children []Iterator
	cur      []*Batch // pending head batch per child; nil ⇒ exhausted, or consumed and awaiting refill
	advance  []bool   // children to refill on the next step
	errs     []error  // scratch for the concurrent refill, reused per step

	pf      *profile.Handle
	err     error // sticky: a child's iteration error ends the merge for every later Next
	batches int64
	closed  bool
}

func (it *mergeIter) Next(ctx context.Context) (*Batch, error) {
	if it.err != nil { // a failed child leaves the merge's cursor untrustworthy: stay failed
		return nil, it.err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := it.fill(ctx); err != nil {
		it.err = err

		return nil, err
	}

	// The smallest pending id, lowest child index first — so a multi-child series is combined in
	// child order and the later child still wins a duplicate timestamp.
	first := -1

	for i, b := range it.cur {
		if b != nil && (first < 0 || b.ID.Less(it.cur[first].ID)) {
			first = i
		}
	}

	if first < 0 {
		return nil, io.EOF
	}

	it.batches++
	id := it.cur[first].ID

	sources := 0 // contributors can only sit at or after first, which holds the lowest of them

	for i := first; i < len(it.cur); i++ {
		if it.cur[i] != nil && it.cur[i].ID == id {
			sources++
		}
	}

	// A series only one child carries passes straight through — no copy, and its release hook (and
	// columns) survive, exactly as the single-child Merge pass-through does.
	if sources == 1 {
		b := it.cur[first]
		it.cur[first] = nil
		it.advance[first] = true

		return b, nil
	}

	return it.combine(first, id), nil
}

// Close closes every child and drops any batch still pending, releasing its buffers. Idempotent.
func (it *mergeIter) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true

	for i, b := range it.cur {
		if b != nil {
			b.Release()
			it.cur[i] = nil
		}
	}

	var err error

	for _, c := range it.children {
		if cerr := c.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}

	it.pf.Add("batches", it.batches)
	it.pf.End()

	return err
}

// combine merges the pending batches sharing id into one owned batch (cloned sample columns, so
// the children's buffers stay untouched and are released here) and marks their children for
// refill. Columns are not carried: only the metric fan-out federates an id across children — the
// record signals concatenate instead — so a merged batch is samples only, as before.
func (it *mergeIter) combine(first int, id signal.SeriesID) *Batch {
	out := &Batch{
		ID:         id,
		Series:     it.cur[first].Series,
		Timestamps: slices.Clone(it.cur[first].Timestamps),
		Values:     slices.Clone(it.cur[first].Values),
	}

	for i := first; i < len(it.cur); i++ {
		b := it.cur[i]
		if b == nil || b.ID != id {
			continue
		}

		if i != first {
			out.Timestamps = append(out.Timestamps, b.Timestamps...)
			out.Values = append(out.Values, b.Values...)
		}

		// The samples are copied out, so the child's buffers are dead: release them to recycle the
		// producing engine's pools (the merged batch carries no hook).
		b.Release()
		it.cur[i] = nil
		it.advance[i] = true
	}

	out.Timestamps, out.Values = dedupByTimestamp(out.Timestamps, out.Values)

	return out
}

// fill refills every child marked for advancing, concurrently when more than one is pending, and
// returns the first error by child index.
func (it *mergeIter) fill(ctx context.Context) error {
	pending := 0

	for _, a := range it.advance {
		if a {
			pending++
		}
	}

	if pending == 0 {
		return nil
	}

	clear(it.errs)

	if pending == 1 { // the common step: one contributor, no goroutine worth spawning
		for i, a := range it.advance {
			if a {
				it.pull(ctx, i)
			}
		}
	} else {
		parallel.ForEach(len(it.children), fanOutConcurrency, func(i int) {
			if it.advance[i] {
				it.pull(ctx, i)
			}
		})
	}

	for _, err := range it.errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// pull reads child i's next batch into its pending slot, leaving it nil at EOF (exhausted).
func (it *mergeIter) pull(ctx context.Context, i int) {
	it.advance[i] = false

	b, err := it.children[i].Next(ctx)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			it.errs[i] = err
		}

		return
	}

	it.cur[i] = b
}
