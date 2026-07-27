package fetch

import (
	"context"
	"io"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/signal"
)

// mergeIter is the streaming k-way merge over the fan-out's children. It holds at most one pending
// batch per child (`cur`) and, per Next, emits the smallest [signal.SeriesID] among them: the
// children are each id-sorted, so the smallest pending id is globally next and every child carrying
// that id has it pending right now.
//
// The ordering is a binary min-heap of child indices keyed on `(pending id, child index)`, so a step
// costs O(log children) rather than a scan of every child — a cross-tenant or wide-shard fan-out has
// one child per engine, which is hundreds, not a handful. The index tie-break is what keeps "later
// child wins" a duplicate timestamp: contributors pop in ascending child order.
//
// A consumed child is refilled at the *start* of the following Next, not when it is consumed, so a
// consumer holding the batch we just returned is never racing a producer that reuses buffers, and the
// resident set stays k batches plus the consumer's one. Refills fan out concurrently, so a step that
// advances several children overlaps their backend reads as the old drain-everything merge did.
type mergeIter struct {
	children []Iterator
	cur      []*Batch // pending head batch per child; nil ⇒ exhausted, or consumed and awaiting refill
	heap     []int32  // child indices ordered by (cur[i].ID, i); children with no pending batch are absent
	refill   []int32  // children consumed by the last step, to be pulled again on the next one
	errs     []error  // scratch for the concurrent refill, reused per step
	// last is each child's previously yielded id (valid once seen[i]), the failsafe for the
	// ascending-order requirement the heap relies on. Checked per pull, so a child that breaks it
	// fails the read loudly instead of silently splitting a series into two batches and skipping its
	// cross-child dedup. The zero SeriesID is a legal id, hence seen rather than a sentinel.
	last []signal.SeriesID
	seen []bool

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

	if len(it.heap) == 0 {
		return nil, io.EOF
	}

	it.batches++

	// Pop every child holding the smallest id. They pop in ascending child index, so the merge order
	// below is child order and the later child still wins a duplicate timestamp.
	first := it.pop()
	id := it.cur[first].ID
	it.refill = append(it.refill, first)

	for len(it.heap) > 0 && it.cur[it.heap[0]].ID == id {
		it.refill = append(it.refill, it.pop())
	}

	// A series only one child carries passes straight through — no copy, and its release hook (and
	// columns) survive, exactly as the single-child Merge pass-through does.
	if len(it.refill) == 1 {
		b := it.cur[first]
		it.cur[first] = nil

		return b, nil
	}

	return it.combine(id), nil
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

// combine merges the batches of the children just popped for id — all of them, held in refill —
// into one owned batch (cloned sample columns, so the children's buffers stay untouched and are
// released here). Columns are not carried: only the metric fan-out federates an id across children —
// the record signals concatenate instead — so a merged batch is samples only, as before.
func (it *mergeIter) combine(id signal.SeriesID) *Batch {
	rows := 0
	for _, i := range it.refill {
		rows += len(it.cur[i].Timestamps)
	}

	out := &Batch{
		ID:         id,
		Series:     it.cur[it.refill[0]].Series,
		Timestamps: make([]int64, 0, rows),
		Values:     make([]float64, 0, rows),
	}

	for _, i := range it.refill {
		b := it.cur[i]
		out.Timestamps, out.Values, out.ScaleFactors = appendSamples(out.Timestamps, out.Values, out.ScaleFactors, b)

		// The samples are copied out, so the child's buffers are dead: release them to recycle the
		// producing engine's pools (the merged batch carries no hook).
		b.Release()
		it.cur[i] = nil
	}

	out.Timestamps, out.Values, out.ScaleFactors = dedupByTimestamp(out.Timestamps, out.Values, out.ScaleFactors)

	return out
}

// fill pulls a fresh batch for every child the last step consumed — concurrently when there is more
// than one, so a federated series' children overlap their reads — and re-heaps those that yielded
// one. It returns the first error by child index.
func (it *mergeIter) fill(ctx context.Context) error {
	if len(it.refill) == 0 {
		return nil
	}

	clear(it.errs)

	if len(it.refill) == 1 { // the common step: one contributor, no goroutine worth spawning
		it.pull(ctx, int(it.refill[0]))
	} else {
		parallel.ForEach(len(it.refill), fanOutConcurrency, func(n int) {
			it.pull(ctx, int(it.refill[n]))
		})
	}

	for _, i := range it.refill {
		if it.cur[i] != nil {
			it.push(i)
		}
	}

	it.refill = it.refill[:0]

	for _, err := range it.errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// pull reads child i's next batch into its pending slot, leaving it nil at EOF (exhausted). A batch
// whose id does not advance breaks the merge's precondition, so it is reported rather than merged:
// the batch is still parked in the slot so Close releases it.
func (it *mergeIter) pull(ctx context.Context, i int) {
	b, err := it.children[i].Next(ctx)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			it.errs[i] = err
		}

		return
	}

	it.cur[i] = b

	if prev := it.last[i]; it.seen[i] && b.ID.Compare(prev) <= 0 {
		it.errs[i] = errors.Errorf(
			"fetch: merge child %d yielded series %s after %s: children must yield ascending series ids",
			i, b.ID, prev)

		return
	}

	it.last[i], it.seen[i] = b.ID, true
}

// less orders the heap: by pending series id, child index breaking a tie (so equal ids pop in child
// order, which is what makes the later child win a duplicate timestamp).
func (it *mergeIter) less(a, b int32) bool {
	if c := it.cur[a].ID.Compare(it.cur[b].ID); c != 0 {
		return c < 0
	}

	return a < b
}

// push adds child i to the heap and sifts it up. Hand-rolled rather than container/heap: that
// interface boxes every element into an `any` (an allocation per push on the read path).
func (it *mergeIter) push(i int32) {
	it.heap = append(it.heap, i)

	for c := len(it.heap) - 1; c > 0; {
		p := (c - 1) / 2
		if !it.less(it.heap[c], it.heap[p]) {
			break
		}

		it.heap[c], it.heap[p] = it.heap[p], it.heap[c]
		c = p
	}
}

// pop removes and returns the heap's smallest child index.
func (it *mergeIter) pop() int32 {
	top := it.heap[0]
	last := len(it.heap) - 1
	it.heap[0] = it.heap[last]
	it.heap = it.heap[:last]

	for p := 0; ; {
		l, r := 2*p+1, 2*p+2
		if l >= last {
			break
		}

		c := l
		if r < last && it.less(it.heap[r], it.heap[l]) {
			c = r
		}

		if !it.less(it.heap[c], it.heap[p]) {
			break
		}

		it.heap[c], it.heap[p] = it.heap[p], it.heap[c]
		p = c
	}

	return top
}
