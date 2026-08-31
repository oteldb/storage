package fetch

import (
	"context"
	"sync"

	"github.com/oteldb/storage/internal/readbudget"
)

// ErrTooLarge is returned once a query has asked to hold more memory than its budget allows. It is
// the public sentinel for the whole read path — a part read, a fan-out response body and a
// materialized batch all fail with it — so an embedder matches one error rather than one per layer.
//
// It is load shedding, not a storage fault: the data is intact and a narrower window or a more
// selective matcher will answer. Map it to a 4xx (Prometheus and Tempo both spell this 422) rather
// than a 5xx, so a caller retrying the same query verbatim is not encouraged.
var ErrTooLarge = readbudget.ErrExceeded

// Budgeted charges every batch a fetch materializes against the query's memory budget (see
// [readbudget.With]), failing the query with [ErrTooLarge] once it would hold more than it is
// allowed. A read whose context carries no budget is returned unchanged.
//
// The charge is released when the batch is, so the bound is on what the query holds *at once*
// rather than on what it has read in total. That distinction is the whole reason to account this
// way: a consumer that streams and releases as it goes is charged only for what is in flight, and
// one that accumulates — [Drain] returns every batch at once — is charged for everything it
// accumulates. Neither has to declare which it is.
//
// This is the last of the three charge points and the only one that measures the thing that
// actually exhausts the heap: a decoded, materialized batch. The earlier two (a part read, a fan-out
// body) charge sooner, which lets them refuse before the memory is committed, but they can only
// estimate what the bytes will expand to. Keeping this one is what catches the amplification the
// estimates miss.
//
// The wrapper forwards [Unwraper], so optional capabilities reached through the decorator chain —
// [CounterOf] and the like — still find the underlying fetcher.
func Budgeted(inner Fetcher) Fetcher { return budgetFetcher{inner: inner} }

type budgetFetcher struct{ inner Fetcher }

func (f budgetFetcher) Fetch(ctx context.Context, r Request) (Iterator, error) {
	budget := readbudget.From(ctx)
	if budget == nil {
		return f.inner.Fetch(ctx, r)
	}

	it, err := f.inner.Fetch(ctx, r)
	if err != nil {
		return nil, err
	}

	return &budgetIterator{inner: it, budget: budget}, nil
}

// Unwrap exposes the decorated fetcher so the optional-capability lookups keep working through the
// budget layer, which only counts bytes. See [CounterOf].
func (f budgetFetcher) Unwrap() Fetcher { return f.inner }

// budgetIterator holds a reservation per live batch and returns it when the batch is released, or in
// bulk at Close for a consumer that never releases.
type budgetIterator struct {
	inner  Iterator
	budget *readbudget.Budget

	mu sync.Mutex
	// outstanding is the bytes this iterator has reserved and not yet returned. Guarded by mu, which
	// also orders a batch's release against Close so neither double-releases the other's bytes.
	outstanding int64
	closed      bool
}

func (it *budgetIterator) Next(ctx context.Context) (*Batch, error) {
	b, err := it.inner.Next(ctx)
	if err != nil {
		return b, err
	}

	size := b.Size()
	if err := it.budget.Reserve(size); err != nil {
		// The caller never sees this batch, so its buffers go back to the producer's pool rather than
		// waiting for the GC — the budget is exhausted precisely when that matters most.
		b.Release()

		return nil, err
	}

	it.mu.Lock()
	it.outstanding += size
	it.mu.Unlock()

	it.chargeRelease(b, size)

	return b, nil
}

func (it *budgetIterator) Close() error {
	it.mu.Lock()
	if !it.closed {
		it.closed = true
		it.budget.Release(it.outstanding)
		it.outstanding = 0
	}
	it.mu.Unlock()

	return it.inner.Close()
}

// chargeRelease makes releasing the batch also return its reservation, preserving any hook the
// producer already installed (the buffer pool's) rather than replacing it.
func (it *budgetIterator) chargeRelease(b *Batch, size int64) {
	prev := b.release

	b.release = func(bb *Batch) {
		it.mu.Lock()
		// After Close the reservation was already returned in bulk; releasing again here would credit
		// the budget bytes it never held, letting a later query overdraw.
		if !it.closed {
			it.outstanding -= size
			it.budget.Release(size)
		}
		it.mu.Unlock()

		if prev != nil {
			prev(bb)
		}
	}
}
