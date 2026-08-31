package fetch

import (
	"context"

	"github.com/go-faster/errors"
)

// ErrTooLarge is returned by a [LimitBytes] iterator once a query has read more than its byte
// budget. It is a load-shedding signal, not a storage fault: the data is intact and a narrower
// window or a more selective matcher will answer. Embedders should map it to a 4xx (Prometheus and
// Tempo both spell this 422) rather than a 5xx, so a caller retrying the same query verbatim is not
// encouraged.
var ErrTooLarge = errors.New("fetch: query exceeds its read byte budget")

// LimitBytes bounds how many bytes one [Fetcher] call may materialize, so a single query cannot
// exhaust the process. Once a query's cumulative [Batch.Size] passes maxBytes its iterator fails
// with [ErrTooLarge] instead of continuing.
//
// This is deliberately *rejection*, not admission control, and the two are complementary. The
// metric engine's decode budget makes concurrent queries queue behind a shared ceiling, but it
// admits an over-budget query alone rather than refusing it — it bounds queries against each other,
// not any one of them against the process. Nothing bounded a single unbounded read, and the record
// engines (logs, traces, profiles) have no decode admission at all.
//
// The bound is on bytes *read*, not bytes *retained*, which is the stricter of the two readings. A
// caller that streams and releases each batch never holds the total, so it is charged for work it
// does not keep. That is the intended trade: the common consumers accumulate ([Drain] returns every
// batch at once), a bound that only tracked retention would need to know each caller's intent, and
// a query reading tens of gigabytes is worth refusing whether or not it would have survived.
//
// maxBytes ≤ 0 disables the bound and returns inner unchanged, so a caller that bounds reads itself
// pays nothing. The wrapper forwards [Unwraper] so optional capabilities reached through the
// decorator chain — [CounterOf] and the like — still find the underlying fetcher.
func LimitBytes(inner Fetcher, maxBytes int64) Fetcher {
	if maxBytes <= 0 {
		return inner
	}

	return limitFetcher{inner: inner, maxBytes: maxBytes}
}

type limitFetcher struct {
	inner    Fetcher
	maxBytes int64
}

func (f limitFetcher) Fetch(ctx context.Context, r Request) (Iterator, error) {
	it, err := f.inner.Fetch(ctx, r)
	if err != nil {
		return nil, err
	}

	return &limitIterator{inner: it, maxBytes: f.maxBytes}, nil
}

// Unwrap exposes the decorated fetcher so the optional-capability lookups keep working through the
// limit layer, which only counts bytes. See [CounterOf].
func (f limitFetcher) Unwrap() Fetcher { return f.inner }

// limitIterator counts the bytes its batches carry and fails the query once they pass the budget.
type limitIterator struct {
	inner    Iterator
	maxBytes int64
	read     int64
}

func (it *limitIterator) Next(ctx context.Context) (*Batch, error) {
	b, err := it.inner.Next(ctx)
	if err != nil {
		return b, err
	}

	it.read += b.Size()
	if it.read > it.maxBytes {
		// The caller never sees this batch, so its buffers go back to the producer's pool rather
		// than waiting for the GC — the budget is exhausted precisely when that matters most.
		b.Release()

		return nil, errors.Wrapf(ErrTooLarge, "read %d bytes, limit %d", it.read, it.maxBytes)
	}

	return b, nil
}

func (it *limitIterator) Close() error { return it.inner.Close() }
