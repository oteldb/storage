// Package scale provides fetch-seam scale-out primitives: [Fetcher → Fetcher] decorators that
// any embedder's query engine composes over [fetch.Fetcher] (a per-tenant engine, a cluster
// fan-out, a cross-tenant [fetch.Merge]) without the library owning a query language.
//
//   - [SplitFetcher] splits a wide time window into aligned sub-intervals fetched in parallel under
//     a bound, the query-frontend "split by interval" technique applied to the fetch contract.
//   - [CacheFetcher] memoizes results of fully-pushable (serializable-equality) requests.
//
// Both implement [fetch.Fetcher], so they nest freely — e.g. a SplitFetcher over a CacheFetcher
// over a cluster fetcher caches each aligned sub-window independently.
package scale

import (
	"context"

	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/query/fetch"
)

// splitConcurrency bounds how many sub-window fetches a split runs at once. A split is unbounded in
// the *request* (a 30-day window at hourly interval is 720 sub-windows), and each in-flight sub-fetch
// pins its parts and reserves its decode footprint from the engine's memory budget — so running them
// all at once contends on exactly the resource that budget exists to protect, buying no throughput.
// Matched to the fan-out cap in query/fetch for the same reason: reads are I/O-bound, so a handful
// above the CPU count overlaps latency without swamping the backend.
const splitConcurrency = 16

// SplitFetcher fetches a [fetch.Request] as a set of aligned sub-windows of width Interval,
// run concurrently (at most [splitConcurrency] at a time) against Inner and merged by series.
// Splitting on a fixed grid (aligned to multiples of Interval) makes each sub-window's bounds
// independent of the overall request, so
// overlapping queries reuse the same sub-windows — the property a downstream [CacheFetcher]
// relies on for hits across shifting ranges.
type SplitFetcher struct {
	Inner    fetch.Fetcher
	Interval int64 // sub-window width in nanos; ≤ 0 disables splitting (transparent pass-through)
}

var _ fetch.Fetcher = SplitFetcher{}

// Fetch splits r's window into aligned sub-windows, fetches them from Inner under a concurrency
// bound, and returns their merged batches. A request spanning a single sub-window (or with Interval ≤ 0)
// passes straight through, so splitting never adds overhead to a narrow query.
func (f SplitFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if f.Interval <= 0 || r.End < r.Start {
		return f.Inner.Fetch(ctx, r)
	}

	windows := splitWindows(r.Start, r.End, f.Interval)
	if len(windows) <= 1 {
		return f.Inner.Fetch(ctx, r)
	}

	groups := make([][]*fetch.Batch, len(windows))
	errs := make([]error, len(windows))

	// Bounded: at most splitConcurrency sub-fetches are in flight, collected into per-index slots so
	// the group order (which decides the duplicate-timestamp winner in MergeBatches) is independent of
	// completion order.
	parallel.ForEach(len(windows), splitConcurrency, func(i int) {
		sub := r
		sub.Start, sub.End = windows[i].lo, windows[i].hi

		it, err := f.Inner.Fetch(ctx, sub)
		if err != nil {
			errs[i] = err

			return
		}

		batches, derr := fetch.Drain(ctx, it) // Drain closes the iterator
		if derr != nil {
			errs[i] = derr

			return
		}

		groups[i] = batches
	})

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return fetch.NewSliceIterator(fetch.MergeBatches(groups...)), nil
}

// Unwrap exposes the split fetcher's inner so [fetch.CounterOf] can reach the engine's Count for
// the count() pushdown: count is evaluated over the full [Start, End] on Inner directly (a count
// is not a split-friendly aggregate — a series active in two sub-windows counts once for the whole
// range, so summing per-window counts would over-count).
func (f SplitFetcher) Unwrap() fetch.Fetcher { return f.Inner }

// window is one inclusive sub-range [lo, hi].
type window struct{ lo, hi int64 }

// splitWindows divides the inclusive window [start, end] into sub-windows aligned to multiples
// of interval (so a sub-window's bounds depend only on the grid, not on start). The first and
// last sub-windows are clamped to [start, end]; interval > 0 is guaranteed by the caller.
func splitWindows(start, end, interval int64) []window {
	// Align the first grid line at or below start (floor division that is correct for
	// negative timestamps, where Go's % can be negative).
	rem := start % interval
	if rem < 0 {
		rem += interval
	}

	base := start - rem

	var ws []window

	for lo := base; lo <= end; lo += interval {
		a := max(lo, start)
		b := min(lo+interval-1, end)

		if a <= b {
			ws = append(ws, window{lo: a, hi: b})
		}
	}

	return ws
}
