package fetch

import (
	"context"
	"slices"

	"github.com/oteldb/storage/internal/parallel"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/signal"
)

// Merge returns a [Fetcher] that fans a [Request] out to each child fetcher and merges the
// results by [signal.SeriesID]. Batches that share an id — the same series present in more
// than one child, e.g. equal labels across tenants (cross-tenant / multi-tenant reads) or
// replicas across nodes (cluster fan-out) — are combined into one batch with samples in
// timestamp order, the value from the later child winning on a duplicate timestamp.
//
// The merge is **streaming**: children are opened concurrently but not drained, and one output
// batch is assembled per Next, so peak memory is O(children) batches rather than
// O(children x matched series) — the property the fetch contract promises a consumer that folds
// and releases each batch. This requires each child to yield batches in ascending
// [signal.SeriesID] order, which every producer does (an engine emits in the sorted order its
// postings resolution returns). An id a child emits out of order is not merged with its
// duplicate; it is emitted as its own batch.
//
// With a single child it is a transparent pass-through (no copy or re-sort). The children are
// already bound to their data (a per-tenant engine, a remote node), so each receives the same
// Request and its [Request.Tenant] field is advisory. nil/empty input yields an empty fetcher.
func Merge(fetchers ...Fetcher) Fetcher {
	switch len(fetchers) {
	case 0:
		return emptyFetcher{}
	case 1:
		return fetchers[0]
	default:
		return mergeFetcher(fetchers)
	}
}

// emptyFetcher yields no series.
type emptyFetcher struct{}

func (emptyFetcher) Fetch(context.Context, Request) (Iterator, error) {
	return NewSliceIterator(nil), nil
}

// fanOutConcurrency bounds how many children a merge fans out to at once. Reads are I/O-bound
// (backend round-trips, node RPCs), so this is set above the CPU count to overlap latency while
// still capping in-flight requests on a very wide fan-out.
const fanOutConcurrency = 16

type mergeFetcher []Fetcher

// mergeAcc tracks one merged series and how many children contributed to it (so only
// genuinely cross-child series pay the re-sort/dedup; single-source ones are already
// timestamp-ordered).
type mergeAcc struct {
	b       *Batch
	sources int
}

func (m mergeFetcher) Fetch(ctx context.Context, r Request) (Iterator, error) {
	ctx, pf := profile.Begin(ctx, "fan-out")
	pf.Add("children", int64(len(m)))

	// Children are independent shards/tenants/replicas; open them concurrently (bounded, so a wide
	// fan-out can't spawn an unbounded number of in-flight backend/RPC requests) but keep their
	// iterators: the merge below pulls from them lazily. The per-index slots preserve child order,
	// which decides the duplicate-timestamp winner, regardless of completion order.
	its := make([]Iterator, len(m))
	errs := make([]error, len(m))

	parallel.ForEach(len(m), fanOutConcurrency, func(i int) {
		it, err := m[i].Fetch(ctx, r) // children profile under the fan-out node
		if err != nil {
			errs[i] = err

			return
		}

		its[i] = it
	})

	for _, err := range errs { // surface the first error deterministically (by child index)
		if err != nil {
			closeAll(its)
			pf.End()

			return nil, err
		}
	}

	it := &mergeIter{
		children: its,
		cur:      make([]*Batch, len(its)),
		advance:  make([]bool, len(its)),
		errs:     errs,
		pf:       pf,
	}
	for i := range it.advance {
		it.advance[i] = true // nothing pulled yet: the first Next primes every child
	}

	return it, nil
}

func closeAll(its []Iterator) {
	for _, it := range its {
		if it != nil {
			_ = it.Close()
		}
	}
}

// MergeBatches merges batches from multiple result groups by [signal.SeriesID] into one slice,
// ordered by first appearance. Batches that share an id — the same series in more than one
// group (cluster fan-out across replicas, or the sub-windows of a split-by-interval fetch) —
// are combined into one batch with samples in timestamp order, the value from the later group
// winning on a duplicate timestamp. It is the batch-level form of [Merge]; a series present in
// a single group is copied through unchanged (no re-sort/dedup). Input batches are never
// mutated (a merged batch holds cloned sample columns).
func MergeBatches(groups ...[]*Batch) []*Batch {
	byID := make(map[signal.SeriesID]*mergeAcc)

	var order []signal.SeriesID

	for _, g := range groups {
		for _, b := range g {
			if a, ok := byID[b.ID]; ok {
				a.b.Timestamps = append(a.b.Timestamps, b.Timestamps...)
				a.b.Values = append(a.b.Values, b.Values...)
				a.sources++

				continue
			}

			byID[b.ID] = &mergeAcc{
				b: &Batch{
					ID:         b.ID,
					Series:     b.Series,
					Timestamps: slices.Clone(b.Timestamps),
					Values:     slices.Clone(b.Values),
				},
				sources: 1,
			}
			order = append(order, b.ID)
		}
	}

	out := make([]*Batch, 0, len(order))
	for _, id := range order {
		a := byID[id]
		if a.sources > 1 {
			a.b.Timestamps, a.b.Values = dedupByTimestamp(a.b.Timestamps, a.b.Values)
		}

		out = append(out, a.b)
	}

	return out
}

// dedupByTimestamp sorts (ts, value) pairs by timestamp, keeping the last value seen for a
// duplicate timestamp (the later child wins). Input order is preserved as the tie-break.
func dedupByTimestamp(ts []int64, values []float64) ([]int64, []float64) {
	if len(ts) == 0 {
		return ts, values
	}

	last := make(map[int64]float64, len(ts))
	for i, t := range ts {
		last[t] = values[i]
	}

	outTs := make([]int64, 0, len(last))
	for t := range last {
		outTs = append(outTs, t)
	}

	slices.Sort(outTs)

	outVals := make([]float64, len(outTs))
	for i, t := range outTs {
		outVals[i] = last[t]
	}

	return outTs, outVals
}
