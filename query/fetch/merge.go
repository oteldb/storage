package fetch

import (
	"context"
	"errors"
	"io"
	"slices"

	"github.com/oteldb/storage/signal"
)

// Merge returns a [Fetcher] that fans a [Request] out to each child fetcher and merges the
// results by [signal.SeriesID]. Batches that share an id — the same series present in more
// than one child, e.g. equal labels across tenants (cross-tenant / multi-tenant reads) or
// replicas across nodes (cluster fan-out) — are combined into one batch with samples in
// timestamp order, the value from the later child winning on a duplicate timestamp.
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

type mergeFetcher []Fetcher

// mergeAcc tracks one merged series and how many children contributed to it (so only
// genuinely cross-child series pay the re-sort/dedup; single-source ones are already
// timestamp-ordered).
type mergeAcc struct {
	b       *Batch
	sources int
}

func (m mergeFetcher) Fetch(ctx context.Context, r Request) (Iterator, error) {
	byID := make(map[signal.SeriesID]*mergeAcc)

	var order []signal.SeriesID

	for _, f := range m {
		it, err := f.Fetch(ctx, r)
		if err != nil {
			return nil, err
		}

		if err := mergeDrain(ctx, it, byID, &order); err != nil {
			_ = it.Close()

			return nil, err
		}

		if err := it.Close(); err != nil {
			return nil, err
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

	return NewSliceIterator(out), nil
}

// mergeDrain folds an iterator's batches into the accumulator: a new id is stored as a fresh
// copy (so a child's batch is never mutated), a repeat id appends its samples.
func mergeDrain(ctx context.Context, it Iterator, byID map[signal.SeriesID]*mergeAcc, order *[]signal.SeriesID) error {
	for {
		b, err := it.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

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
		*order = append(*order, b.ID)
	}
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
