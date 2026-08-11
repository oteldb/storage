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
// timestamp order, the value from the later child winning on a duplicate timestamp. Each sample keeps
// the [Batch.ScaleFactors] weight it arrived with, so federating a sampled tenant's series does not
// silently reset its weights to 1.
//
// The merge is **streaming**: children are opened concurrently but not drained, and one output
// batch is assembled per Next, so peak memory is O(children) batches rather than
// O(children x matched series) — the property the fetch contract promises a consumer that folds
// and releases each batch. This requires each child to yield batches in ascending
// [signal.SeriesID] order, which every producer here does (an engine emits in the sorted order its
// postings resolution returns; [MergeBatches] sorts). A child that breaks it is **reported, not
// silently mis-merged**: the merge fails the iteration rather than emitting one series as two
// batches with its cross-child dedup skipped.
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
		heap:     make([]int32, 0, len(its)),
		refill:   make([]int32, len(its)),
		last:     make([]signal.SeriesID, len(its)),
		seen:     make([]bool, len(its)),
		errs:     errs,
		pf:       pf,
	}
	for i := range it.refill {
		it.refill[i] = int32(i) // nothing pulled yet: the first Next primes every child
	}

	return it, nil
}

// Series implements [SeriesLister] for the fan-out. Unlike counting, enumeration composes across
// children: identities are content-addressed, so the union is a **sort-merge** over the children's
// ascending id streams — a series present in more than one child (replicas of a shard, or the same
// labels in two tenants) is emitted once, matching how [Merge] federates it into one series, and the
// result stays ascending like every other producer here, so it is itself a legal child.
//
// Children are asked concurrently, each through [SeriesListerOf] so a child's own decorators are
// seen through; a child that does not implement the capability contributes the identities of a plain
// Fetch, so a partially-capable fan-out still answers exactly rather than dropping that child's
// series. A child that breaks the ascending contract is sorted rather than mis-merged (the check is
// one linear pass, and no producer here should ever trip it).
func (m mergeFetcher) Series(ctx context.Context, r Request) ([]signal.Series, error) {
	ctx, pf := profile.Begin(ctx, "fan-out")
	pf.Add("children", int64(len(m)))

	defer pf.End()

	var (
		lists = make([]seriesList, len(m))
		errs  = make([]error, len(m))
	)

	parallel.ForEach(len(m), fanOutConcurrency, func(i int) {
		series, err := listSeries(ctx, m[i], r)
		if err != nil {
			errs[i] = err

			return
		}

		lists[i] = newSeriesList(series)
	})

	for _, err := range errs { // surface the first error deterministically (by child index)
		if err != nil {
			return nil, err
		}
	}

	return mergeSeriesLists(lists), nil
}

// seriesList is one child's enumeration result paired with each identity's id, in ascending id
// order — the sort-merge's input cursor.
type seriesList struct {
	series []signal.Series
	ids    []signal.SeriesID
	i      int
}

// newSeriesList derives the ids (identities are content-addressed, so the id is the hash of the
// identity — the same value the producer's Batch.ID carries) and restores ascending order if the
// child did not honor it.
func newSeriesList(series []signal.Series) seriesList {
	var (
		ids    = make([]signal.SeriesID, len(series))
		buf    []byte
		sorted = true
	)

	for i := range series {
		buf = series[i].AppendHashInput(buf[:0])
		ids[i] = signal.HashBytes(buf)

		if i > 0 && ids[i].Compare(ids[i-1]) < 0 {
			sorted = false
		}
	}

	l := seriesList{series: series, ids: ids}
	if !sorted {
		l.sort()
	}

	return l
}

func (l *seriesList) sort() {
	order := make([]int, len(l.ids))
	for i := range order {
		order[i] = i
	}

	slices.SortFunc(order, func(a, b int) int { return l.ids[a].Compare(l.ids[b]) })

	series := make([]signal.Series, len(order))
	ids := make([]signal.SeriesID, len(order))

	for k, i := range order {
		series[k], ids[k] = l.series[i], l.ids[i]
	}

	l.series, l.ids = series, ids
}

// mergeSeriesLists k-way merges the children's ascending id streams into one deduplicated ascending
// slice. The cursor scan is linear in the child count (a fan-out has one child per tenant/shard, a
// handful), so it costs no heap and no per-id map.
func mergeSeriesLists(lists []seriesList) []signal.Series {
	total := 0
	for i := range lists {
		total += len(lists[i].series)
	}

	out := make([]signal.Series, 0, total)

	for {
		next := -1

		for i := range lists {
			if lists[i].i >= len(lists[i].ids) {
				continue
			}

			if next < 0 || lists[i].ids[lists[i].i].Compare(lists[next].ids[lists[next].i]) < 0 {
				next = i
			}
		}

		if next < 0 {
			return out
		}

		id := lists[next].ids[lists[next].i]
		out = append(out, lists[next].series[lists[next].i])

		// Drop this id everywhere: a series carried by several children is one series.
		for i := range lists {
			for lists[i].i < len(lists[i].ids) && lists[i].ids[lists[i].i].Compare(id) == 0 {
				lists[i].i++
			}
		}
	}
}

// LabelNames implements [LabelLister] for the fan-out: the children's sorted name sets are unioned
// (label metadata is a set, so the union is the whole composition — no dedup subtlety like the
// sample merge's). A child without the capability is skipped rather than fetched: unlike identity
// enumeration there is no way to derive names from a child's batches more cheaply than the caller's
// own fallback, so a partially-capable chain must not silently answer from a subset. That case is
// kept out by construction — [LabelListerOf] finds this fan-out only when every child is capable.
func (m mergeFetcher) LabelNames(ctx context.Context, r Request) ([]string, error) {
	return m.mergeLabels(ctx, func(ctx context.Context, l LabelLister) ([]string, error) {
		return l.LabelNames(ctx, r)
	})
}

// LabelValues is [mergeFetcher.LabelNames] for one name's values.
func (m mergeFetcher) LabelValues(ctx context.Context, r Request, name []byte) ([]string, error) {
	return m.mergeLabels(ctx, func(ctx context.Context, l LabelLister) ([]string, error) {
		return l.LabelValues(ctx, r, name)
	})
}

// mergeLabels asks every child concurrently and unions their sorted results. A child lacking the
// capability makes the whole call unsupported (ErrLabelsUnsupported), so the caller falls back to a
// path that sees every child.
func (m mergeFetcher) mergeLabels(
	ctx context.Context, ask func(context.Context, LabelLister) ([]string, error),
) ([]string, error) {
	ctx, pf := profile.Begin(ctx, "fan-out")
	pf.Add("children", int64(len(m)))

	defer pf.End()

	var (
		lists = make([][]string, len(m))
		errs  = make([]error, len(m))
	)

	parallel.ForEach(len(m), fanOutConcurrency, func(i int) {
		l := LabelListerOf(m[i])
		if l == nil {
			errs[i] = ErrLabelsUnsupported

			return
		}

		lists[i], errs[i] = ask(ctx, l)
	})

	for _, err := range errs { // surface the first error deterministically (by child index)
		if err != nil {
			return nil, err
		}
	}

	var out []string
	for _, list := range lists {
		out = append(out, list...)
	}

	slices.Sort(out)

	return slices.Compact(out), nil
}

// listSeries enumerates one child's matching identities: through its [SeriesLister] when it has
// one, else by draining a fetch and keeping only the identities.
func listSeries(ctx context.Context, f Fetcher, r Request) ([]signal.Series, error) {
	if l := SeriesListerOf(f); l != nil {
		return l.Series(ctx, r)
	}

	it, err := f.Fetch(ctx, r)
	if err != nil {
		return nil, err
	}

	batches, err := Drain(ctx, it)
	if err != nil {
		return nil, err
	}

	out := make([]signal.Series, 0, len(batches))
	for _, b := range batches {
		out = append(out, b.Series)
	}

	return out, nil
}

func closeAll(its []Iterator) {
	for _, it := range its {
		if it != nil {
			_ = it.Close()
		}
	}
}

// MergeBatches merges batches from multiple result groups by [signal.SeriesID] into one slice,
// **ascending by id** — the order every [Iterator] in this package yields, so a merged slice served
// as one (a split-by-interval fetch) is a legal child of a streaming [Merge]. Batches that share an id — the same series in more than one
// group (cluster fan-out across replicas, or the sub-windows of a split-by-interval fetch) —
// are combined into one batch with samples in timestamp order, the value from the later group
// winning on a duplicate timestamp. It is the batch-level form of [Merge]; a series present in
// a single group is copied through unchanged (no re-sort/dedup of its samples). Input batches are
// never mutated (a merged batch holds cloned sample columns).
func MergeBatches(groups ...[]*Batch) []*Batch {
	byID := make(map[signal.SeriesID]*mergeAcc)

	var order []signal.SeriesID

	for _, g := range groups {
		for _, b := range g {
			if a, ok := byID[b.ID]; ok {
				a.b.Timestamps, a.b.Values, a.b.ScaleFactors = appendSamples(
					a.b.Timestamps, a.b.Values, a.b.ScaleFactors, b)
				a.sources++

				continue
			}

			nb := &Batch{ID: b.ID, Series: b.Series}
			nb.Timestamps, nb.Values, nb.ScaleFactors = appendSamples(nil, nil, nil, b)

			byID[b.ID] = &mergeAcc{b: nb, sources: 1}
			order = append(order, b.ID)
		}
	}

	slices.SortFunc(order, signal.SeriesID.Compare)

	out := make([]*Batch, 0, len(order))
	for _, id := range order {
		a := byID[id]
		if a.sources > 1 {
			a.b.Timestamps, a.b.Values, a.b.ScaleFactors = dedupByTimestamp(
				a.b.Timestamps, a.b.Values, a.b.ScaleFactors)
		}

		out = append(out, a.b)
	}

	return out
}

// dedupByTimestamp sorts samples by timestamp, keeping the last one seen for a duplicate timestamp
// (the later child wins). Input order is preserved as the tie-break. It indexes rather than copying
// values so the winning sample's **weight** travels with its value: dropping the weight would make a
// federated sampled series read as if every sample counted once. A nil sf stays nil (all weights 1).
func dedupByTimestamp(ts []int64, values, sf []float64) ([]int64, []float64, []float64) {
	if len(ts) == 0 {
		return ts, values, sf
	}

	lastAt := make(map[int64]int, len(ts))
	for i, t := range ts {
		lastAt[t] = i
	}

	outTs := make([]int64, 0, len(lastAt))
	for t := range lastAt {
		outTs = append(outTs, t)
	}

	slices.Sort(outTs)

	outVals := make([]float64, len(outTs))

	var outSF []float64
	if sf != nil {
		outSF = make([]float64, len(outTs))
	}

	for i, t := range outTs {
		j := lastAt[t]
		outVals[i] = values[j]

		if outSF != nil {
			outSF[i] = sf[j]
		}
	}

	return outTs, outVals, outSF
}

// appendSamples appends b's samples to the (ts, values, sf) accumulator a merge builds. sf stays nil
// while no source carries weights; the first weighted source materializes unit weights for whatever
// was accumulated before it, so federating a sampled child with an unsampled one keeps the sampled
// side's weights instead of discarding them (see [Batch.ScaleFactors]).
func appendSamples(ts []int64, values, sf []float64, b *Batch) ([]int64, []float64, []float64) {
	rows := len(b.Timestamps)

	switch {
	case sf == nil && b.ScaleFactors != nil:
		sf = make([]float64, len(ts), len(ts)+rows)
		for i := range sf {
			sf[i] = 1
		}

		sf = append(sf, b.ScaleFactors...)
	case sf != nil && b.ScaleFactors == nil:
		for range rows {
			sf = append(sf, 1)
		}
	case sf != nil:
		sf = append(sf, b.ScaleFactors...)
	}

	return append(ts, b.Timestamps...), append(values, b.Values...), sf
}
