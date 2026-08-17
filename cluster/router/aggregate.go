package router

import (
	"context"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Aggregate runs the step-bucketed metric aggregate on one shard's owner, hedged across its owners.
// The owner folds its own samples and ships one compact entry per series — identity + buckets, never
// raw points — so the pushdown the `*_over_time` paths rest on survives being run off the ring.
//
// A step of zero asks for one whole-range bucket per series. Series that fail the full matcher set
// are dropped here: the owner applied only the serializable (equality) subset.
func (r *Router) Aggregate(
	ctx context.Context, shardKey signal.TenantID, start, end, step int64, matchers []fetch.Matcher,
) ([]engine.NamedAgg, error) {
	eq := fetch.EqualitySpecs(matchers)

	aggs, err := hedgeOwners(ctx, r, shardKey, func(ctx context.Context, addr string) ([]engine.NamedAgg, error) {
		return cluster.NewRemoteAggregator(addr, r.httpc).Aggregate(ctx, string(shardKey), start, end, step, eq)
	})
	if err != nil {
		return nil, err
	}

	return filterNamed(aggs, matchers, func(a *engine.NamedAgg) signal.Series { return a.Series }), nil
}

// AggregateWindow is the overlapping-window form of [Router.Aggregate]: the owner slides a range
// vector's evaluation windows itself and returns one entry per series per window.
//
// It is a separate endpoint rather than a widened request on purpose: an owner that predates windows
// answers 404, which fails over, instead of silently returning disjoint step buckets for an
// overlapping question.
func (r *Router) AggregateWindow(
	ctx context.Context, shardKey signal.TenantID, start, end int64, spec engine.WindowSpec,
	matchers []fetch.Matcher,
) ([]engine.NamedWindowAgg, error) {
	eq := fetch.EqualitySpecs(matchers)

	aggs, err := hedgeOwners(ctx, r, shardKey, func(ctx context.Context, addr string) ([]engine.NamedWindowAgg, error) {
		return cluster.NewRemoteAggregator(addr, r.httpc).AggregateWindow(ctx, string(shardKey), start, end, spec, eq)
	})
	if err != nil {
		return nil, err
	}

	return filterNamed(aggs, matchers, func(a *engine.NamedWindowAgg) signal.Series { return a.Series }), nil
}

// filterNamed drops the entries whose identity fails the full matcher set, in place.
func filterNamed[T any](aggs []T, matchers []fetch.Matcher, seriesOf func(*T) signal.Series) []T {
	if len(matchers) == 0 {
		return aggs
	}

	kept := aggs[:0]
	for i := range aggs {
		if fetch.MatchesSeries(seriesOf(&aggs[i]), matchers) {
			kept = append(kept, aggs[i])
		}
	}

	return kept
}
