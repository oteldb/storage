package router

import (
	"context"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Series lists one shard's stream identities matching matchers within the window, hedged across the
// shard's owners. It pushes down the serializable (equality) matchers and re-applies the full set to
// the owner's superset, so the result is exactly what matchers select.
//
// The signal selects the peer's engine: logs, traces, profiles and metric series share one
// enumeration RPC, dispatched by the request's signal byte.
func (r *Router) Series(
	ctx context.Context, sig signal.Signal, shardKey signal.TenantID,
	matchers []fetch.Matcher, start, end int64,
) ([]signal.Series, error) {
	eq := fetch.EqualitySpecs(matchers)

	return hedgeOwners(ctx, r, shardKey, func(ctx context.Context, addr string) ([]signal.Series, error) {
		series, err := cluster.FetchSeries(ctx, r.httpc, addr, sig, string(shardKey), start, end, eq)
		if err != nil {
			return nil, err
		}

		return fetch.FilterSeries(series, matchers), nil
	})
}

// Keys lists one shard's distinct record-attribute keys within the window, with the scope(s) each
// was observed in, hedged across the shard's owners. Keys are window-scoped, not matcher-scoped, so
// nothing is pushed down beyond the window.
func (r *Router) Keys(
	ctx context.Context, sig signal.Signal, shardKey signal.TenantID, start, end int64,
) ([]cluster.KeyInfo, error) {
	return hedgeOwners(ctx, r, shardKey, func(ctx context.Context, addr string) ([]cluster.KeyInfo, error) {
		return cluster.FetchKeys(ctx, r.httpc, addr, sig, string(shardKey), start, end)
	})
}

// Side returns one shard's side-store tables (the profile symbol store, for stack resolution),
// hedged across the shard's owners. Symbols ride the write path, so every owner's copy is complete.
func (r *Router) Side(
	ctx context.Context, sig signal.Signal, shardKey signal.TenantID,
) (map[string][]byte, error) {
	return hedgeOwners(ctx, r, shardKey, func(ctx context.Context, addr string) (map[string][]byte, error) {
		return cluster.FetchSide(ctx, r.httpc, addr, sig, string(shardKey))
	})
}
