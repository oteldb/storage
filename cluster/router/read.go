package router

import (
	"context"
	"sync/atomic"

	"github.com/go-faster/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/internal/retry"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// The router carries the read RPCs itself — the fetch fan-out below, and the enumeration and
// aggregate clients in enum.go / aggregate.go — so an off-ring reader shares one HTTP client,
// one retry profile and one hedging policy with it, instead of pairing the router with a client
// of its own and failing over sequentially.

// Fetcher returns a [fetch.Fetcher] over one shard, racing its owners under the hedged read policy.
// Every owner holds a complete copy, so the first success is authoritative.
//
// An owner answering [cluster.ErrShardAbsent] is a failover, not a result: it means the ring points
// at that node but the node holds no data for the shard (a fresh owner after a rebalance, a node
// whose membership view lags, one that has not backfilled). Accepting its empty answer would drop
// every row the shard holds elsewhere, so the read moves to the next owner and reports empty only
// when all of them disclaim it.
//
// The owner applies only the serializable ([fetch.Matcher].Spec) subset of a request's matchers, so
// its answer is a superset; the returned fetcher re-applies the full set ([fetch.Filter]) before
// yielding, exactly as the node's own fan-out does. A caller therefore gets what it asked for and
// not a superset it has to know to narrow itself.
func (r *Router) Fetcher(sig signal.Signal, shardKey signal.TenantID) fetch.Fetcher {
	addrs := r.Owners(shardKey)

	remotes := make([]fetch.Fetcher, len(addrs))
	for i, addr := range addrs {
		remotes[i] = cluster.NewRemoteFetcher(sig, addr, r.httpc, r.clusterOpts...)
	}

	return fetch.Filter(hedgedFetcher{
		obs: r.obs, sig: sig, shardKey: shardKey,
		policy: r.readPolicy(), remotes: remotes,
	})
}

// hedgeOwners races a call across a shard's owners under the hedged read policy — the enumeration
// and aggregate twin of [Router.Fetcher]'s fan-out, and the same policy the node's own read path
// uses, so an off-ring reader fails over as fast as a member does. name and extra label the span
// ("cluster.series.hedge", …), which is where hedging and [cluster.ErrShardAbsent] failover across
// a shard's owners are visible end to end.
//
// An owner's [cluster.ErrShardAbsent] is no answer rather than an empty one: the call fails over to
// the next owner and returns the zero value only when every owner disclaims the shard (nothing holds
// it) or the ring has no owner to ask.
func hedgeOwners[T any](
	ctx context.Context, r *Router, name string, shardKey signal.TenantID,
	call func(context.Context, string) (T, error), extra ...attribute.KeyValue,
) (_ T, err error) {
	var zero T

	addrs := r.Owners(shardKey)

	attrs := append([]attribute.KeyValue{
		attribute.String("storage.shard", string(shardKey)),
		attribute.Int("storage.rpc.owners", len(addrs)),
	}, extra...)

	ctx, span := r.obs.Tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	defer func() { endSpan(span, err) }()

	if len(addrs) == 0 {
		return zero, nil
	}

	var absent atomic.Int64

	thunks := make([]func(context.Context) (T, error), len(addrs))
	for i := range addrs {
		addr := addrs[i]
		thunks[i] = func(ctx context.Context) (T, error) {
			v, err := call(ctx, addr)
			if errors.Is(err, cluster.ErrShardAbsent) {
				absent.Add(1)
			}

			return v, err
		}
	}

	v, err := retry.Hedge(ctx, r.readPolicy(), thunks)

	span.SetAttributes(attribute.Int64("storage.rpc.owners_absent", absent.Load()))

	if err != nil && int(absent.Load()) >= len(addrs) {
		err = nil

		return zero, nil
	}

	return v, err
}

// hedgedFetcher races a shard's owners, treating absence as no answer. It is the client-side twin
// of the node's own fan-out, without the node's shard-holding and metering concerns.
type hedgedFetcher struct {
	obs      *obs.Obs
	sig      signal.Signal
	shardKey signal.TenantID
	policy   retry.Policy
	remotes  []fetch.Fetcher
}

func (h hedgedFetcher) Fetch(ctx context.Context, r fetch.Request) (_ fetch.Iterator, err error) {
	//nolint:spancheck // ended by the deferred endSpan below, on every return path
	ctx, span := h.obs.Tracer.Start(ctx, "cluster.fetch.hedge", trace.WithAttributes(
		attribute.String("storage.shard", string(h.shardKey)),
		attribute.String("storage.signal", h.sig.String()),
		attribute.Int("storage.rpc.owners", len(h.remotes)),
	))
	defer func() { endSpan(span, err) }()

	if len(h.remotes) == 0 {
		return nil, errors.New("router: no reachable owners for shard") //nolint:spancheck // ended by the deferred endSpan above
	}

	var absent atomic.Int64

	thunks := make([]func(context.Context) (fetch.Iterator, error), len(h.remotes))
	for i := range h.remotes {
		f := h.remotes[i]
		thunks[i] = func(ctx context.Context) (fetch.Iterator, error) {
			it, err := f.Fetch(ctx, r)
			if errors.Is(err, cluster.ErrShardAbsent) {
				absent.Add(1)
			}

			return it, err
		}
	}

	policy := h.policy
	if len(h.remotes) == 1 {
		// The lone owner's "I do not hold this shard" is final — retrying it only burns the budget.
		policy.Retryable = func(err error) bool {
			return !errors.Is(err, cluster.ErrShardAbsent) && retry.Transient(err)
		}
	}

	it, err := retry.Hedge(ctx, policy, thunks)

	span.SetAttributes(attribute.Int64("storage.rpc.owners_absent", absent.Load()))

	if err != nil && int(absent.Load()) >= len(h.remotes) {
		err = nil

		return fetch.NewSliceIterator(nil), nil
	}

	return it, err
}

// readPolicy is the hedged profile for idempotent fetches: a per-attempt timeout, retries on
// transient transport errors, and opportunistic concurrent attempts across a shard's owners.
func (r *Router) readPolicy() retry.Policy {
	return retry.Policy{
		MaxAttempts:   r.retry.MaxAttempts,
		PerTryTimeout: r.retry.PerTryTimeout,
		BaseBackoff:   r.retry.BaseBackoff,
		MaxBackoff:    r.retry.MaxBackoff,
		HedgeDelay:    r.retry.HedgeDelay,
		Retryable:     retry.Transient,
	}
}
