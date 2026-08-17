package router

import (
	"context"
	"sync/atomic"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/internal/retry"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Fetcher returns a [fetch.Fetcher] over one shard, racing its owners under the hedged read policy.
// Every owner holds a complete copy, so the first success is authoritative.
//
// An owner answering [cluster.ErrShardAbsent] is a failover, not a result: it means the ring points
// at that node but the node holds no data for the shard (a fresh owner after a rebalance, a node
// whose membership view lags, one that has not backfilled). Accepting its empty answer would drop
// every row the shard holds elsewhere, so the read moves to the next owner and reports empty only
// when all of them disclaim it.
func (r *Router) Fetcher(sig signal.Signal, shardKey signal.TenantID) fetch.Fetcher {
	addrs := r.Owners(shardKey)

	remotes := make([]fetch.Fetcher, len(addrs))
	for i, addr := range addrs {
		remotes[i] = cluster.NewRemoteFetcher(sig, addr, r.httpc)
	}

	return hedgedFetcher{policy: r.readPolicy(), remotes: remotes}
}

// hedgedFetcher races a shard's owners, treating absence as no answer. It is the client-side twin
// of the node's own fan-out, without the node's shard-holding and metering concerns.
type hedgedFetcher struct {
	policy  retry.Policy
	remotes []fetch.Fetcher
}

func (h hedgedFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	if len(h.remotes) == 0 {
		return nil, errors.New("router: no reachable owners for shard")
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
	if err != nil && int(absent.Load()) >= len(h.remotes) {
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
