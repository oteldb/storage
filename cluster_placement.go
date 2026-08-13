package storage

import (
	"context"
	"sync/atomic"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/internal/retry"
	"github.com/oteldb/storage/signal"
)

// The read path resolves a shard to a *holder*, not to a ring owner. The two can disagree: a node
// promoted into the owner set by a rebalance has no data until it backfills, and a node whose
// membership view lags the writer's derives a different owner set entirely. Serving the local
// (empty) shard in either case answers successfully with rows missing, which a query engine on top
// cannot distinguish from real data — so a node that owns a shard but does not hold it fails over
// to the owners that do, and only a shard every owner disclaims reads as empty.

// holdsShard reports whether this node has an engine for the shard in sig. An engine exists once
// the shard's data is here: created by the write path, by startup recovery over the local backend's
// parts, or by the bootstrap backfill after a rebalance.
func (s *Storage) holdsShard(sig signal.Signal, shardKey signal.TenantID) bool {
	if sig == signal.Metric {
		_, ok := s.lookupEngine(shardKey)

		return ok
	}

	_, ok := s.lookupRecordEngine(sig, shardKey)

	return ok
}

// shardPlacement resolves where a shard read is served from: locally only when this node both owns
// the shard and holds its data, plus the addresses of the other owners to fail over to.
func (s *Storage) shardPlacement(
	ctx context.Context, op string, sig signal.Signal, shardKey signal.TenantID,
) (local bool, remotes []string) {
	owner, remotes := s.shardOwners(shardKey)
	if !owner {
		return false, remotes
	}

	if s.holdsShard(sig, shardKey) {
		return true, remotes
	}

	s.reportAbsentShard(ctx, op, sig, shardKey, len(remotes))

	return false, remotes
}

// reportAbsentShard meters and logs that the ring points at this node for a shard it does not hold.
func (s *Storage) reportAbsentShard(ctx context.Context, op string, sig signal.Signal, shardKey signal.TenantID, remotes int) {
	s.obs.RPC.ShardAbsent(ctx, op)
	s.obs.Logger(ctx).Warn("owned shard is not held locally, failing over",
		zap.String("op", op), zap.Stringer("signal", sig),
		zap.String("shard", string(shardKey)), zap.Int("remote_owners", remotes))
}

// absentShard names a shard this node owns per the ring but holds no data for. A read seam is built
// without a request context, so it carries the anomaly to the fetch that reports it.
type absentShard struct {
	sig   signal.Signal
	shard signal.TenantID
}

// absentOf returns the carrier when the shard is owned-but-absent here, nil otherwise.
func absentOf(absent bool, sig signal.Signal, shardKey signal.TenantID) *absentShard {
	if !absent {
		return nil
	}

	return &absentShard{sig: sig, shard: shardKey}
}

// hedgeOwners races an RPC across a shard's remote owners under the hedged read policy, treating an
// owner's [cluster.ErrShardAbsent] as no answer rather than as an empty result: the call fails over
// to the next owner, and returns the zero value only when every owner disclaims the shard (it has no
// data anywhere) or there are no owners to ask.
func hedgeOwners[T any](
	ctx context.Context, s *Storage, op string, remotes []string, call func(context.Context, string) (T, error),
) (T, error) {
	var zero T

	if len(remotes) == 0 {
		return zero, nil
	}

	var absent atomic.Int64

	thunks := make([]func(context.Context) (T, error), len(remotes))
	for i := range remotes {
		addr := remotes[i]
		thunks[i] = func(ctx context.Context) (T, error) {
			v, err := call(ctx, addr)
			if errors.Is(err, cluster.ErrShardAbsent) {
				absent.Add(1)
			}

			return v, err
		}
	}

	v, err := retry.Hedge(ctx, s.readPolicy(ctx, op), thunks)
	if err != nil && int(absent.Load()) == len(remotes) {
		s.obs.RPC.ShardAbsent(ctx, op)

		return zero, nil
	}

	return v, err
}
