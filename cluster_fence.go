package storage

import (
	"context"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/signal"
)

// A node that cannot renew its membership lease keeps its ring frozen at the last view it saw, so
// it goes on resolving itself as the primary of shards etcd has already handed to someone else. Its
// writes are then acknowledged and lost: replicated to owners that no longer exist in the real ring
// (or to nobody), and superseded by the new owner's index the moment it flushes.
//
// The boundary is the lease, not the connection. Every claim hangs off the membership lease, so the
// last instant this node can prove it owns anything is the last keep-alive etcd answered plus the
// TTL — less a margin for clock error and the delay in noticing ([etcd.Membership.FenceDeadline]).
// Past that instant another node's Acquire can already have succeeded, so this node must stop
// acting as any shard's primary, whether or not it can currently reach etcd.
//
// Fencing suppresses the primary role only. Reads stay served — they have their own disclaim path
// ([Storage.canAnswer]) and a stale read is a lesser fault than a lost write — and replicated
// writes arriving from a primary that *can* prove its claim are still applied, since that primary
// is the authoritative one. What the node has already accepted and not yet flushed is kept, not
// dropped and not flushed: dropping loses writes that were properly replicated when they were
// acked, and flushing writes parts under a tenure that has ended. It flushes when (and only when)
// the node can prove the shard is its own again.

// fenced reports whether this node is past its membership lease's fence deadline and so cannot
// prove it owns anything. Always false in single-node mode, where there is no second writer.
func (s *Storage) fenced() bool {
	return s.cluster != nil && s.cluster.membership.Fenced()
}

// checkPrimaryClaim refuses the primary role for shardKey when this node cannot prove it still
// holds the shard's claim, so the write fails at its origin instead of being acknowledged and
// withheld.
func (s *Storage) checkPrimaryClaim(ctx context.Context, sig signal.Signal, shardKey string) error {
	if !s.fenced() {
		return nil
	}

	s.obs.Logger(ctx).Warn("refusing primary write: this node can no longer prove it holds the shard",
		zap.Stringer("signal", sig), zap.String("shard", shardKey))

	return errors.Wrapf(cluster.ErrNotPrimary, "shard %q", shardKey)
}
