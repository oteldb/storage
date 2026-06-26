// Package replica is the L0 write-replication layer: it fans an opaque write payload out to
// the ring-owners of a key and returns once a write **quorum** has durably applied it, so the
// unflushed head survives the loss of a minority of replicas (DESIGN.md §11; RF=3, quorum
// (RF/2)+1=2). The storage library owns the node-to-node transport (see [Transport] and the
// HTTP implementation in this package) rather than delegating it to the embedder.
//
// The replicator is decoupled from the ring and membership: the caller resolves a key to its
// owners' [Target]s (network addresses), and a target equal to the local node is applied in
// process instead of sent over the wire.
package replica

import (
	"context"

	"github.com/go-faster/errors"
)

// Transport sends a replicated write payload to a peer node at addr and returns when the peer
// has applied it (ack) or an error if it did not. Implementations must be safe for concurrent
// use. The HTTP implementation is [NewHTTPTransport].
type Transport interface {
	Send(ctx context.Context, addr string, payload []byte) error
}

// Target is one replica destination: a node's network address.
type Target struct {
	Addr string
}

// ApplyFunc applies a replicated write to the local store (decoding the payload). It is what
// a [Target] equal to the local node runs in process, and what the receiving side of the
// transport runs for a remote write.
type ApplyFunc func(ctx context.Context, payload []byte) error

// Replicator fans writes out to replica targets and enforces write quorum.
type Replicator struct {
	self      string // this node's address; a target with this Addr is applied locally
	transport Transport
	apply     ApplyFunc
}

// New returns a replicator for the local node at self, using transport for remote sends and
// apply for local (and, on the receiving side, remote) application.
func New(self string, transport Transport, apply ApplyFunc) *Replicator {
	return &Replicator{self: self, transport: transport, apply: apply}
}

// Apply applies a payload locally — the entry point the transport's receiving side calls for
// an inbound replicated write.
func (r *Replicator) Apply(ctx context.Context, payload []byte) error {
	return r.apply(ctx, payload)
}

// ErrNoTargets is returned by [Replicator.Replicate] when no replica targets are given.
var ErrNoTargets = errors.New("replica: no targets")

// Replicate sends payload to every target (the local one applied in process, the rest over
// the transport) and returns nil as soon as a quorum — (len(targets)/2)+1 — has acked. It
// returns early with an error once enough targets have failed that quorum is unreachable. The
// non-quorum targets still receive the write (best-effort) so all replicas converge; only the
// wait is quorum-bounded.
func (r *Replicator) Replicate(ctx context.Context, targets []Target, payload []byte) error {
	return r.ReplicateQuorum(ctx, targets, payload, len(targets)/2+1)
}

// ReplicateQuorum is [Replicator.Replicate] with an explicit required-ack count, for callers
// that compute quorum themselves — e.g. a shard primary that has already applied locally and
// needs only (RF/2) more acks from its secondaries. A quorum ≤ 0 returns immediately (the
// caller's own copy suffices); a quorum exceeding len(targets) can never be met.
func (r *Replicator) ReplicateQuorum(ctx context.Context, targets []Target, payload []byte, quorum int) error {
	if quorum <= 0 {
		// Fan out best-effort, wait for none (the caller already holds a durable copy).
		for _, t := range targets {
			go func(addr string) {
				if addr == r.self {
					_ = r.apply(ctx, payload)

					return
				}

				_ = r.transport.Send(ctx, addr, payload)
			}(t.Addr)
		}

		return nil
	}

	if len(targets) == 0 {
		return ErrNoTargets
	}

	results := make(chan error, len(targets)) // buffered so stragglers never block

	for _, t := range targets {
		go func(addr string) {
			if addr == r.self {
				results <- r.apply(ctx, payload)

				return
			}

			results <- r.transport.Send(ctx, addr, payload)
		}(t.Addr)
	}

	var acks, fails int

	var lastErr error

	for range targets {
		err := <-results
		if err == nil {
			acks++
			if acks >= quorum {
				return nil
			}

			continue
		}

		lastErr = err
		fails++
		// Once too many have failed, quorum can no longer be reached.
		if len(targets)-fails < quorum {
			return errors.Wrapf(lastErr, "replica: quorum %d/%d not met (%d failed)", quorum, len(targets), fails)
		}
	}

	return errors.Wrapf(lastErr, "replica: quorum %d/%d not met", quorum, len(targets))
}
