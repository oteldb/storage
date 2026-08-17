package router

import (
	"context"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/internal/retry"
	"github.com/oteldb/storage/signal"
)

// PrimaryWrite sends a shard's WAL-framed records to its ring primary and returns what the primary
// rejected. The primary is the shard's single authority, so routing every write for a shard to it
// is what makes the admission decision and the accepted set identical across all its replicas.
//
// It retries only when the request provably never reached the server ([retry.ConnFailure]): a write
// is not idempotent at this layer, so one that may have been applied is never re-sent. A caller
// with its own idempotency key (a Kafka offset watermark, say) can retry more aggressively itself.
func (r *Router) PrimaryWrite(
	ctx context.Context, sig signal.Signal, shardKey signal.TenantID, walBytes []byte,
) (cluster.Reject, error) {
	addr, ok := r.Primary(shardKey)
	if !ok {
		return cluster.Reject{}, errors.Errorf("router: no primary for shard %q (empty ring)", shardKey)
	}

	r.lg.Debug("primary-write send",
		zap.String("addr", addr), zap.Stringer("signal", sig),
		zap.String("shard", string(shardKey)), zap.Int("wal_bytes", len(walBytes)))

	payload := cluster.EncodeWrite(sig, string(shardKey), walBytes)

	return retry.Do(ctx, r.writePolicy(), func(ctx context.Context) (cluster.Reject, error) {
		return cluster.SendPrimaryWrite(ctx, r.httpc, addr, payload)
	})
}

// writePolicy is the conservative profile for a non-idempotent write: a per-attempt timeout so a
// stuck primary is abandoned, retries only on a proven connection failure, and no hedging —
// concurrent attempts at a write are unsafe.
func (r *Router) writePolicy() retry.Policy {
	return retry.Policy{
		MaxAttempts:   r.retry.MaxAttempts,
		PerTryTimeout: r.retry.PerTryTimeout,
		BaseBackoff:   r.retry.BaseBackoff,
		MaxBackoff:    r.retry.MaxBackoff,
		Retryable:     retry.ConnFailure,
	}
}
