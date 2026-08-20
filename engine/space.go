package engine

import (
	"context"

	"go.uber.org/zap"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"

	"github.com/oteldb/storage/internal/diskguard"
)

// flushObjectsPerPart is how many backend objects one flushed part occupies: its column objects
// (series, timestamps, values, and the optional scale factor), the identity object, the marks and
// the footer. It over-estimates by a couple of objects on purpose — the inode axis is checked once
// for the whole flush, so being generous costs nothing, while being short would admit a flush that
// cannot finish.
const flushObjectsPerPart = 8

// admitFlush refuses a flush the backend has no room for, and clears a previous refusal when it
// finds room again — so the guard is re-evaluated on the maintenance loop's cadence and a node
// recovers on its own once space is freed. It runs *before* the head is detached: a flush that
// cannot land must leave the samples where a later one can still write them.
//
// A probe that fails for its own reasons (a statfs error) is logged and admitted. Refusing writes
// because the capacity check broke would be a worse failure than the one it guards against.
func (e *Engine) admitFlush(ctx context.Context) error {
	need := e.HeadBytes()

	parts := int64(1)
	if e.cfg.MaxPartBytes > 0 {
		parts = need/e.cfg.MaxPartBytes + 1
	}

	err := e.space.Admit(ctx, e.cfg.Backend, need, parts*flushObjectsPerPart+1)
	e.cfg.Obs.Disk.Record(ctx, metricSignal, e.space.Exhausted(), e.space.TakeRejections())

	switch {
	case err == nil:
		return nil
	case !diskguard.IsNoSpace(err):
		zctx.From(ctx).Warn("free capacity probe failed; flushing anyway",
			zap.String("prefix", e.cfg.Prefix), zap.Error(err))

		return nil
	default:
		zctx.From(ctx).Error("refusing to flush: the backend is out of space",
			zap.String("prefix", e.cfg.Prefix), zap.Int64("head_bytes", need), zap.Error(err))

		return errors.Wrap(err, "flush")
	}
}

// refuseWrite is the ingest path's disk-pressure valve: while the medium cannot take a flush, the
// engine rejects writes with an error wrapping [backend.ErrNoSpace] instead of buffering samples
// that will never be stored. Rejecting rather than blocking is deliberate — a full disk is not a
// transient queue, so a waiting writer would only convert an unbounded head into unbounded blocked
// callers — and the error is retryable, so a caller can shed or throttle upstream.
func (e *Engine) refuseWrite() error {
	err := e.space.Refuse()
	if err == nil {
		return nil
	}

	return errors.Wrap(err, "refusing write")
}
