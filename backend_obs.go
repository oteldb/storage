package storage

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs"
)

// instrumentedBackend wraps a [backend.Backend], recording per-operation count, latency, and bytes
// to the [obs.Backend] meter (DESIGN §16). Backend ops are whole-object (read/write/list of a part,
// a manifest, the bucket index) — coarse, never per sample — so the one time.Now and one Add per
// op never touch a hot loop. With the no-op meter it is a thin pass-through.
type instrumentedBackend struct {
	inner backend.Backend
	m     *obs.Backend
}

// instrumentBackend wraps b so its operations are metered. It is applied only when a meter is
// configured, so the default path is the bare backend.
func instrumentBackend(b backend.Backend, m *obs.Backend) backend.Backend {
	return &instrumentedBackend{inner: b, m: m}
}

func (b *instrumentedBackend) IsEphemeral() bool { return b.inner.IsEphemeral() }

func (b *instrumentedBackend) Read(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	v, err := b.inner.Read(ctx, key)
	b.m.Record(ctx, "read", result(err), time.Since(start), int64(len(v)))
	zctx.From(ctx).Debug("backend read",
		zap.String("key", key), zap.Int("bytes", len(v)),
		zap.String("result", result(err)), zap.Duration("took", time.Since(start)))

	return v, err
}

// ReadView forwards the no-copy read capability (metered as a read), so wrapping a [backend.Viewer]
// in metering does not silently reintroduce the defensive copy. Implements [backend.Viewer].
func (b *instrumentedBackend) ReadView(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	v, err := backend.ReadView(ctx, b.inner, key)
	b.m.Record(ctx, "read", result(err), time.Since(start), int64(len(v)))
	zctx.From(ctx).Debug("backend read",
		zap.String("key", key), zap.Int("bytes", len(v)),
		zap.String("result", result(err)), zap.Duration("took", time.Since(start)))

	return v, err
}

func (b *instrumentedBackend) Write(ctx context.Context, key string, data []byte) error {
	start := time.Now()
	err := b.inner.Write(ctx, key, data)
	b.m.Record(ctx, "write", result(err), time.Since(start), int64(len(data)))
	zctx.From(ctx).Debug("backend write",
		zap.String("key", key), zap.Int("bytes", len(data)),
		zap.String("result", result(err)), zap.Duration("took", time.Since(start)))

	return err
}

func (b *instrumentedBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	start := time.Now()
	ok, err := b.inner.PutIfAbsent(ctx, key, data)
	b.m.Record(ctx, "cas", result(err), time.Since(start), int64(len(data)))
	zctx.From(ctx).Debug("backend cas",
		zap.String("key", key), zap.Int("bytes", len(data)), zap.Bool("stored", ok),
		zap.String("result", result(err)), zap.Duration("took", time.Since(start)))

	return ok, err
}

func (b *instrumentedBackend) List(ctx context.Context, prefix string) ([]string, error) {
	start := time.Now()
	keys, err := b.inner.List(ctx, prefix)
	b.m.Record(ctx, "list", result(err), time.Since(start), 0)
	zctx.From(ctx).Debug("backend list",
		zap.String("prefix", prefix), zap.Int("keys", len(keys)),
		zap.String("result", result(err)), zap.Duration("took", time.Since(start)))

	return keys, err
}

func (b *instrumentedBackend) Size(ctx context.Context, key string) (int64, error) {
	start := time.Now()
	n, err := backend.SizeOf(ctx, b.inner, key)
	b.m.Record(ctx, "size", result(err), time.Since(start), 0)
	zctx.From(ctx).Debug("backend size",
		zap.String("key", key), zap.Int64("bytes", n),
		zap.String("result", result(err)), zap.Duration("took", time.Since(start)))

	return n, err
}

func (b *instrumentedBackend) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := b.inner.Delete(ctx, key)
	b.m.Record(ctx, "delete", result(err), time.Since(start), 0)
	zctx.From(ctx).Debug("backend delete",
		zap.String("key", key), zap.String("result", result(err)), zap.Duration("took", time.Since(start)))

	return err
}

// result classifies a backend error for the metric label: a missing key is a normal outcome, not an
// error.
func result(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, backend.ErrNotExist):
		return "not_found"
	default:
		return "error"
	}
}
