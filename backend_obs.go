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
	i := &instrumentedBackend{inner: b, m: m}

	// Forwarded by a distinct type rather than a method, so metering claims the incremental-write
	// capability only when the wrapped backend actually has it — a method would make
	// [backend.StreamsWrites] report a streaming write over a backend that buffers.
	if backend.StreamsWrites(b) {
		return &instrumentedStreamBackend{instrumentedBackend: i}
	}

	return i
}

// instrumentedStreamBackend is [instrumentedBackend] over a backend that streams object writes.
type instrumentedStreamBackend struct {
	*instrumentedBackend
}

var _ backend.ObjectCreator = (*instrumentedStreamBackend)(nil)

// CreateObject forwards the incremental write, metering the object once it commits — the point at
// which its bytes become an object, and the only point at which their total is known.
func (b *instrumentedStreamBackend) CreateObject(ctx context.Context, key string) (backend.ObjectWriter, error) {
	w, err := backend.CreateObject(ctx, b.inner, key)
	if err != nil {
		return nil, err
	}

	return &instrumentedObjectWriter{ObjectWriter: w, b: b.instrumentedBackend, key: key}, nil
}

// instrumentedObjectWriter records a streamed object as one write of its total bytes.
type instrumentedObjectWriter struct {
	backend.ObjectWriter

	b       *instrumentedBackend
	key     string
	written int64
}

func (w *instrumentedObjectWriter) Write(p []byte) (int, error) {
	n, err := w.ObjectWriter.Write(p)
	w.written += int64(n)

	return n, err
}

func (w *instrumentedObjectWriter) Commit(ctx context.Context) error {
	start := time.Now()
	err := w.ObjectWriter.Commit(ctx)
	w.b.m.Record(ctx, "write", result(err), time.Since(start), w.written)
	zctx.From(ctx).Debug("backend write",
		zap.String("key", w.key), zap.Int64("bytes", w.written), zap.Bool("streamed", true),
		zap.String("result", result(err)), zap.Duration("took", time.Since(start)))

	return err
}

func (b *instrumentedBackend) IsEphemeral() bool { return b.inner.IsEphemeral() }

// IsNodeLocal forwards the [backend.NodeLocal] capability; without it a metered backend would look
// shared.
func (b *instrumentedBackend) IsNodeLocal() bool { return backend.IsNodeLocal(b.inner) }

// FreeSpace forwards the [backend.SpaceReporter] capability, unmetered — it is a statfs, not an
// object operation. Without it a metered backend would hide the disk from the merge cap and every
// merge would fall back to the ceiling.
func (b *instrumentedBackend) FreeSpace(ctx context.Context) (int64, error) {
	return backend.FreeSpace(ctx, b.inner)
}

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

// ReadAt forwards the ranged-read capability, metered as a read of the bytes it actually returned.
// Without it a metered backend would hide the capability and every column read would pull the whole
// object. Implements [backend.ReaderAt].
func (b *instrumentedBackend) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	start := time.Now()
	v, err := backend.ReadAt(ctx, b.inner, key, off, n)
	b.m.Record(ctx, "read", result(err), time.Since(start), int64(len(v)))
	zctx.From(ctx).Debug("backend read",
		zap.String("key", key), zap.Int("bytes", len(v)),
		zap.Int64("offset", off), zap.Int64("length", n),
		zap.String("result", result(err)), zap.Duration("took", time.Since(start)))

	return v, err
}

// ReadViewAt forwards the no-copy ranged read, so metering does not reintroduce a copy per frame on
// the query path. Implements [backend.ViewerAt].
func (b *instrumentedBackend) ReadViewAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	start := time.Now()
	v, err := backend.ReadViewAt(ctx, b.inner, key, off, n)
	b.m.Record(ctx, "read", result(err), time.Since(start), int64(len(v)))

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
