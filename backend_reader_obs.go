package storage

import (
	"context"
	"io"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/memsize"
)

var (
	_ backend.ReadAppender = (*instrumentedBackend)(nil)
	_ backend.ObjectOpener = (*instrumentedBackend)(nil)
)

// ReadAtAppend meters only bytes appended after the caller's existing prefix.
func (b *instrumentedBackend) ReadAtAppend(ctx context.Context, dst []byte, key string, off, n int64) ([]byte, error) {
	start := time.Now()
	buf, err := backend.ReadAtAppend(ctx, b.inner, dst, key, off, n)
	b.m.Record(ctx, "read", result(err), time.Since(start), int64(len(buf)-len(dst)))
	return buf, err
}

// ReaderMemory includes the metering handle's state in the inner bound.
func (b *instrumentedBackend) ReaderMemory(key string) (int64, bool) {
	n, known := backend.ReaderMemory(b.inner, key)
	return n + memsize.Of[instrumentedObjectReader](), known
}

// OpenReader meters reads through an optional opened object.
func (b *instrumentedBackend) OpenReader(ctx context.Context, key string) (backend.ObjectReader, bool, error) {
	r, supported, err := backend.OpenReader(ctx, b.inner, key)
	if err != nil || !supported {
		return r, supported, err
	}
	if r == nil {
		return nil, true, errors.New("storage: backend returned a nil object reader")
	}
	return &instrumentedObjectReader{ObjectReader: r, ctx: ctx, b: b}, true, nil
}

type instrumentedObjectReader struct {
	backend.ObjectReader

	ctx context.Context //nolint:containedctx // The explicitly closed handle belongs to this operation.
	b   *instrumentedBackend
}

func (r *instrumentedObjectReader) ReadAt(dst []byte, off int64) (int, error) {
	start := time.Now()
	n, err := r.ObjectReader.ReadAt(dst, off)
	r.record(start, n, err)
	return n, err
}

func (r *instrumentedObjectReader) Read(dst []byte) (int, error) {
	start := time.Now()
	n, err := r.ObjectReader.Read(dst)
	r.record(start, n, err)
	return n, err
}

func (r *instrumentedObjectReader) record(start time.Time, n int, err error) {
	if errors.Is(err, io.EOF) {
		err = nil
	}
	r.b.m.Record(r.ctx, "read", result(err), time.Since(start), int64(n))
}
