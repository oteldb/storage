package backendtest

import (
	"context"
	"sync/atomic"

	"github.com/oteldb/storage/backend"
)

// StreamingMemory is [backend.Memory] plus the incremental-write [backend.ObjectCreator], standing in
// for the file backend so the streamed write path runs without a disk. Its writer buffers and
// commits with one Write. It embeds the Backend interface, so memory's other capabilities
// ([backend.Viewer], [backend.Sizer], …) are hidden, except ranged reads: memory serves those
// natively, and a merge chooses between a windowed and a whole read by [backend.RangesNatively].
type StreamingMemory struct {
	backend.Backend

	creates atomic.Int64
}

var (
	_ backend.ObjectCreator = (*StreamingMemory)(nil)
	_ backend.ReaderAt      = (*StreamingMemory)(nil)
)

// NewStreamingMemory returns an empty StreamingMemory.
func NewStreamingMemory() *StreamingMemory { return &StreamingMemory{Backend: backend.Memory()} }

// Creates returns how many objects CreateObject has opened.
func (b *StreamingMemory) Creates() int64 { return b.creates.Load() }

// CreateObject implements [backend.ObjectCreator].
func (b *StreamingMemory) CreateObject(_ context.Context, key string) (backend.ObjectWriter, error) {
	b.creates.Add(1)

	return &memoryObjectWriter{b: b.Backend, key: key}, nil
}

// ReadAt implements [backend.ReaderAt].
func (b *StreamingMemory) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return backend.ReadAt(ctx, b.Backend, key, off, n)
}

// StreamsWrites implements [backend.ObjectCreator].
func (*StreamingMemory) StreamsWrites() bool { return true }

type memoryObjectWriter struct {
	b   backend.Backend
	key string
	buf []byte
}

func (w *memoryObjectWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)

	return len(p), nil
}

func (w *memoryObjectWriter) Commit(ctx context.Context) error { return w.b.Write(ctx, w.key, w.buf) }
func (w *memoryObjectWriter) Abort()                           { w.buf = nil }
