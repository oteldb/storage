package backendtest

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/oteldb/storage/backend"
)

// ByteCounter counts the calls and bytes of every Read, ReadView and ReadAt, in total and per key.
// It serves [backend.Viewer] and [backend.ReaderAt] through [backend.ReadView] and [backend.ReadAt],
// so an inner backend without them falls back to a whole read. Every other capability is hidden,
// [backend.Sizer] included: a size probe falls back to a counted Read. [SizedByteCounter] forwards Size.
type ByteCounter struct {
	backend.Backend

	bytes atomic.Int64
	reads atomic.Int64

	mu     sync.Mutex
	perKey map[string]int64
}

// NewByteCounter wraps b.
func NewByteCounter(b backend.Backend) *ByteCounter {
	return &ByteCounter{Backend: b, perKey: map[string]int64{}}
}

// Bytes returns the bytes read since the last [ByteCounter.Reset].
func (b *ByteCounter) Bytes() int64 { return b.bytes.Load() }

// Reads returns the reads made since the last [ByteCounter.Reset].
func (b *ByteCounter) Reads() int64 { return b.reads.Load() }

// Reset zeroes every count.
func (b *ByteCounter) Reset() {
	b.bytes.Store(0)
	b.reads.Store(0)
	b.mu.Lock()
	clear(b.perKey)
	b.mu.Unlock()
}

// Report renders the per-key byte totals, one sorted line per key, so a failure says which object was
// read rather than only how much.
func (b *ByteCounter) Report() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	var sb strings.Builder
	for _, k := range slices.Sorted(maps.Keys(b.perKey)) {
		fmt.Fprintf(&sb, "\n  %-44s %8d", k, b.perKey[k])
	}

	return sb.String()
}

// Read implements [backend.Backend].
func (b *ByteCounter) Read(ctx context.Context, key string) ([]byte, error) {
	v, err := b.Backend.Read(ctx, key)
	b.note(key, len(v))

	return v, err
}

// ReadView implements [backend.Viewer].
func (b *ByteCounter) ReadView(ctx context.Context, key string) ([]byte, error) {
	v, err := backend.ReadView(ctx, b.Backend, key)
	b.note(key, len(v))

	return v, err
}

// ReadAt implements [backend.ReaderAt].
func (b *ByteCounter) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	v, err := backend.ReadAt(ctx, b.Backend, key, off, n)
	b.note(key, len(v))

	return v, err
}

func (b *ByteCounter) note(key string, n int) {
	b.bytes.Add(int64(n))
	b.reads.Add(1)
	b.mu.Lock()
	b.perKey[key] += int64(n)
	b.mu.Unlock()
}

// SizedByteCounter is a [ByteCounter] that also forwards [backend.Sizer] (through [backend.SizeOf]),
// so a ranged open learns an object's length without reading it whole.
type SizedByteCounter struct{ *ByteCounter }

var _ backend.Sizer = SizedByteCounter{}

// NewSizedByteCounter wraps b.
func NewSizedByteCounter(b backend.Backend) SizedByteCounter {
	return SizedByteCounter{NewByteCounter(b)}
}

// Size implements [backend.Sizer].
func (b SizedByteCounter) Size(ctx context.Context, key string) (int64, error) {
	return backend.SizeOf(ctx, b.Backend, key)
}
