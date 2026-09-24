package heaptest

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
)

// FileSampler is the file backend with a [Live] sample after every read of a matching key, taken
// while the bytes just read are still referenced. The file backend is the one that matters: it has
// no zero-copy view, so every byte a caller reads is a heap copy it owns.
//
// It embeds *[file.File] rather than wrapping [backend.Backend], so every optional capability the
// file backend has ([backend.ReaderAt], [backend.Sizer], [backend.DeferredSyncer],
// [backend.ObjectCreator], [backend.NodeLocal], ...) still forwards, and the code under test takes
// the path it takes over a bare file backend.
type FileSampler struct {
	*file.File

	// KeyContains selects the sampled keys; empty samples every key.
	KeyContains string
	// Writes samples WriteDeferred too, before the write, while the caller still holds the encoded
	// bytes.
	Writes bool

	mu    sync.Mutex
	armed bool
	peak  uint64
}

var (
	_ backend.ReaderAt       = (*FileSampler)(nil)
	_ backend.Sizer          = (*FileSampler)(nil)
	_ backend.DeferredSyncer = (*FileSampler)(nil)
	_ backend.ObjectCreator  = (*FileSampler)(nil)
	_ backend.NodeLocal      = (*FileSampler)(nil)
)

// Read implements [backend.Backend].
func (s *FileSampler) Read(ctx context.Context, key string) ([]byte, error) {
	data, err := s.File.Read(ctx, key)
	s.sample(key)
	runtime.KeepAlive(data)

	return data, err
}

// ReadAt implements [backend.ReaderAt].
func (s *FileSampler) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	data, err := s.File.ReadAt(ctx, key, off, n)
	s.sample(key)
	runtime.KeepAlive(data)

	return data, err
}

// WriteDeferred implements [backend.DeferredSyncer].
func (s *FileSampler) WriteDeferred(ctx context.Context, key string, data []byte) error {
	if s.Writes {
		s.sample(key)
	}

	return s.File.WriteDeferred(ctx, key, data)
}

func (s *FileSampler) sample(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.armed || !strings.Contains(key, s.KeyContains) {
		return
	}

	s.peak = max(s.peak, Live())
}

func (s *FileSampler) arm() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.armed, s.peak = true, 0
}

func (s *FileSampler) disarm() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	peak := s.peak
	s.armed, s.peak = false, 0

	return peak
}

// Resident runs run with s armed and returns its peak live heap above what the process held
// before it. It fails tb if run read no matching key.
func Resident(tb testing.TB, s *FileSampler, run func()) uint64 {
	tb.Helper()

	// Heap other tests in the process left behind can still be draining; a second cycle finishes
	// what the first one's finalizers released.
	runtime.GC()

	base := Live()

	s.arm()
	run()

	peak := s.disarm()
	require.Positive(tb, peak, "no sampled key was read")

	return peak - min(peak, base)
}
