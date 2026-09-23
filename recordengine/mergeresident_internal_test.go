//go:build !race

// The race detector's instrumentation changes what escapes and when the heap is collected, so the
// live-heap measurement below is taken only in an uninstrumented build.

package recordengine

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
)

// heapSampler is the file backend with every column read and every column write followed by a
// collection and a live-heap sample, taken while the bytes just read or written are still
// referenced. The file backend is the one that matters: it has no zero-copy view, so every byte a
// merge reads is a heap copy the merge owns. Sampling the writes too catches the merge's other peak,
// an output part encoded while the sources are still held.
type heapSampler struct {
	*file.File

	mu    sync.Mutex
	armed bool
	peak  uint64
}

func (s *heapSampler) Read(ctx context.Context, key string) ([]byte, error) {
	data, err := s.File.Read(ctx, key)
	s.sample(key)
	runtime.KeepAlive(data)

	return data, err
}

func (s *heapSampler) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	data, err := s.File.ReadAt(ctx, key, off, n)
	s.sample(key)
	runtime.KeepAlive(data)

	return data, err
}

func (s *heapSampler) WriteDeferred(ctx context.Context, key string, data []byte) error {
	s.sample(key)

	return s.File.WriteDeferred(ctx, key, data)
}

func (s *heapSampler) sample(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.armed || !strings.Contains(key, "/c/") {
		return
	}

	s.peak = max(s.peak, liveHeap())
}

func (s *heapSampler) arm(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.armed, s.peak = on, 0
}

func liveHeap() uint64 {
	var ms runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&ms)

	return ms.HeapAlloc
}

// mergeResident merges `parts` flushed parts of `rows` records each over the file backend, sealing
// output parts at capBytes, and returns the merge's peak live heap above what the engine held
// before it, with the sources' decoded bytes.
func mergeResident(t *testing.T, parts, rows int, capBytes int64) (resident, decoded uint64) {
	t.Helper()

	ctx := context.Background()

	fb, err := file.New(t.TempDir())
	require.NoError(t, err)

	b := &heapSampler{File: fb}
	e := benchTraceEngine(t, b, parts, rows)
	src := e.parts
	require.Len(t, src, parts)

	for _, p := range src {
		for _, name := range []string{"trace_id", "name", "attrs"} {
			desc, ok := p.reader.ColumnDescByName(name)
			require.True(t, ok)
			require.True(t, desc.Blocked, "column %q must be read by granule, or this measures a whole read", name)
		}
	}

	// Heap other tests in the process left behind can still be draining; a second cycle finishes
	// what the first one's finalizers released.
	runtime.GC()

	base := liveHeap()

	b.arm(true)

	out, err := e.compactParts(ctx, src, minInt64, capBytes)
	require.NoError(t, err)

	peak := b.peak
	b.arm(false)

	require.Greater(t, len(out), 1, "the merge must seal several parts")
	require.Positive(t, peak, "the merge read no column")

	runtime.KeepAlive(e)

	return peak - min(peak, base), uint64(partsBytes(src))
}

// TestMergeResidentFlatInPartSize is the read side's claim, measured: a merge that seals its output
// at a fixed size holds a read-ahead window of each source column and one output part, not the
// sources decoded. Growing the sources eightfold must leave the merge's live heap roughly where it
// was; decoding them whole grows it with them.
//
// The output is still buffered, so the claim needs a sealing merge: a merge that writes one part
// holds all of it decoded, and its peak is that part, however its sources are read.
//
//nolint:paralleltest // collects and samples the process-wide heap, so it must not run concurrently
func TestMergeResidentFlatInPartSize(t *testing.T) {
	if testing.Short() {
		t.Skip("ingests 2M rows")
	}

	const (
		parts    = 4
		capBytes = 4 << 20
		window   = defaultMergeReadWindow
		// ts, four ints and three bytes columns.
		readColumns = 8
		// What the merge holds by design: a window per source column and one output part. The
		// measured baseline is process-wide, so heap another test frees during the merge can push the
		// small case toward zero; the ratio is taken against at least this, never against noise.
		modelBound = parts*readColumns*window + capBytes
	)

	smallResident, smallDecoded := mergeResident(t, parts, 64<<10, capBytes)
	largeResident, largeDecoded := mergeResident(t, parts, 512<<10, capBytes)

	t.Logf("small: sources %.1f MiB decoded, resident %.1f MiB", float64(smallDecoded)/(1<<20), float64(smallResident)/(1<<20))
	t.Logf("large: sources %.1f MiB decoded, resident %.1f MiB", float64(largeDecoded)/(1<<20), float64(largeResident)/(1<<20))

	require.Greater(t, smallDecoded, uint64(2*capBytes), "the small corpus must already seal several parts")
	require.Greater(t, float64(largeDecoded)/float64(smallDecoded), 6.0,
		"the corpus did not grow the sources enough to tell flat from proportional")

	growth := float64(largeResident) / float64(max(smallResident, modelBound))

	assert.Less(t, growth, 2.0,
		"sources grew %.1fx and the merge's live heap grew %.1fx (%.1f → %.1f MiB): the merge holds "+
			"its sources decoded, not a window of them",
		float64(largeDecoded)/float64(smallDecoded), growth,
		float64(smallResident)/(1<<20), float64(largeResident)/(1<<20))

	// Independent of the small run's baseline: holding the sources decoded costs about their size.
	assert.Less(t, largeResident, largeDecoded/2,
		"the merge held %.1f MiB against %.1f MiB of decoded sources: it holds them, not a window of them",
		float64(largeResident)/(1<<20), float64(largeDecoded)/(1<<20))
}
