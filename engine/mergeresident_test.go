//go:build !race

// The race detector's instrumentation changes what escapes and when the heap is collected, so the
// live-heap measurement below is taken only in an uninstrumented build.

package engine_test

import (
	"context"
	"math"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"
)

// heapSampler is the file backend with every column read followed by a collection and a live-heap
// sample, taken while the bytes just read are still referenced. The file backend is the one that
// matters: it has no zero-copy view, so every byte a merge reads is a heap copy the merge owns.
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

// mergeResident merges `parts` flushed parts of `series` series × `samples` samples each over the
// file backend and returns the merge's peak live heap above what the engine held before it, along
// with the source column bytes on disk.
func mergeResident(t *testing.T, series, samples, parts int, window int64) (resident, source uint64) {
	t.Helper()

	ctx := context.Background()

	fb, err := file.New(t.TempDir())
	require.NoError(t, err)

	b := &heapSampler{File: fb}
	e := engine.New(engine.Config{
		Backend: b, Prefix: "m", MaxPartBytes: 0, MergeMemoryBytes: 1 << 30,
	})
	e.SetMergeReadWindow(window)

	ser := make([]signal.Series, series)
	ids := make([]signal.SeriesID, series)

	for i := range series {
		ser[i] = mkSeries("__name__", "resident", "instance", "host-"+strconv.Itoa(i))
		ids[i] = ser[i].Hash()
	}

	r := rand.New(rand.NewPCG(3, 5))

	// Jittered, or delta-of-delta collapses the timestamp column below one window and only the value
	// column would measure anything.
	flushCorpus(t, ctx, e, ser, ids, samples, parts,
		func(p, _, s int) int64 { return int64(p*samples+s)*15_000 + r.Int64N(1000) },
		func(_, _, _ int) float64 { return r.Float64() * 1e6 })

	require.Equal(t, parts, e.PartCount())

	for _, prefix := range e.PartPrefixes() {
		keys, err := b.List(ctx, prefix+"/c/")
		require.NoError(t, err)

		for _, key := range keys {
			size, err := b.Size(ctx, key)
			require.NoError(t, err)

			source += uint64(size)
		}
	}

	// Heap other tests in the process left behind can still be draining; a second cycle finishes
	// what the first one's finalizers released.
	runtime.GC()

	base := liveHeap()

	// The output's compression level steps with its row count, and a denser level's encoder is
	// megabytes of state: pinning it to none keeps the write side identical across corpus sizes.
	b.arm(true)
	require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Recompress: &engine.RecompressSpec{
		Before: math.MaxInt64, Algorithm: compress.AlgorithmNone,
	}}))

	peak := b.peak
	b.arm(false)

	require.Equal(t, 1, e.PartCount(), "the parts merge in one pass")
	require.Positive(t, peak, "the merge read no column")

	runtime.KeepAlive(e)

	return peak - min(peak, base), source
}

// TestMergeResidentFlatInPartSize is the read side's claim, measured: a merge holds a read-ahead
// window of each source column, not the column. Growing the sources eightfold must leave the merge's
// live heap roughly where it was; holding whole encoded columns grows it with them (measured: 1.13x
// against 7.8x).
//
// Series count is fixed and only samples per series grow, because a metric merge's unit is one whole
// series — the ranges it decodes grow with samples per series, and with that axis kept short they
// stay small beside the columns.
//
//nolint:paralleltest // collects and samples the process-wide heap, so it must not run concurrently
func TestMergeResidentFlatInPartSize(t *testing.T) {
	if testing.Short() {
		t.Skip("ingests 2M rows")
	}

	const (
		series = 1024
		parts  = 4
		window = 64 << 10
		// ts and value; the corpus has no weight column.
		readColumns = 2
		// What the read side holds by design. The measured baseline is process-wide, so heap another
		// test frees during the merge can push the small case toward zero; the ratio is taken against
		// at least this, never against noise.
		readBound = parts * readColumns * window
	)

	smallResident, smallSource := mergeResident(t, series, 64, parts, window)
	largeResident, largeSource := mergeResident(t, series, 512, parts, window)

	t.Logf("small: source %.1f MiB, resident %.1f MiB", float64(smallSource)/(1<<20), float64(smallResident)/(1<<20))
	t.Logf("large: source %.1f MiB, resident %.1f MiB", float64(largeSource)/(1<<20), float64(largeResident)/(1<<20))

	require.Greater(t, smallSource, uint64(parts*readColumns*window),
		"the small corpus must already span several windows per column, or both sizes read whole")
	require.Greater(t, float64(largeSource)/float64(smallSource), 6.0,
		"the corpus did not grow the sources enough to tell flat from proportional")

	growth := float64(largeResident) / float64(max(smallResident, readBound))

	assert.Less(t, growth, 2.0,
		"sources grew %.1fx and the merge's live heap grew %.1fx (%.1f → %.1f MiB): the merge holds "+
			"source columns, not a window of them",
		float64(largeSource)/float64(smallSource), growth,
		float64(smallResident)/(1<<20), float64(largeResident)/(1<<20))

	// Independent of the small run's baseline: holding the sources whole costs about their size.
	assert.Less(t, largeResident, largeSource/3,
		"the merge held %.1f MiB against %.1f MiB of sources: it holds source columns, not a window of them",
		float64(largeResident)/(1<<20), float64(largeSource)/(1<<20))
}
