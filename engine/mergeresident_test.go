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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/signal"
)

// mergeResident merges `parts` flushed parts of `series` series × `samples` samples each over the
// file backend and returns the merge's peak live heap above what the engine held before it, along
// with the source column bytes on disk.
func mergeResident(t *testing.T, series, samples, parts int, window int64) heaptest.Run {
	t.Helper()

	ctx := context.Background()

	fb, err := file.New(t.TempDir())
	require.NoError(t, err)

	b := &heaptest.FileSampler{File: fb, KeyContains: "/c/"}
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

	var run heaptest.Run

	for _, prefix := range e.PartPrefixes() {
		keys, err := b.List(ctx, prefix+"/c/")
		require.NoError(t, err)

		for _, key := range keys {
			size, err := b.Size(ctx, key)
			require.NoError(t, err)

			run.Source += uint64(size)
		}
	}

	// The output's compression level steps with its row count, and a denser level's encoder is
	// megabytes of state: pinning it to none keeps the write side identical across corpus sizes.
	run.Resident = heaptest.Resident(t, b, func() {
		require.NoError(t, e.MergeWith(ctx, engine.MergeOptions{Recompress: &engine.RecompressSpec{
			Before: math.MaxInt64, Algorithm: compress.AlgorithmNone,
		}}))
	})

	require.Equal(t, 1, e.PartCount(), "the parts merge in one pass")

	runtime.KeepAlive(e)

	return run
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
		// What the read side holds by design.
		readBound = parts * readColumns * window
	)

	small := mergeResident(t, series, 64, parts, window)
	large := mergeResident(t, series, 512, parts, window)

	require.Greater(t, small.Source, uint64(parts*readColumns*window),
		"the small corpus must already span several windows per column, or both sizes read whole")
	require.Greater(t, float64(large.Source)/float64(small.Source), 6.0,
		"the corpus did not grow the sources enough to tell flat from proportional")

	heaptest.AssertFlat(t, small, large, heaptest.Flat{Floor: readBound, MaxGrowth: 2.0, SourceShare: 3})
}
