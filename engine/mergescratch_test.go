package engine

import (
	"context"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

func scratchTestPart(rows int, weighted bool, tiers []DownsampleTier) *part {
	return &part{index: partIndex{paged: &pagedIndex{n: 1, total: rows, maxRows: rows}},
		hasSF: weighted, rollup: tiers}
}

func TestMergeSeriesScratchRaw(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		weighted bool
		rowBytes int64
	}{
		{"unweighted", false, 48},
		{"weighted", true, 72},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			parts := []*part{scratchTestPart(10, tt.weighted, nil), scratchTestPart(20, tt.weighted, nil)}
			headers := int64(len(parts)) * (2*(memsize.Of[tsRun]()+memsize.Of[[]DownsampleTier]()) + memsize.Of[rangeBuf]())
			require.Equal(t, 30*tt.rowBytes+headers, mergeSeriesScratchBound(parts, nil))
		})
	}
	require.Zero(t, mergeSeriesScratchBound(nil, nil))
	require.Zero(t, mergeSeriesScratchBound([]*part{scratchTestPart(0, false, nil)}, nil))
}

func TestMergeSeriesScratchMapAndTags(t *testing.T) {
	t.Parallel()
	const rows = 100
	last := []DownsampleTier{{Before: 1, Interval: 1, Agg: signal.AggLast}}
	avg := []DownsampleTier{{Before: 1, Interval: 1, Agg: signal.AggAvg}}
	plain := []*part{scratchTestPart(rows, false, nil)}
	raw := mergeSeriesScratchBound(plain, nil)
	mapped := mergeSeriesScratchBound(plain, last)
	mapRow := memsize.Of[bucketAcc]() + 4*memsize.MapEntry[rollupBucketKey, *bucketAcc]()
	mapRow += memsize.Of[rollupBucketKey]() + memsize.Of[rollupRep]()
	mapRow += memsize.Of[int64]() + memsize.Of[float64]() + 4*memsize.Of[int64]()
	metadata := memsize.MapBase + 4*memsize.Of[DownsampleTier]()
	require.Equal(t, rows*mapRow+metadata, mapped-raw)
	weightedOutput := mergeSeriesScratchBound(plain, avg)
	require.Equal(t, int64(rows)*memsize.Of[float64](), weightedOutput-mapped)

	// No new policy is needed for already-rolled representatives to share a bucket.
	recorded := []*part{scratchTestPart(rows, false, last)}
	require.Equal(t, mapped+rows*memsize.Of[rollupTag](), mergeSeriesScratchBound(recorded, nil))
	recordedAvg := []*part{scratchTestPart(rows, false, avg)}
	require.Equal(t, weightedOutput+rows*(memsize.Of[rollupTag]()+memsize.Of[float64]()),
		mergeSeriesScratchBound(recordedAvg, nil))

	weighted := []*part{scratchTestPart(rows, true, last)}
	require.Equal(t, mergeSeriesScratchBound(recorded, nil)+int64(rows)*4*memsize.Of[float64](),
		mergeSeriesScratchBound(weighted, nil))
}

func TestMergeMaximumSeriesRowsFallbackAndOverflow(t *testing.T) {
	t.Parallel()
	legacy := &part{index: partIndex{ids: make([]signal.SeriesID, 2), starts: []int32{0, 4, 10}}}
	require.EqualValues(t, 9, mergeMaximumSeriesRows(legacy))
	legacy.index.maxRows = 6
	require.EqualValues(t, 6, mergeMaximumSeriesRows(legacy))
	legacy.index.paged = &pagedIndex{n: 2, total: 10, maxRows: 7}
	require.EqualValues(t, 7, mergeMaximumSeriesRows(legacy))
	if memsize.Of[int]() != 8 {
		t.Skip("an int-sized series maximum cannot overflow int64 on this platform")
	}
	huge := []*part{scratchTestPart(maxInt, true, nil)}
	require.Equal(t, int64(math.MaxInt64), mergeSeriesScratchBound(huge, nil))
	require.Equal(t, int64(math.MaxInt64), mergeSeriesScratchBound(huge,
		[]DownsampleTier{{Before: 1, Interval: 1, Agg: signal.AggSum}}))
}

type mergeScratchFile struct {
	*backendfile.File

	columns atomic.Int64
}

func (b *mergeScratchFile) Read(ctx context.Context, key string) ([]byte, error) {
	if strings.Contains(key, "/c/") {
		b.columns.Add(1)
	}
	return b.File.Read(ctx, key)
}

func (b *mergeScratchFile) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if strings.Contains(key, "/c/") {
		b.columns.Add(1)
	}
	return b.File.ReadAt(ctx, key, off, n)
}

func TestMergeRollupScratchRejectedBeforeColumnRead(t *testing.T) {
	t.Parallel()
	const rows = 500000
	const limit = int64(210000000)
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	b := &mergeScratchFile{File: files}
	e := New(Config{Backend: b, Prefix: "scratch", MergeMemoryBytes: limit, FlushMemoryBytes: 128 << 20})
	series := stagedTestSeries("one-long-series")
	ids := make([]signal.SeriesID, 4096)
	ts, values := make([]int64, len(ids)), make([]float64, len(ids))
	for i := range ids {
		ids[i], values[i] = series.Hash(), 1
	}
	for generation := range 2 {
		for lo := 0; lo < rows; lo += len(ids) {
			n := min(len(ids), rows-lo)
			for i := range n {
				ts[i] = int64(generation*rows + lo + i)
			}
			result, err := e.AppendBatch(ids[:n], ts[:n], values[:n], nil,
				func(int) signal.Series { return series }, AppendLimits{})
			require.NoError(t, err)
			require.Equal(t, n, result.Accepted)
		}
		require.NoError(t, e.Flush(t.Context()))
	}
	require.Len(t, e.parts, 2)
	for _, p := range e.parts {
		require.Equal(t, rows, p.rows())
		require.EqualValues(t, rows, mergeMaximumSeriesRows(p))
	}
	opts := MergeOptions{Force: true, Downsample: []DownsampleTier{{Before: 1, Interval: 1, Agg: signal.AggSum}},
		Recompress: &RecompressSpec{Before: math.MaxInt64, Algorithm: compress.AlgorithmNone}}
	base, err := e.mergeInputBound(t.Context(), e.parts, false)
	require.NoError(t, err)
	floor := e.minimumMergeOutput(e.parts, 0, opts, opts.Downsample)
	legacy := buildbudget.Add(base.bytes(), buildbudget.Add(2*rows*192, floor))
	require.Less(t, legacy, limit, "the old per-series multiplier would admit this merge")
	require.Greater(t, buildbudget.Add(base.bytes(), buildbudget.Add(mergeSeriesScratchBound(e.parts, opts.Downsample), floor)), limit)
	b.columns.Store(0)
	changed, err := e.MergeStep(t.Context(), opts)
	require.ErrorIs(t, err, errMergeInputMemory)
	require.False(t, changed)
	require.Zero(t, b.columns.Load(), "rollup rejection precedes timestamp/value reads and map allocation")
	require.Equal(t, 2, e.PartCount())
	opts.Downsample = nil
	changed, err = e.MergeStep(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, changed, "the same long raw series fits the no-map streaming path")
	require.Equal(t, 1, e.PartCount())
	require.Equal(t, 2*rows, e.parts[0].rows())
}
