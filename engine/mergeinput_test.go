package engine

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

type mergeInputFile struct {
	*backendfile.File

	sources        map[string]bool
	indexReads     atomic.Int64
	watermarkReads atomic.Int64
}

func (b *mergeInputFile) Read(ctx context.Context, key string) ([]byte, error) {
	if b.sources[key] {
		b.indexReads.Add(1)
	}
	if strings.HasSuffix(key, "/smax") {
		b.watermarkReads.Add(1)
	}
	return b.File.Read(ctx, key)
}

func inputMemoryCorpus(t *testing.T, identities, parts int, budget int64, differentTimes bool) (*Engine, *mergeInputFile) {
	t.Helper()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	b := &mergeInputFile{File: files, sources: make(map[string]bool)}
	e := New(Config{Backend: b, Prefix: "input", MergeMemoryBytes: budget, FlushMemoryBytes: 128 << 20})
	series := make([]signal.Series, identities)
	ids := make([]signal.SeriesID, identities)
	ts, values := make([]int64, identities), make([]float64, identities)
	for i := range series {
		series[i] = stagedTestSeries(strconv.Itoa(i))
		ids[i] = series[i].Hash()
		values[i] = 1
	}
	for p := range parts {
		for i := range ts {
			ts[i] = int64(100 + p*identities)
			if differentTimes {
				ts[i] += int64(i)
			}
		}
		res, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return series[i] }, AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, identities, res.Accepted)
		require.NoError(t, e.Flush(t.Context()))
	}
	require.Len(t, e.parts, parts)
	for _, p := range e.parts {
		b.sources[sidxKey(p.prefix)] = true
	}
	return e, b
}

func TestMergeReselectsInputsBeforeReadingWholeIndexes(t *testing.T) {
	t.Parallel()
	e, b := inputMemoryCorpus(t, 8192, 16, 10<<20, false)
	require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true,
		Recompress: &RecompressSpec{Before: math.MaxInt64, Algorithm: compress.AlgorithmNone}}))
	require.Positive(t, b.indexReads.Load())
	require.LessOrEqual(t, b.indexReads.Load(), int64(8), "rejected input sets must not materialize their indexes")
	require.Less(t, e.PartCount(), 16)
}

func TestMergeInputLimitRerunsSelectionRatherThanTruncating(t *testing.T) {
	t.Parallel()
	e, b := inputMemoryCorpus(t, 8192, 16, 10<<20, false)
	for i, p := range e.parts {
		p.diskBytes = int64(i + 1)
	}
	opts := MergeOptions{Force: true, Recompress: &RecompressSpec{Before: math.MaxInt64, Algorithm: compress.AlgorithmNone}}
	readable := slices.Clone(e.parts)
	cohorts := mergeCohorts(e.parts, readable)
	initial, _, _ := cohortRun(cohorts, readable, opts, 0, mergeIdleRounds, 0)
	require.Len(t, initial, 16)
	selection, err := e.selectBoundedMerge(t.Context(), cohorts, readable, opts, 0, mergeIdleRounds, 0)
	require.NoError(t, err)
	require.Len(t, selection.parts, 8)
	require.Equal(t, e.parts[8:], selection.parts, "the bounded selector chooses the best eight-sized run afresh")
	require.NotEqual(t, initial[:8], selection.parts)
	require.Zero(t, b.indexReads.Load(), "planning may only stat, not consume source IDs")
}

func TestMergeInputDefaultCodecAndLowBudget(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		budget int64
		fits   bool
	}{
		{"default zstd fits", 32 << 20, true}, {"default zstd does not fit", 4 << 20, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e, b := inputMemoryCorpus(t, 8192, 2, tt.budget, false)
			profile, _, _ := mergeEncoding(e.parts, 0, MergeOptions{}, nil)
			require.Equal(t, compress.AlgorithmZSTD, profile.Algorithm)
			changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true, Background: true})
			if tt.fits {
				require.NoError(t, err)
				require.True(t, changed)
			} else {
				require.ErrorIs(t, err, errMergeInputMemory)
				require.False(t, changed)
				require.False(t, e.MergeDeferred())
				require.Zero(t, b.indexReads.Load())
				require.Zero(t, b.watermarkReads.Load())
			}
		})
	}
}

func TestMergeMinimumPairMayUseMoreThanHalfBudget(t *testing.T) {
	t.Parallel()
	e, b := inputMemoryCorpus(t, 65536, 2, 20<<20, false)
	bound, err := e.mergeInputBound(t.Context(), e.parts, false)
	require.NoError(t, err)
	opts := MergeOptions{Force: true, Recompress: &RecompressSpec{Before: math.MaxInt64, Algorithm: compress.AlgorithmNone}}
	require.Greater(t, bound.bytes(), e.mergeMemoryBudgetBytes()/2)
	require.Less(t, bound.bytes()+e.minimumMergeOutput(e.parts, 0, opts, nil), e.mergeMemoryBudgetBytes())
	require.NoError(t, e.MergeWith(t.Context(), opts))
	require.EqualValues(t, 2, b.indexReads.Load())
	require.NotEmpty(t, e.parts)
	require.Equal(t, 2*65536, partRows(e.parts))
	for _, p := range e.parts {
		require.False(t, b.sources[sidxKey(p.prefix)], "both original sources were replaced")
	}
}

func TestMergeOversizedWatermarkFailsBeforeSourceRead(t *testing.T) {
	t.Parallel()
	e, b := inputMemoryCorpus(t, 8192, 2, 32<<20, false)
	require.NoError(t, b.Write(t.Context(), watermark.Key(e.parts[0].prefix), make([]byte, 16<<20)))
	changed, err := e.MergeStep(t.Context(), MergeOptions{Force: true, Background: true})
	require.ErrorIs(t, err, errMergeInputMemory)
	require.False(t, changed)
	require.False(t, e.MergeDeferred(), "an intrinsic budget failure must not trigger a busy admission retry")
	require.Zero(t, b.indexReads.Load())
	require.Zero(t, b.watermarkReads.Load())
	require.Equal(t, 2, e.PartCount())
}

func TestMergeOversizedIndexFailsBeforeSourceRead(t *testing.T) {
	t.Parallel()
	e, b := inputMemoryCorpus(t, 8192, 2, 32<<20, false)
	require.NoError(t, b.Write(t.Context(), sidxKey(e.parts[0].prefix), make([]byte, 16<<20)))
	_, err := e.MergeStep(t.Context(), MergeOptions{Force: true, Background: true})
	require.ErrorIs(t, err, errMergeInputMemory)
	require.Zero(t, b.indexReads.Load())
	require.Zero(t, b.watermarkReads.Load())
}

func TestMergeCachedWatermarksChargeCapacity(t *testing.T) {
	t.Parallel()
	e, _ := inputMemoryCorpus(t, 8192, 2, 32<<20, false)
	p := e.parts[0]
	entries, err := p.seriesWatermarks(t.Context())
	require.NoError(t, err)
	before, err := mergeWatermarkBound(t.Context(), p)
	require.NoError(t, err)
	p.seriesMaxMu.Lock()
	p.seriesMax = make([]watermark.Entry, len(entries), 2*len(entries))
	copy(p.seriesMax, entries)
	p.seriesMaxMu.Unlock()
	after, err := mergeWatermarkBound(t.Context(), p)
	require.NoError(t, err)
	require.Equal(t, int64(len(entries))*memsize.Of[watermark.Entry](), after.resident-before.resident)
	require.Zero(t, after.transient, "a populated immutable cache needs no watermark read or fallback decode")
}

func TestMergeWatermarkFallbackIsPricedBeforeDecode(t *testing.T) {
	t.Parallel()
	for _, corrupt := range []bool{false, true} {
		t.Run(strconv.FormatBool(corrupt), func(t *testing.T) {
			t.Parallel()
			e, b := inputMemoryCorpus(t, 8192, 2, 32<<20, true)
			for _, p := range e.parts {
				if corrupt {
					require.NoError(t, b.Write(t.Context(), watermark.Key(p.prefix), []byte("bad")))
				} else {
					require.NoError(t, b.Delete(t.Context(), watermark.Key(p.prefix)))
				}
			}
			require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true,
				Recompress: &RecompressSpec{Before: math.MaxInt64, Algorithm: compress.AlgorithmNone}}))
			require.Equal(t, 1, e.PartCount())
		})
	}
}

func TestMergeUnboundedLegacyTimestampFailsBeforeSourceRead(t *testing.T) {
	t.Parallel()
	e, b := inputMemoryCorpus(t, 8192, 2, 32<<20, true)
	for i, p := range e.parts {
		manifest := p.reader.Manifest()
		manifest.RawBytes = 0
		for c := range manifest.Columns {
			manifest.Columns[c].HasSizing = false
		}
		require.NoError(t, b.Write(t.Context(), p.prefix+"/manifest", manifest.Encode(nil)))
		require.NoError(t, b.Delete(t.Context(), watermark.Key(p.prefix)))
		opened, err := openPart(t.Context(), b, p.prefix, e.cfg.Obs.Corruption, e.readCompressors)
		require.NoError(t, err)
		opened.minTime, opened.maxTime = p.minTime, p.maxTime
		e.parts[i] = opened
	}
	b.indexReads.Store(0)
	b.watermarkReads.Store(0)
	_, err := e.MergeStep(t.Context(), MergeOptions{Force: true, Background: true})
	require.ErrorIs(t, err, errMergeInputMemory)
	require.Zero(t, b.indexReads.Load())
	require.Zero(t, b.watermarkReads.Load())
}

func TestMergeSingletonRewriteIsPricedBeforeRollupDecode(t *testing.T) {
	t.Parallel()
	e, b := inputMemoryCorpus(t, 8192, 1, 4<<20, false)
	_, err := e.MergeStep(t.Context(), MergeOptions{Downsample: []DownsampleTier{{
		Before: 1000, Interval: 15, Agg: signal.AggSum,
	}}})
	require.ErrorIs(t, err, errMergeInputMemory)
	require.Zero(t, b.indexReads.Load())
	require.Zero(t, b.watermarkReads.Load())
}

type opaqueMergeSizer struct {
	backend.Backend
}

func (opaqueMergeSizer) Size(context.Context, string) (int64, error) {
	panic("opaque Size must not read before admission")
}

func TestMergeOpaqueSidecarPricingDoesNotRead(t *testing.T) {
	t.Parallel()
	n, err := mergeSidecarBytes(t.Context(), opaqueMergeSizer{}, "unknown", 1234)
	require.NoError(t, err)
	require.EqualValues(t, 1234, n)
}

func TestSeriesIndexMaximumRange(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		runs []int
		want int
	}{
		{"empty", nil, 0}, {"single", []int{7}, 7}, {"first", []int{9, 2, 3}, 9},
		{"middle", []int{1, 12, 3}, 12}, {"last", []int{1, 2, 15}, 15},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			column := sidxColumn(tt.runs)
			resident := buildPartIndex(column)
			require.Equal(t, tt.want, resident.maxRows)
			e := New(Config{Backend: backend.Memory()})
			require.NoError(t, e.cfg.Backend.Write(t.Context(), sidxKey("p"), encodeSeriesIndex(column)))
			paged, ok := openPagedIndex(t.Context(), e.cfg.Backend, "p", len(column), e.cfg.Obs.Corruption)
			require.True(t, ok)
			require.Equal(t, tt.want, paged.maxRows)
		})
	}
}

func TestSeriesIndexMaximumRangeSaturatesBudget(t *testing.T) {
	t.Parallel()
	entries, _, _, err := parseSeriesIndex(encodeSeriesIndex(sidxColumn([]int{1})), true)
	require.NoError(t, err)
	rows, valid := sidxRunBounds(entries, 1, maxInt)
	require.True(t, valid)
	require.Equal(t, maxInt, rows)
	if memsize.Of[int]() == 8 {
		require.Equal(t, int64(math.MaxInt64), buildbudget.Mul(int64(rows), 192))
	}
}

//nolint:paralleltest // observes the package-global output residency hook.
func TestMergeCapturesOneAllowanceThroughAdmissionAndOutput(t *testing.T) {
	e, _ := inputMemoryCorpus(t, 8192, 2, 128<<20, false)
	var concurrency atomic.Int64
	concurrency.Store(1)
	e.cfg.MergeConcurrency = func() int { return int(concurrency.Load()) }
	var admitted, outputLimit int64
	e.cfg.MergeAdmission = func(_ context.Context, bytes int64, _ bool) (func(), bool, error) {
		admitted = bytes
		concurrency.Store(2)
		return func() {}, true, nil
	}
	defer SetMergeResidentObserver(func(_, _, limit int64) { outputLimit = limit })()
	require.NoError(t, e.MergeWith(t.Context(), MergeOptions{Force: true}))
	require.EqualValues(t, 128<<20, admitted)
	require.Equal(t, admitted, outputLimit, "a new engine cannot shrink the grant after selection")
	require.EqualValues(t, 64<<20, e.mergeMemoryBudgetBytes(), "subsequent operations see the new share")
}

func TestMergeCodecFloorUsesActualSharedWorkspaceAtBoundary(t *testing.T) {
	t.Parallel()
	e, _ := inputMemoryCorpus(t, 32768, 2, 64<<20, false)
	profile, _, _ := mergeEncoding(e.parts, 0, MergeOptions{}, nil)
	actual := newMetricMergeCoders(e.parts, profile)
	oldObjects := compress.NewFrameCompressor(profile.Algorithm, profile.Level, 24*65536)
	require.Greater(t, actual.objects.EncodeWorkspace(), oldObjects.EncodeWorkspace())
	staging := int64(4<<20) + int64(DefaultMetricBlockRows)*64
	require.Equal(t, staging+actual.workspace(), e.minimumMergeOutput(e.parts, 0, MergeOptions{}, nil))
}

type mergeRangedNoNativeSize struct {
	backend.Backend

	reads, hints atomic.Int64
}

func (b *mergeRangedNoNativeSize) Read(context.Context, string) ([]byte, error) {
	b.reads.Add(1)
	return nil, context.Canceled
}

func (b *mergeRangedNoNativeSize) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	return backend.ReadAt(ctx, b.Backend, key, off, n)
}

func (b *mergeRangedNoNativeSize) RangesNatively(context.Context, string) bool {
	b.hints.Add(1)
	return true
}

func TestMergeInputNeverUsesReadingSizeFallbackThroughCache(t *testing.T) {
	t.Parallel()
	raw := &mergeRangedNoNativeSize{Backend: backend.Memory()}
	b := backend.Cached(raw, 1024)
	p := &part{be: b, index: partIndex{paged: &pagedIndex{be: b, key: "p/sidx", n: 2_000_000, total: 2_000_000, maxRows: 1}}}
	bound, err := mergeIndexBound(t.Context(), p)
	require.NoError(t, err)
	require.Greater(t, bound.bytes(), int64(32<<20))
	require.Zero(t, raw.reads.Load(), "a canonical 40MB index must be rejected without a size fallback read")
	require.Zero(t, raw.hints.Load(), "a range hint is not a metadata-only size guarantee")
}
