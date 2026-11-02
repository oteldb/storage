package engine

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type watermarkPreflightFile struct {
	*backendfile.File

	armed            atomic.Bool
	sources          map[string]bool
	timestamps       map[string]bool
	watermarkReads   atomic.Int64
	indexReads       atomic.Int64
	wholeTimestamps  atomic.Int64
	outsideAdmission atomic.Int64
	grants           atomic.Int64
	active           atomic.Int64
	poison           string
	poisonReads      atomic.Int64
}

func (b *watermarkPreflightFile) Read(ctx context.Context, key string) ([]byte, error) {
	b.note(key, true)
	return b.File.Read(ctx, key)
}

func (b *watermarkPreflightFile) ReadAt(ctx context.Context, key string, off, n int64) ([]byte, error) {
	b.note(key, false)
	return b.File.ReadAt(ctx, key, off, n)
}

func (b *watermarkPreflightFile) note(key string, whole bool) {
	if !b.armed.Load() {
		return
	}
	source := b.sources[key] || b.timestamps[key] || strings.HasSuffix(key, "/smax")
	if source && b.active.Load() == 0 {
		b.outsideAdmission.Add(1)
	}
	if b.sources[key] {
		b.indexReads.Add(1)
	}
	if whole && b.timestamps[key] {
		b.wholeTimestamps.Add(1)
	}
	if strings.HasSuffix(key, "/smax") {
		b.watermarkReads.Add(1)
	}
	if key == b.poison {
		b.poisonReads.Add(1)
	}
}

func watermarkPreflightCorpus(t *testing.T, parts int, budget int64, nonconstant bool) (*Engine, *watermarkPreflightFile) {
	t.Helper()
	return watermarkPreflightRowsCorpus(t, 128, parts, budget, nonconstant)
}

func watermarkPreflightRowsCorpus(t *testing.T, identities, parts int, budget int64, nonconstant bool) (*Engine, *watermarkPreflightFile) {
	t.Helper()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	b := &watermarkPreflightFile{File: files, sources: make(map[string]bool), timestamps: make(map[string]bool)}
	e := New(Config{Backend: b, Prefix: "wm-preflight", MergeMemoryBytes: budget, FlushMemoryBytes: 128 << 20})
	const samples = 512
	series := make([]signal.Series, identities)
	ids := make([]signal.SeriesID, identities*samples)
	ts, values := make([]int64, len(ids)), make([]float64, len(ids))
	for i := range series {
		series[i] = stagedTestSeries("preflight-" + strconv.Itoa(i))
		for point := range samples {
			row := i*samples + point
			ids[row], ts[row], values[row] = series[i].Hash(), 1, 1
			if nonconstant {
				ts[row] += int64(point)
			}
		}
	}
	for range parts {
		result, err := e.AppendBatch(ids, ts, values, nil, func(i int) signal.Series { return series[i/samples] }, AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, len(ids), result.Accepted)
		require.NoError(t, e.Flush(t.Context()))
	}
	require.Len(t, e.parts, parts)
	for _, p := range e.parts {
		require.Equal(t, identities*samples, p.rows(), "ordinary flush must retain the duplicated input rows")
		b.sources[sidxKey(p.prefix)] = true
		b.timestamps[p.prefix+"/c/1"] = true
	}
	e.cfg.MergeAdmission = func(_ context.Context, _ int64, _ bool) (func(), bool, error) {
		b.grants.Add(1)
		b.active.Add(1)
		return func() { b.active.Add(-1) }, true, nil
	}
	return e, b
}

func watermarkPreflightOptions() MergeOptions {
	return MergeOptions{Force: true, Recompress: &RecompressSpec{Before: math.MaxInt64, Algorithm: compress.AlgorithmNone}}
}

func checkWatermarkPreflightRows(t *testing.T, e *Engine, samples int) {
	t.Helper()
	iterator, err := e.Fetch(t.Context(), fetch.Request{Start: 0, End: 1000})
	require.NoError(t, err)
	rows, err := fetch.Drain(t.Context(), iterator)
	require.NoError(t, err)
	require.Len(t, rows, 128)
	for _, row := range rows {
		require.Len(t, row.Timestamps, samples)
		require.Len(t, row.Values, samples)
		for i := range samples {
			assert.Equal(t, int64(i+1), row.Timestamps[i])
			assert.InDelta(t, 1.0, row.Values[i], 0)
		}
	}
}

func checkWatermarkPreflightUncached(t *testing.T, parts []*part) {
	t.Helper()
	for _, p := range parts {
		p.seriesMaxMu.Lock()
		cached := p.seriesMax != nil
		p.seriesMaxMu.Unlock()
		assert.False(t, cached, "rejected source %q must not retain a preflight watermark table", p.prefix)
	}
}

func TestMergeValidWatermarksDoNotReserveTimestampFallback(t *testing.T) {
	t.Parallel()
	for _, budget := range []int64{5 << 20, 8 << 20} {
		t.Run(strconv.FormatInt(budget>>20, 10)+"MiB", func(t *testing.T) {
			t.Parallel()
			e, b := watermarkPreflightCorpus(t, 2, budget, false)
			b.armed.Store(true)
			changed, err := e.MergeStep(t.Context(), watermarkPreflightOptions())
			require.NoError(t, err, "valid watermarks must not force an unaffordable whole timestamp fallback")
			require.True(t, changed)
			require.EqualValues(t, 1, b.grants.Load(), "preflight and execution must share one captured grant")
			require.Zero(t, b.active.Load())
			require.Zero(t, b.outsideAdmission.Load())
			require.Zero(t, b.wholeTimestamps.Load())
			b.armed.Store(false)
			checkWatermarkPreflightRows(t, e, 1)
		})
	}
}

func TestMergeInvalidWatermarkFallbackIsGuarded(t *testing.T) {
	t.Parallel()
	for _, corrupt := range []bool{false, true} {
		t.Run(strconv.FormatBool(corrupt), func(t *testing.T) {
			t.Parallel()
			e, b := watermarkPreflightRowsCorpus(t, 512, 2, 5<<20, true)
			key := watermark.Key(e.parts[0].prefix)
			if corrupt {
				data, err := b.File.Read(t.Context(), key)
				require.NoError(t, err)
				data[len(data)-1] ^= 1 // Preserve canonical length; only decoding detects the corrupt CRC.
				require.NoError(t, b.Write(t.Context(), key, data))
			} else {
				require.NoError(t, b.Delete(t.Context(), key))
			}
			b.armed.Store(true)
			_, err := e.MergeStep(t.Context(), watermarkPreflightOptions())
			require.ErrorIs(t, err, errMergeInputMemory)
			require.Zero(t, b.wholeTimestamps.Load(), "an unaffordable fallback must be rejected before whole-column reads")
			require.Zero(t, b.indexReads.Load())
			require.Zero(t, b.outsideAdmission.Load())
			require.Zero(t, b.active.Load(), "all preflight reservations must be released")
			checkWatermarkPreflightUncached(t, e.parts)
		})
	}
}

func TestMergeReselectedWatermarksDoNotStickToRejectedSources(t *testing.T) {
	t.Parallel()
	e, b := watermarkPreflightCorpus(t, 16, 6<<20, true)
	original := slices.Clone(e.parts)
	for i, p := range original {
		p.diskBytes = int64(i + 1)
	}
	b.poison = watermark.Key(original[0].prefix)
	data, err := b.File.Read(t.Context(), b.poison)
	require.NoError(t, err)
	data[len(data)-1] ^= 1
	require.NoError(t, b.Write(t.Context(), b.poison, data))
	initial, err := e.mergeInputBoundWithWatermarks(t.Context(), original, false, nil)
	require.NoError(t, err)
	invalid, err := e.mergeInputBoundWithWatermarks(t.Context(), original, false, map[*part]bool{original[0]: true})
	require.NoError(t, err)
	scratch := mergeSeriesScratchBound(original, nil)
	floor := e.minimumMergeOutput(original, 0, watermarkPreflightOptions(), nil)
	require.Less(t, initial.bytes()+scratch+floor, e.cfg.MergeMemoryBytes)
	require.Greater(t, invalid.bytes()+scratch+floor, e.cfg.MergeMemoryBytes)
	t.Logf("unknown=%d invalid=%d scratch=%d floor=%d budget=%d",
		initial.bytes(), invalid.bytes(), scratch, floor, e.cfg.MergeMemoryBytes)
	b.armed.Store(true)
	changed, err := e.MergeStep(t.Context(), watermarkPreflightOptions())
	require.NoError(t, err)
	require.True(t, changed)
	require.Positive(t, b.poisonReads.Load(), "canonical-length corruption must be learned by admitted preflight")
	require.Zero(t, b.active.Load())
	require.Zero(t, b.outsideAdmission.Load())
	var rejected []*part
	for _, p := range original {
		if slices.Contains(e.parts, p) {
			rejected = append(rejected, p)
		}
	}
	require.NotEmpty(t, rejected, "repricing must reselect a smaller input set")
	checkWatermarkPreflightUncached(t, rejected)
	b.armed.Store(false)
	checkWatermarkPreflightRows(t, e, 512)
}
