//go:build !race

package engine_test

import (
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/heaptest"
	"github.com/oteldb/storage/signal"
)

//nolint:paralleltest // Samples process-wide live heap at identity writes.
func TestMergeIdentityFinishFitsBudget(t *testing.T) {
	heaptest.SkipUnsupported(t)
	mergeIdentityFinishHeap(t, 32768, 32<<20, 2048, engine.MergeOptions{Force: true,
		Recompress: &engine.RecompressSpec{Before: math.MaxInt64, Algorithm: compress.AlgorithmNone}})
}

//nolint:paralleltest // Samples process-wide live heap at identity writes.
func TestMergeDefaultCodecFinishFitsBudget(t *testing.T) {
	heaptest.SkipUnsupported(t)
	mergeIdentityFinishHeap(t, 4096, 32<<20, 64, engine.MergeOptions{Force: true})
}

func mergeIdentityFinishHeap(t *testing.T, identities, budget, payload int, options engine.MergeOptions) {
	t.Helper()
	files, err := file.New(t.TempDir())
	require.NoError(t, err)
	backend := &heaptest.FileSampler{File: files, KeyContains: "/identity", Writes: true}
	e := engine.New(engine.Config{Backend: backend, Prefix: "finish", MergeMemoryBytes: int64(budget), FlushMemoryBytes: 512 << 20})
	series := make([]signal.Series, identities)
	for i := range series {
		series[i] = mkSeries("__name__", "finish", "instance", "host-"+strconv.Itoa(i),
			"payload", strings.Repeat("x", payload)+strconv.Itoa(i))
	}
	for generation := range 2 {
		for i := range series {
			_, err := e.Append(series[i], int64(generation+1), float64(generation+1))
			require.NoError(t, err)
		}
		require.NoError(t, e.Flush(t.Context()))
	}
	require.Equal(t, 2, e.PartCount())
	peak := heaptest.Resident(t, backend, func() {
		require.NoError(t, e.MergeWith(t.Context(), options))
	})
	t.Logf("identities=%d budget=%d sampled_identity_finish_heap=%d output_parts=%d", identities, budget, peak, e.PartCount())
	require.LessOrEqual(t, peak, uint64(budget), "identity finishing must fit the merge's reserved working set")
	if options.Recompress == nil {
		reader, err := block.OpenPart(t.Context(), backend, e.PartPrefixes()[0])
		require.NoError(t, err)
		column, ok := reader.ColumnDescByName("value")
		require.True(t, ok)
		require.Equal(t, compress.AlgorithmZSTD, column.Compress)
		require.Equal(t, compress.Level(1), column.Level)
	}
	runtime.KeepAlive(e)
	runtime.KeepAlive(series)
}
