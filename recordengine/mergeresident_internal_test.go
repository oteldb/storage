//go:build !race

// The race detector's instrumentation changes what escapes and when the heap is collected, so the
// live-heap measurement below is taken only in an uninstrumented build.

package recordengine

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/heaptest"
)

// mergeResident merges `parts` flushed parts of `rows` records each over the file backend, sealing
// output parts at capBytes, and returns the merge's peak live heap above what the engine held
// before it, with the sources' decoded bytes.
func mergeResident(t *testing.T, parts, rows int, capBytes int64) heaptest.Run {
	t.Helper()

	ctx := context.Background()

	fb, err := file.New(t.TempDir())
	require.NoError(t, err)

	b := &heaptest.FileSampler{File: fb, KeyContains: "/c/", Writes: true}
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

	var out []*part

	resident := heaptest.Resident(t, b, func() {
		out, err = e.compactParts(ctx, src, minInt64, capBytes)
		require.NoError(t, err)
	})

	require.Greater(t, len(out), 1, "the merge must seal several parts")

	runtime.KeepAlive(e)

	return heaptest.Run{Resident: resident, Source: uint64(partsBytes(src))}
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
		// What the merge holds by design: a window per source column and one output part.
		modelBound = parts*readColumns*window + capBytes
	)

	small := mergeResident(t, parts, 64<<10, capBytes)
	large := mergeResident(t, parts, 512<<10, capBytes)

	require.Greater(t, small.Source, uint64(2*capBytes), "the small corpus must already seal several parts")
	require.Greater(t, float64(large.Source)/float64(small.Source), 6.0,
		"the corpus did not grow the sources enough to tell flat from proportional")

	heaptest.AssertFlat(t, small, large, heaptest.Flat{Floor: modelBound, MaxGrowth: 2.0, SourceShare: 2})
}
