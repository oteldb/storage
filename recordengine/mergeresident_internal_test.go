//go:build !race

// The race detector's instrumentation changes what escapes and when the heap is collected, so the
// live-heap measurement below is taken only in an uninstrumented build.

package recordengine

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/heaptest"
)

// mergeResident merges `parts` flushed parts of `rows` records each over the file backend, sealing
// output parts at capBytes (0 ⇒ one output part), and returns the merge's peak live heap above what
// the engine held before it, with the sources' decoded bytes.
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

	if capBytes > 0 {
		require.Greater(t, len(out), 1, "the merge must seal several parts")
	} else {
		require.Len(t, out, 1)
	}

	runtime.KeepAlive(e)

	return heaptest.Run{Resident: resident, Source: uint64(partsBytes(src))}
}

// TestMergeResidentFlatInPartSize is the merge's claim, measured: it holds a read-ahead window of each
// source column and a granule of each, plus what its writer holds — an unsealed frame and the
// dictionary of each column, never the part. Growing the sources eightfold must leave the merge's
// live heap roughly where it was, whether it seals its output or writes it as one part; decoding the
// sources, or buffering the output, grows it with them.
//
//nolint:paralleltest // collects and samples the process-wide heap, so it must not run concurrently
func TestMergeResidentFlatInPartSize(t *testing.T) {
	if testing.Short() {
		t.Skip("ingests 2M rows")
	}

	const (
		parts  = 4
		window = defaultMergeReadWindow
		// ts, four ints and three bytes columns.
		readColumns = 8
		// What the merge holds by design: a window per source column, and per output column a frame
		// being filled, one being compressed, and the granule being staged.
		modelBound = parts*readColumns*window + 8<<20
	)

	for _, capBytes := range []int64{4 << 20, 0} {
		t.Run(fmt.Sprintf("cap=%d", capBytes), func(t *testing.T) {
			small := mergeResident(t, parts, 64<<10, capBytes)
			large := mergeResident(t, parts, 512<<10, capBytes)

			require.Greater(t, small.Source, uint64(8<<20), "the small corpus must already outgrow a part's worth of frames")
			require.Greater(t, float64(large.Source)/float64(small.Source), 6.0,
				"the corpus did not grow the sources enough to tell flat from proportional")

			heaptest.AssertFlat(t, small, large, heaptest.Flat{Floor: modelBound, MaxGrowth: 2.0, SourceShare: 4})
		})
	}
}
