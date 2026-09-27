package recordengine

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/obs/obstest"
	"github.com/oteldb/storage/signal"
)

func TestForwardReadable(t *testing.T) {
	t.Parallel()

	rng := func(id uint64, start, end int) streamRange {
		return streamRange{id: signal.SeriesID{Lo: id}, rowRange: rowRange{start: start, end: end}}
	}

	for _, tt := range []struct {
		name   string
		ranges []streamRange
		want   bool
	}{
		{"empty", nil, true},
		{"contiguous", []streamRange{rng(1, 0, 3), rng(2, 3, 5), rng(3, 5, 9)}, true},
		{"gap", []streamRange{rng(1, 0, 3), rng(2, 4, 5)}, true},
		{"backwards", []streamRange{rng(1, 3, 5), rng(2, 0, 3)}, false},
		{"repeated stream", []streamRange{rng(1, 0, 1), rng(1, 2, 3), rng(2, 1, 2)}, false},
		{"repeated stream in row order", []streamRange{rng(1, 0, 1), rng(1, 1, 2)}, false},
		{"descending ids in row order", []streamRange{rng(2, 0, 1), rng(1, 1, 2)}, false},
		{"inverted", []streamRange{rng(1, 2, 1)}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, forwardReadable(tt.ranges))
		})
	}
}

func TestIntCursorConstant(t *testing.T) {
	t.Parallel()

	c := intCursor{name: "k", constant: true, value: 7, hi: 4}

	got, err := c.from(1, 3)
	require.NoError(t, err)
	assert.Equal(t, []int64{7, 7}, got)
	assert.Equal(t, int64(7), c.at(3))

	_, err = c.from(4, 5)
	require.ErrorIs(t, err, block.ErrCorrupt, "a row past a column with no granules")
}

// TestMergeGathersDisorderedStream: a part both writers would never produce — one stream's rows out of
// timestamp order, its stream column still grouped — is read forward like any other until the merge
// meets the disorder; the merge then starts over with that part decoded whole, which gathers and sorts
// the stream. It writes what a whole decode of the same sources writes, rows equal in timestamp
// resolved by source then row, reports the part once, and remembers it.
//
//nolint:paralleltest // flips and observes package-level seams
func TestMergeGathersDisorderedStream(t *testing.T) {
	ctx := context.Background()
	o, metrics := obstest.New(t)
	be := backend.Memory()
	e := New(Config{Schema: headTestSchema, Backend: be, Prefix: "t/gather", Obs: o})
	reads := observeReads(t)

	f := streamColumns(t, e, map[string][]int64{"a": {1, 2, 3, 4, 5}})
	copy(f.cols.ts, []int64{9, 3, 7, 3, 1})

	bad := writeTestPart(t, e, be, f)
	require.True(t, forwardReadable(bad.ranges))

	good := writeTestPart(t, e, be, streamColumns(t, e, map[string][]int64{"a": {3, 8}, "b": {2}}))
	src := []*part{bad, good}

	streamed, err := e.compactParts(ctx, src, 2, 0)
	require.NoError(t, err)

	restore := SetMergeReadWhole(true)
	whole, err := e.compactParts(ctx, src, 2, 0)

	restore()
	require.NoError(t, err)

	assert.Equal(t, [][]bool{{true, true}, {false, true}, {false, false}}, *reads)
	assert.True(t, bad.tsDisorder.Load())
	assert.False(t, good.tsDisorder.Load())
	assert.Equal(t, partObjects(t, be, whole), partObjects(t, be, streamed))

	merged, err := streamed[0].readCols(ctx, fullSel(headTestSchema), nil, nil)
	require.NoError(t, err)

	got := map[byte][]string{}
	for i := range merged.len() {
		body := merged.bytes[0].at(i)
		got[body[0]] = append(got[body[0]], fmt.Sprintf("%d/%s", merged.ts[i], body))
	}

	assert.Equal(t, []string{"3/a-2", "3/a-4", "3/a-3", "7/a-3", "8/a-8", "9/a-1"}, got['a'],
		"equal timestamps resolve by source, then by row")
	assert.Equal(t, []string{"2/b-2"}, got['b'])
	assert.Equal(t, int64(2),
		metrics.Counter("storage.corruption.detected", "component", "stream_order", "disposition", "tolerated"),
		"reported once per merge that read the part, however it was read")
}

func TestSourceErr(t *testing.T) {
	t.Parallel()

	assert.Same(t, block.ErrCorrupt, sourceErr(3, block.ErrCorrupt))

	err := sourceErr(3, errRunDisorder)

	var disorder *sourceDisorderError
	require.ErrorAs(t, err, &disorder)
	assert.Equal(t, 3, disorder.src)
	require.ErrorIs(t, err, errRunDisorder)
	assert.Contains(t, err.Error(), "source 3")
}

// TestCursorOpenRejectsMismatchedColumns: a cursor opened on a column the part lacks, or of the other
// kind, fails rather than decoding it as something it is not.
func TestCursorOpenRejectsMismatchedColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := New(Config{Schema: headTestSchema, Backend: be, Prefix: "t/open"})
	p := writeTestPart(t, e, be, streamColumns(t, e, map[string][]int64{"a": {1, 2}}))

	var ic intCursor
	require.Error(t, ic.open(ctx, p.reader, "missing", 0))
	require.Error(t, ic.open(ctx, p.reader, "body", 0))

	var bc byteCursor
	require.Error(t, bc.open(ctx, p.reader, "missing", 0))
	require.Error(t, bc.open(ctx, p.reader, "sev", 0))
	require.NoError(t, bc.open(ctx, p.reader, "body", 0))
	require.ErrorIs(t, bc.ensure(2), block.ErrCorrupt, "a row past the column")
}

// writeTestPart writes f as a part under e and opens it.
func writeTestPart(t *testing.T, e *Engine, be backend.Backend, f *flushColumns) *part {
	t.Helper()

	ctx := context.Background()
	prefix := e.newPartPrefix()
	require.NoError(t, writePart(ctx, be, e.cfg.Schema, prefix, f, e.identitiesForColumn(f.stream),
		compress.AlgorithmNone, 0, e.blooms()))

	p, err := openPart(ctx, be, e.cfg.Schema, prefix, e.cfg.Obs.Corruption)
	require.NoError(t, err)

	return p
}

// reorderRows rewrites f's rows into the order given, every column alike.
func reorderRows(f *flushColumns, order []int) {
	stream := make([]chunk.U128, 0, len(order))
	for _, i := range order {
		stream = append(stream, f.stream[i])
	}

	f.stream = stream
	f.cols.ts = permute(f.cols.ts, order)

	for k := range f.cols.ints {
		f.cols.ints[k] = permute(f.cols.ints[k], order)
	}

	for k := range f.cols.bytes {
		var bc byteCol
		for _, i := range order {
			bc.appendCell(f.cols.bytes[k].at(i))
		}

		f.cols.bytes[k] = bc
	}
}

// mergedRows is every row of the parts as "ts/body" strings, sorted.
func mergedRows(t *testing.T, parts []*part) []string {
	t.Helper()

	var out []string

	for _, p := range parts {
		c, err := p.readCols(context.Background(), fullSel(p.schema), nil, nil)
		require.NoError(t, err)

		for i := range c.len() {
			out = append(out, fmt.Sprintf("%d/%s", c.ts[i], c.bytes[0].at(i)))
		}
	}

	slices.Sort(out)

	return out
}
