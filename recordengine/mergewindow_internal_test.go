package recordengine

import (
	"context"
	"fmt"
	"math"
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

func mergeWindowPart(ts []int64) *decodedPart {
	ints := make([]int64, len(ts))
	entries := make([][]byte, len(ts))

	for i := range ts {
		ints[i] = int64(i)
		entries[i] = []byte{byte(i)}
	}

	dc := &chunk.DictColumn{Entries: entries}

	return &decodedPart{
		ts:       ts,
		ints:     [][]int64{ints},
		bytes:    []arrayBytes{{col: dc, g: block.OwnedGranule(dc, block.NewDictGen()), stable: true}},
		tsSorted: true,
	}
}

// windowRows is what a whole-decoded source serves of rows rng from start on, as rows of
// "ts/int/value".
func windowRows(t *testing.T, d *decodedPart, rng rowRange, start int64) []string {
	t.Helper()

	id := signal.SeriesID{Lo: 1}
	s := &wholeSource{schema: headTestSchema, ranges: []streamRange{{rowRange: rng, id: id}}, d: d}
	s.served.init(headTestSchema)
	s.gather.init(headTestSchema)

	r, ok, err := s.run(id, start)
	require.NoError(t, err)

	if !ok {
		return nil
	}

	a, ok := r.(*arrayRun)
	require.True(t, ok)

	out := make([]string, 0, len(a.ts))
	for i := range a.ts {
		b := &a.bytes[0]
		out = append(out, fmt.Sprintf("%d/%d/%x", a.ts[i], a.ints[0][i], b.col.At(b.base+i)))
	}

	return out
}

// FuzzMergeWindowSearchMatchesScan: on a ts-ascending stream range, serving the window in place by a
// search yields exactly the rows gathering and sorting the range does, in the same order — including
// runs of equal timestamps at the edge.
func FuzzMergeWindowSearchMatchesScan(f *testing.F) {
	f.Add([]byte{0, 0, 1, 1, 1, 3, 5, 5}, byte(1), byte(6), uint8(1))
	f.Add([]byte{7}, byte(0), byte(1), uint8(0))
	f.Add([]byte{}, byte(0), byte(0), uint8(0))

	f.Fuzz(func(t *testing.T, steps []byte, lo, hi byte, start uint8) {
		ts := make([]int64, len(steps))

		var cur int64
		for i, s := range steps {
			cur += int64(s % 3) // 0 repeats the previous timestamp
			ts[i] = cur
		}

		sorted := mergeWindowPart(ts)
		scanned := mergeWindowPart(ts)
		scanned.tsSorted = false

		a, b := min(int(lo), len(ts)), min(int(hi), len(ts))
		rng := rowRange{start: min(a, b), end: max(a, b)}

		for _, from := range []int64{int64(start), math.MinInt64} {
			require.Equal(t, windowRows(t, scanned, rng, from), windowRows(t, sorted, rng, from),
				"window from %d over %v", from, ts[rng.start:rng.end])
		}
	})
}

// TestMergeKeepsRowsOfUnsortedPart: a part that breaks the ts order within a stream — structurally
// valid, so nothing rejects it on open — must lose no in-window row in a retention merge, and must be
// reported. A windowed search over it would skip rows, and the merge would then retire the part.
func TestMergeKeepsRowsOfUnsortedPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	o, m := obstest.New(t)
	be := backend.Memory()
	e := New(Config{Schema: headTestSchema, Backend: be, Prefix: "t/recs", Obs: o})

	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("svc"))},
	)}}
	e.head.registerStream(series)

	unsorted := []int64{50, 10, 40, 20, 30}

	buf := newRecordCols(headTestSchema, 0, fullSel(headTestSchema))
	for range unsorted {
		buf.appendClone(headTestRec(0))
	}

	f := buildFlushColumns(headTestSchema, map[signal.SeriesID]*recordCols{series.Hash(): buf}, nil)
	copy(f.cols.ts, unsorted)

	prefix := e.newPartPrefix()
	require.NoError(t, writePart(ctx, be, headTestSchema, prefix, f, e.identitiesForColumn(f.stream),
		compress.AlgorithmNone, 0, e.blooms()))

	p, err := openPart(ctx, be, headTestSchema, prefix, o.Corruption)
	require.NoError(t, err)

	const retainFrom = 25

	out, err := e.compactParts(ctx, []*part{p}, retainFrom, 0)
	require.NoError(t, err)
	require.Len(t, out, 1)

	merged, err := out[0].readCols(ctx, fullSel(headTestSchema), nil, nil)
	require.NoError(t, err)

	want := slices.DeleteFunc(slices.Clone(unsorted), func(ts int64) bool { return ts < retainFrom })
	slices.Sort(want)

	assert.Equal(t, want, merged.ts, "every in-window row of the unsorted part survives the merge")
	assert.Equal(t, int64(1),
		m.Counter("storage.corruption.detected", "component", "stream_order", "disposition", "tolerated"))
}

// TestWrittenPartsAreTSSortedPerStream pins the invariant the merge's windowed search and every
// windowed fetch rely on, over each way a part is written: an out-of-order flush of interleaved
// streams, a flush split into several parts, and the merge of those parts.
func TestWrittenPartsAreTSSortedPerStream(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := New(Config{Schema: headTestSchema, Backend: backend.Memory(), Prefix: "t/recs", MaxPartBytes: 256})

	for round := range 3 {
		for s, svc := range []string{"a", "b", "c"} {
			series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
				signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
			)}}

			var b Batch

			b.Stream, b.Identity = series.Hash(), func() signal.Series { return series }
			b.Ints, b.Bytes = [][]int64{nil}, [][][]byte{nil}

			for _, ts := range []int64{90, 10, 50, 10, 70, 30} {
				b.Ts = append(b.Ts, ts+int64(round*100+s))
				b.Ints[0] = append(b.Ints[0], ts)
				b.Bytes[0] = append(b.Bytes[0], []byte("body padding to split parts"))
			}

			_, err := e.AppendBatch(&b, AppendLimits{})
			require.NoError(t, err)
		}

		require.NoError(t, e.Flush(ctx))
	}

	requireSorted := func(stage string) {
		t.Helper()

		for _, p := range e.parts {
			d, err := p.readForMerge(ctx)
			require.NoError(t, err)
			require.True(t, d.tsSorted, "%s: part %s has a stream out of ts order", stage, p.prefix)
		}
	}

	flushed := len(e.parts)
	require.Greater(t, flushed, 3, "the flushes must split")
	requireSorted("flush")

	for range flushed {
		require.NoError(t, e.Merge(ctx, 0))
	}

	require.Less(t, len(e.parts), flushed, "the merge must run")
	requireSorted("merge")
}
