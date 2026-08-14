package recordengine

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// granuleTestPart flushes one part holding rows records per stream, one record per unit of time, and
// returns it with the streams' ids in ingest order.
func granuleTestPart(t *testing.T, streams, rows int) (*part, []signal.SeriesID) {
	t.Helper()

	ctx := context.Background()
	schema := NewSchema(
		Column{Name: "severity", Kind: KindInt64, Codec: chunk.CodecT64},
		Column{Name: "body", Kind: KindBytes, Codec: chunk.CodecDict},
	)

	e := New(Config{Schema: schema, Backend: backend.Memory(), Prefix: "t/recs"})

	ids := make([]signal.SeriesID, streams)

	for s := range streams {
		series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue(fmt.Appendf(nil, "svc-%d", s))},
		)}}
		ids[s] = series.Hash()

		ts := make([]int64, rows)
		sev := make([]int64, rows)
		bodies := make([][]byte, rows)

		for i := range rows {
			ts[i] = int64(i)
			sev[i] = int64(i % 9)
			bodies[i] = fmt.Appendf(nil, "request %d handler=template done", i%16)
		}

		_, err := e.AppendBatch(&Batch{
			Stream: ids[s], Identity: func() signal.Series { return series },
			Ts: ts, Ints: [][]int64{sev}, Bytes: [][][]byte{bodies},
		}, AppendLimits{})
		require.NoError(t, err)
	}

	require.NoError(t, e.Flush(ctx))
	require.Len(t, e.parts, 1)

	return e.parts[0], ids
}

// TestWindowGranulesPrunesByTime is the point of the change: a narrow window must select a small
// fraction of a part's granules. Before framing, a fetch decoded every column whole however narrow
// its window was.
func TestWindowGranulesPrunesByTime(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const rows = 40_000

	p, ids := granuleTestPart(t, 1, rows)

	total := len(p.granuleTimes(ctx))
	require.Greater(t, total, 1, "the part must span several granules for pruning to mean anything")

	// A window covering everything prunes nothing, and says so with nil rather than naming every
	// granule.
	assert.Nil(t, p.windowGranules(ctx, ids, minInt64, maxInt64),
		"a whole-part window must not prune")

	// A window over a tenth of the records selects roughly a tenth of the granules.
	narrow := p.windowGranules(ctx, ids, 0, rows/10)
	require.NotEmpty(t, narrow)
	assert.Less(t, len(narrow), total/2,
		"a tenth of the time range must select well under half the granules (got %d of %d)",
		len(narrow), total)

	// A window past every record selects nothing.
	assert.Empty(t, p.windowGranules(ctx, ids, int64(rows)*10, int64(rows)*20),
		"a window past the part's records selects no granule")
}

// TestWindowGranulesScopesToRequestedStreams checks the selection is taken from the requested
// streams' row ranges, not the whole part. Rows are (stream, ts)-ordered, so each stream owns a
// contiguous run — a query for one service must not decode another's granules.
func TestWindowGranulesScopesToRequestedStreams(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const rows = 20_000

	p, ids := granuleTestPart(t, 3, rows)

	total := len(p.granuleTimes(ctx))
	require.Greater(t, total, 3)

	one := p.windowGranules(ctx, ids[:1], minInt64, maxInt64)
	require.NotEmpty(t, one, "one stream of three must not cover the part")
	assert.Less(t, len(one), total,
		"one stream must select fewer granules than the part holds (got %d of %d)", len(one), total)

	// Asking for an id the part does not hold selects nothing.
	assert.Empty(t, p.windowGranules(ctx, []signal.SeriesID{{Hi: 1, Lo: 2}}, minInt64, maxInt64))
}

// TestWindowGranulesWithoutMarksPrunesNothing checks the guard: a part whose marks are unusable must
// return nil so every granule stays a candidate. Pruning may only remove what it can prove empty.
func TestWindowGranulesWithoutMarksPrunesNothing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	p, ids := granuleTestPart(t, 1, 20_000)

	// Force the lazy load to a nil index, as an absent or mismatched sidecar would.
	p.marksOnce.Do(func() {})
	require.Nil(t, p.granuleTimes(ctx))

	assert.Nil(t, p.windowGranules(ctx, ids, 0, 1))
}

// TestPrunedDecodeMatchesFullDecode is the correctness backstop: reading a part through a pruned
// granule set must return, for the rows the window covers, exactly what an unpruned read returns.
func TestPrunedDecodeMatchesFullDecode(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	const rows = 30_000

	p, ids := granuleTestPart(t, 1, rows)

	sel := fullSel(p.schema)

	full, err := p.readCols(ctx, sel, nil, nil)
	require.NoError(t, err)

	const lo, hi = 100, 900

	blocks := p.windowGranules(ctx, ids, lo, hi)
	require.NotEmpty(t, blocks, "the window must prune to a real subset")

	pruned, err := p.readCols(ctx, sel, nil, blocks)
	require.NoError(t, err)

	rng := p.ranges[ids[0]]
	w := tsWindow(full.ts, rng, lo, hi)
	require.Less(t, w.start, w.end)

	for i := w.start; i < w.end; i++ {
		require.Equal(t, full.ts[i], pruned.ts[i], "ts row %d", i)
		require.Equal(t, full.ints[0][i], pruned.ints[0][i], "severity row %d", i)
		require.Equal(t, full.bytes[0].at(i), pruned.bytes[0].at(i), "body row %d", i)
	}
}
