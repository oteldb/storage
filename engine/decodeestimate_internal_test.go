package engine

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
)

// estimatePart builds a resident-index part holding nSeries series of rowsEach samples, laid out
// contiguously the way a flushed part is.
func estimatePart(nSeries, rowsEach int) *part {
	idx := partIndex{
		ids:    make([]signal.SeriesID, nSeries),
		starts: make([]int32, nSeries+1),
	}

	for i := range nSeries {
		idx.ids[i] = signal.SeriesID{Hi: 0, Lo: uint64(i)}
		idx.starts[i] = int32(i * rowsEach)
	}

	idx.starts[nSeries] = int32(nSeries * rowsEach)

	return &part{index: idx}
}

// TestDecodeEstimate pins the reservation to what a query actually materializes. Charging a
// block-sliced part for every row it holds made the estimate a property of the store rather than of
// the query, so on a large store every query exceeded the whole budget and was admitted alone —
// the budget stopped being backpressure and became a global query lock.
func TestDecodeEstimate(t *testing.T) {
	t.Parallel()

	const (
		nSeries   = 1000
		rowsEach  = 100
		blockRows = 50
		allRows   = nSeries * rowsEach
	)

	ctx := context.Background()
	pt := estimatePart(nSeries, rowsEach)

	// One series: 100 rows in 2 blocks of 50, so 100 pinned + 100 copied rows.
	oneSeries := []signal.SeriesID{{Hi: 0, Lo: 7}}

	tests := []struct {
		name  string
		plan  *enginePlan
		need  colNeed
		want  int64
		about string
	}{
		{
			name: "a part read whole is charged for every row",
			plan: &enginePlan{ids: oneSeries, liveParts: []*part{pt}},
			need: colNeed{values: true},
			want: int64(allRows) * 8 * 2,
		},
		{
			name: "a block-sliced part is charged for the blocks it pins and the rows it copies",
			plan: &enginePlan{
				ids:          oneSeries,
				liveParts:    []*part{pt},
				blockReaders: map[*part]*seriesBlockReader{pt: {part: pt, blockRows: blockRows}},
			},
			need: colNeed{values: true},
			want: (100 + 100) * 8 * 2,
		},
		{
			name: "timestamps only halves it",
			plan: &enginePlan{
				ids:          oneSeries,
				liveParts:    []*part{pt},
				blockReaders: map[*part]*seriesBlockReader{pt: {part: pt, blockRows: blockRows}},
			},
			need: colNeed{},
			want: (100 + 100) * 8,
		},
		{
			name: "a part holding none of the matched series costs nothing",
			plan: &enginePlan{
				ids:          []signal.SeriesID{{Hi: 1, Lo: 1}},
				liveParts:    []*part{pt},
				blockReaders: map[*part]*seriesBlockReader{pt: {part: pt, blockRows: blockRows}},
			},
			need: colNeed{values: true},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.plan.decodeEstimate(ctx, tt.need)
			require.NoError(t, err)
			var lookupBytes int64
			for _, positions := range tt.plan.partPositions {
				lookupBytes += int64(cap(positions)) * 4
			}
			require.Equal(t, tt.want+lookupBytes, got)
		})
	}
}

// TestDecodeEstimateMemoizesRanges checks the estimate leaves behind what the prefetch would
// otherwise recompute: one index lookup per matched series per part, and the block union over them.
func TestDecodeEstimateMemoizesRanges(t *testing.T) {
	t.Parallel()

	pt := estimatePart(100, 100)
	r := &seriesBlockReader{part: pt, blockRows: 50}
	plan := &enginePlan{
		ids:          []signal.SeriesID{{Hi: 0, Lo: 3}},
		liveParts:    []*part{pt},
		blockReaders: map[*part]*seriesBlockReader{pt: r},
	}

	_, err := plan.decodeEstimate(context.Background(), colNeed{values: true})
	require.NoError(t, err)

	require.Equal(t, []rowRange{{start: 300, end: 400}}, plan.partRanges[pt])
	require.Equal(t, []uint32{0}, plan.partPositions[pt])
	require.Equal(t, []int{6, 7}, plan.partBlocks[pt])
	require.Equal(t, []int{6, 7}, plan.blocksFor(context.Background(), pt, r, nil))
}

func TestPlannedRangesAvoidRepeatedIndexReads(t *testing.T) {
	t.Parallel()
	col := make([]chunk.U128, indexPageEntries*4)
	for i := range col {
		col[i] = chunk.U128{Lo: uint64(i+1) * 2}
	}
	be := backend.Memory()
	require.NoError(t, be.Write(t.Context(), sidxKey("p"), encodeSeriesIndex(col)))
	counter := backendtest.NewByteCounter(be)
	idx, ok := openPagedIndex(t.Context(), counter, "p", len(col), obs.NewNop().Corruption)
	require.True(t, ok)
	pt := &part{index: partIndex{paged: idx}}
	plan := &enginePlan{ids: []signal.SeriesID{{Lo: 2}, {Lo: 3}, {Lo: 4}}, liveParts: []*part{pt},
		blockReaders: map[*part]*seriesBlockReader{pt: {part: pt, blockRows: 32}}}
	counter.Reset()
	_, err := plan.decodeEstimate(t.Context(), colNeed{values: true})
	require.NoError(t, err)
	require.EqualValues(t, len(plan.ids)*(sparseIndexPageEntries+1)*sidxEntryW, counter.Bytes())
	require.Equal(t, []uint32{0, 2}, plan.partPositions[pt])
	counter.Reset()
	for _, id := range plan.ids {
		want, found, err := buildPartIndex(col).lookup(t.Context(), id)
		require.NoError(t, err)
		got, actual, err := plan.lookupRange(t.Context(), pt, id)
		require.NoError(t, err)
		require.Equal(t, found, actual)
		require.Equal(t, want, got)
	}
	require.Zero(t, counter.Bytes(), "scan reuses both positive and negative planning results")
}

func TestDecodeEstimatePrunedPartAvoidsMarksReads(t *testing.T) {
	t.Parallel()
	counter := backendtest.NewByteCounter(backend.Memory())
	e := New(Config{Backend: counter, Prefix: "pruned-budget", MetricBlockRows: 32})
	for i := range 100 {
		_, err := e.Append(apiSeries(), int64(i), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.parts, 1)
	pt := e.parts[0]
	require.NotNil(t, pt.index.paged)
	require.Positive(t, pt.index.paged.first.Compare(signal.SeriesID{}))
	plan := enginePlan{ids: []signal.SeriesID{{}}, liveParts: []*part{pt}, start: 0, end: 100,
		blockReaders: map[*part]*seriesBlockReader{pt: {part: pt, blockRows: 32}}}
	counter.Reset()
	bytes, err := plan.decodeEstimate(t.Context(), colNeed{values: true})
	require.NoError(t, err)
	require.Zero(t, bytes)
	require.Zero(t, counter.Reads(), counter.Report())
	require.NotNil(t, plan.partRanges[pt])
}

func FuzzPlannedIndexRanges(f *testing.F) {
	for _, count := range []uint16{0, 1, 63, 1024, 4099} {
		f.Add(count, uint64(7), true)
		f.Add(count, uint64(13), false)
	}
	f.Fuzz(func(t *testing.T, size uint16, seed uint64, withReader bool) {
		count := int(size % 4100)
		col := make([]chunk.U128, 0, count*3)
		var ids []signal.SeriesID
		for i := range count {
			seed = seed*6364136223846793005 + 1
			id := chunk.U128{Hi: uint64(i / 64), Lo: uint64(i%64+1) * 2}
			for range 1 + int(seed%3) {
				col = append(col, id)
			}
			if seed&1 == 0 {
				ids = append(ids, signal.SeriesID(id))
			}
			if seed&2 == 0 {
				ids = append(ids, signal.SeriesID{Hi: id.Hi, Lo: id.Lo + 1})
			}
		}
		slices.SortFunc(ids, signal.SeriesID.Compare)
		oracle := buildPartIndex(col)
		pt := &part{index: oracle}
		plan := &enginePlan{ids: ids, liveParts: []*part{pt}}
		if withReader {
			plan.blockReaders = map[*part]*seriesBlockReader{pt: {part: pt, blockRows: 32}}
		}
		_, err := plan.decodeEstimate(t.Context(), colNeed{values: true})
		require.NoError(t, err)
		for _, id := range ids {
			want, found, err := oracle.lookup(t.Context(), id)
			require.NoError(t, err)
			got, actual, err := plan.lookupRange(t.Context(), pt, id)
			require.NoError(t, err)
			require.Equal(t, found, actual)
			require.Equal(t, want, got)
		}
		ranges, err := plan.rangesFor(t.Context(), pt)
		require.NoError(t, err)
		require.NotNil(t, ranges, "an empty selection must not request a whole-part decode")
	})
}
