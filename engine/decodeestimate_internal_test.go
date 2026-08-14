package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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
			require.Equal(t, tt.want, got)
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
	require.Equal(t, []int{6, 7}, plan.partBlocks[pt])
	require.Equal(t, []int{6, 7}, plan.blocksFor(context.Background(), pt, r, nil))
}
