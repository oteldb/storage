package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/block"
)

// gran builds a granule index at blockRows rows per granule from per-block [min, max] pairs.
func gran(blockRows int, bounds ...[2]int64) []block.Granule {
	out := make([]block.Granule, len(bounds))
	for i, b := range bounds {
		out[i] = block.Granule{FirstRow: i * blockRows, MinKey: b[0], MaxKey: b[1]}
	}

	return out
}

func TestBlockInWindow(t *testing.T) {
	t.Parallel()

	g := gran(10, [2]int64{100, 199}, [2]int64{200, 299}, [2]int64{300, 399})

	tests := []struct {
		name       string
		gran       []block.Granule
		b          int
		start, end int64
		want       bool
	}{
		{name: "window inside the block", gran: g, b: 1, start: 250, end: 260, want: true},
		{name: "window before every block", gran: g, b: 1, start: 0, end: 99, want: false},
		{name: "window after every block", gran: g, b: 1, start: 400, end: 500, want: false},
		{name: "window touching the low bound", gran: g, b: 1, start: 0, end: 200, want: true},
		{name: "window touching the high bound", gran: g, b: 1, start: 299, end: 500, want: true},
		{name: "window ending one before the block", gran: g, b: 1, start: 0, end: 199, want: false},
		{name: "window starting one after the block", gran: g, b: 1, start: 300, end: 500, want: false},
		// Pruning may only ever remove blocks it can prove empty, so anything it cannot see is kept.
		{name: "no index keeps the block", gran: nil, b: 1, start: 0, end: 1, want: true},
		{name: "index too short keeps the block", gran: g, b: 9, start: 0, end: 1, want: true},
		{name: "negative index keeps the block", gran: g, b: -1, start: 0, end: 1, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, blockInWindow(tt.gran, tt.b, tt.start, tt.end))
		})
	}
}

func TestWindowBlocksPrunes(t *testing.T) {
	t.Parallel()

	g := gran(10, [2]int64{100, 199}, [2]int64{200, 299}, [2]int64{300, 399}, [2]int64{400, 499})

	tests := []struct {
		name       string
		blocks     []int
		gran       []block.Granule
		start, end int64
		want       []int
	}{
		{
			name:   "keeps only the overlapping blocks",
			blocks: []int{0, 1, 2, 3}, gran: g, start: 250, end: 350,
			want: []int{1, 2},
		},
		{
			name:   "a window spanning everything prunes nothing",
			blocks: []int{0, 1, 2, 3}, gran: g, start: 0, end: 1000,
			want: []int{0, 1, 2, 3},
		},
		{
			name:   "a window matching no block prunes everything",
			blocks: []int{0, 1, 2, 3}, gran: g, start: 600, end: 700,
			want: nil,
		},
		{
			name:   "without an index the input is returned untouched",
			blocks: []int{0, 1, 2, 3}, gran: nil, start: 600, end: 700,
			want: []int{0, 1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// One range spanning every block, so the only thing that can remove a block is the
			// window test.
			ranges := []rowRange{{start: 0, end: len(tt.blocks) * 10}}

			got, _ := windowBlocks(ranges, 10, len(tt.blocks)*10, tt.gran, tt.start, tt.end)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestWindowBlocksCountsRows(t *testing.T) {
	t.Parallel()

	const blockRows, totalRows = 10, 100

	tests := []struct {
		name   string
		ranges []rowRange
		blocks []int
		want   int64
	}{
		{
			name:   "a range wholly inside one surviving block counts every row",
			ranges: []rowRange{{start: 12, end: 18}},
			blocks: []int{1},
			want:   6,
		},
		{
			name:   "a range spanning blocks counts only the surviving ones",
			ranges: []rowRange{{start: 5, end: 35}},
			blocks: []int{1, 2},
			want:   20, // rows 10..29; blocks 0 and 3 pruned
		},
		{
			name:   "no surviving block counts nothing",
			ranges: []rowRange{{start: 5, end: 35}},
			blocks: nil,
			want:   0,
		},
		{
			name:   "several ranges accumulate",
			ranges: []rowRange{{start: 0, end: 10}, {start: 40, end: 45}},
			blocks: []int{0, 4},
			want:   15,
		},
		{
			name:   "an empty range contributes nothing",
			ranges: []rowRange{{start: 20, end: 20}},
			blocks: []int{2},
			want:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Select exactly tt.blocks by giving each block a granule whose bounds sit inside the
			// window only for the wanted ones.
			gran := make([]block.Granule, totalRows/blockRows)
			for i := range gran {
				gran[i] = block.Granule{FirstRow: i * blockRows, MinKey: 1000, MaxKey: 1000}
			}

			for _, b := range tt.blocks {
				gran[b] = block.Granule{FirstRow: b * blockRows, MinKey: 1, MaxKey: 1}
			}

			_, got := windowBlocks(tt.ranges, blockRows, totalRows, gran, 0, 10)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestRowsInBlocksMatchesRangesWhenNothingPruned pins the pre-pruning behavior as the upper bound:
// with every block surviving, the count must equal the plain sum of the ranges' lengths, which is
// what the decode estimate charged before granule pruning existed.
func TestWindowBlocksCountsEveryRowWhenNothingPruned(t *testing.T) {
	t.Parallel()

	const blockRows, totalRows = 8, 64

	ranges := []rowRange{{start: 3, end: 19}, {start: 40, end: 61}}

	var want int64
	for _, r := range ranges {
		want += int64(r.end - r.start)
	}

	_, got := windowBlocks(ranges, blockRows, totalRows, nil, minInt64, maxInt64)
	require.Equal(t, want, got)
}

// TestWindowBlocksHandlesUnorderedRanges checks the sorted-input fast path degrades correctly: the
// watermark dedup is only valid for ascending ranges, so out-of-order input must fall back to a
// sort rather than silently dropping blocks.
func TestWindowBlocksHandlesUnorderedRanges(t *testing.T) {
	t.Parallel()

	const blockRows, totalRows = 10, 100

	ordered := []rowRange{{start: 5, end: 15}, {start: 60, end: 65}}
	reversed := []rowRange{{start: 60, end: 65}, {start: 5, end: 15}}

	wantBlocks, wantRows := windowBlocks(ordered, blockRows, totalRows, nil, minInt64, maxInt64)

	gotBlocks, gotRows := windowBlocks(reversed, blockRows, totalRows, nil, minInt64, maxInt64)

	require.Equal(t, []int{0, 1, 6}, wantBlocks)
	require.Equal(t, wantBlocks, gotBlocks, "block order must not depend on range order")
	require.Equal(t, wantRows, gotRows)
}

// TestWindowBlocksSharedBoundaryBlock checks the two halves disagree on purpose: a block two ranges
// straddle is decoded once (named once) but holds rows for both, so the row count must not dedup.
func TestWindowBlocksSharedBoundaryBlock(t *testing.T) {
	t.Parallel()

	const blockRows, totalRows = 10, 100

	// Both ranges live in block 1; together they hold 6 rows.
	ranges := []rowRange{{start: 11, end: 14}, {start: 15, end: 18}}

	blocks, rows := windowBlocks(ranges, blockRows, totalRows, nil, minInt64, maxInt64)

	require.Equal(t, []int{1}, blocks, "the shared block is decoded once")
	require.Equal(t, int64(6), rows, "but holds both ranges' rows")
}

// TestWindowBlocksDoesNotScaleWithPartSize is the regression guard for the cost this function
// replaced. The previous implementation sized an intermediate to the part's *total* block count, so
// a one-block query against a billion-row part allocated and scanned megabytes. Work must depend on
// the blocks the query touches, not on how large the part is.
//
//nolint:paralleltest // AllocsPerRun requires exclusive use of the process.
func TestWindowBlocksDoesNotScaleWithPartSize(t *testing.T) {
	const blockRows = 1024

	// The same single-block query against a small part and against a part a thousand times larger.
	ranges := []rowRange{{start: 0, end: 10}}

	small := testing.AllocsPerRun(100, func() {
		windowBlocks(ranges, blockRows, 1<<20, nil, minInt64, maxInt64)
	})

	huge := testing.AllocsPerRun(100, func() {
		windowBlocks(ranges, blockRows, 1<<30, nil, minInt64, maxInt64)
	})

	require.InDelta(t, small, huge, 0, "allocation must not grow with the part's block count")
	require.LessOrEqual(t, huge, 2.0, "a one-block query should allocate the result slice, not an index over the part")
}
