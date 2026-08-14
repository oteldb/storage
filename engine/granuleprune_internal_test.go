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

func TestPruneBlocks(t *testing.T) {
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
			want: []int{},
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

			require.Equal(t, tt.want, pruneBlocks(tt.blocks, tt.gran, tt.start, tt.end))
		})
	}
}

func TestRowsInBlocks(t *testing.T) {
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

			require.Equal(t, tt.want, rowsInBlocks(tt.ranges, blockRows, tt.blocks, totalRows))
		})
	}
}

// TestRowsInBlocksMatchesRangesWhenNothingPruned pins the pre-pruning behavior as the upper bound:
// with every block surviving, the count must equal the plain sum of the ranges' lengths, which is
// what the decode estimate charged before granule pruning existed.
func TestRowsInBlocksMatchesRangesWhenNothingPruned(t *testing.T) {
	t.Parallel()

	const blockRows, totalRows = 8, 64

	ranges := []rowRange{{start: 3, end: 19}, {start: 40, end: 61}}

	all := neededBlocks(ranges, blockRows, totalRows)

	var want int64
	for _, r := range ranges {
		want += int64(r.end - r.start)
	}

	require.Equal(t, want, rowsInBlocks(ranges, blockRows, all, totalRows))
}
