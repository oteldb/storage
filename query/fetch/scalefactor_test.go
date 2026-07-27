package fetch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

// weighted returns a one-sample batch carrying a lossy-sampling weight.
func weighted(id uint64, ts int64, value, weight float64) *fetch.Batch {
	b := batch(id, [2]int64{ts, int64(value)})
	b.Values[0] = value
	b.ScaleFactors = []float64{weight}

	return b
}

// TestMergeFederatedKeepsScaleFactors is the correctness of a federated sampled series: a series two
// children carry is combined, and each contributed sample must keep the weight it arrived with.
// Dropping them would make the merged series read as if every sample counted once, under-reporting
// any count/sum/rate over a sampled tenant.
func TestMergeFederatedKeepsScaleFactors(t *testing.T) {
	t.Parallel()

	got := drain(t, fetch.Merge(
		fakeFetcher{batches: []*fetch.Batch{weighted(7, 10, 1, 8)}},
		fakeFetcher{batches: []*fetch.Batch{weighted(7, 20, 2, 4)}},
	))

	require.Len(t, got, 1)
	assert.Equal(t, []int64{10, 20}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, got[0].Values)
	assert.Equal(t, []float64{8, 4}, got[0].ScaleFactors)
}

// TestMergeFederatedMixedSamplingKeepsWeights covers the mixed case: one child sampled, one not. The
// unsampled side's samples weigh 1 — the merge must materialize that rather than drop the sampled
// side's weights (or, worse, mis-align them against the sorted samples).
func TestMergeFederatedMixedSamplingKeepsWeights(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name        string
		left, right *fetch.Batch
		wantTs      []int64
		wantSF      []float64
	}{
		{
			name:  "unsampled first",
			left:  batch(7, [2]int64{10, 1}),
			right: weighted(7, 20, 2, 4),

			wantTs: []int64{10, 20},
			wantSF: []float64{1, 4},
		},
		{
			name:  "sampled first",
			left:  weighted(7, 10, 1, 8),
			right: batch(7, [2]int64{20, 2}),

			wantTs: []int64{10, 20},
			wantSF: []float64{8, 1},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := drain(t, fetch.Merge(
				fakeFetcher{batches: []*fetch.Batch{tt.left}},
				fakeFetcher{batches: []*fetch.Batch{tt.right}},
			))

			require.Len(t, got, 1)
			assert.Equal(t, tt.wantTs, got[0].Timestamps)
			assert.Equal(t, tt.wantSF, got[0].ScaleFactors)
		})
	}
}

// TestMergeDuplicateTimestampKeepsWinnersWeight pins weight and value to the *same* sample: the later
// child wins a duplicate timestamp, so its weight must win too — a value from one child paired with a
// weight from another would scale the wrong sample.
func TestMergeDuplicateTimestampKeepsWinnersWeight(t *testing.T) {
	t.Parallel()

	got := drain(t, fetch.Merge(
		fakeFetcher{batches: []*fetch.Batch{weighted(7, 10, 1, 8)}},
		fakeFetcher{batches: []*fetch.Batch{weighted(7, 10, 99, 4)}},
	))

	require.Len(t, got, 1)
	assert.Equal(t, []float64{99}, got[0].Values, "later child wins the duplicate timestamp")
	assert.Equal(t, []float64{4}, got[0].ScaleFactors, "and its weight comes with it")
}

// TestMergeUnweightedStaysNil keeps the common path allocation-free: no source carries weights, so
// the merged batch carries none either (every weight is 1 by [fetch.Batch.ScaleFactor]).
func TestMergeUnweightedStaysNil(t *testing.T) {
	t.Parallel()

	got := drain(t, fetch.Merge(
		fakeFetcher{batches: []*fetch.Batch{batch(7, [2]int64{10, 1})}},
		fakeFetcher{batches: []*fetch.Batch{batch(7, [2]int64{20, 2})}},
	))

	require.Len(t, got, 1)
	assert.Nil(t, got[0].ScaleFactors)
}

// TestMergeBatchesKeepsScaleFactors is the same guarantee for the batch-level form, which
// split-by-interval uses to stitch a series' sub-windows together.
func TestMergeBatchesKeepsScaleFactors(t *testing.T) {
	t.Parallel()

	got := fetch.MergeBatches(
		[]*fetch.Batch{weighted(7, 10, 1, 8)},
		[]*fetch.Batch{weighted(7, 20, 2, 4)},
	)

	require.Len(t, got, 1)
	assert.Equal(t, []int64{10, 20}, got[0].Timestamps)
	assert.Equal(t, []float64{8, 4}, got[0].ScaleFactors)

	// A single-source series is copied through with its weights intact.
	solo := fetch.MergeBatches([]*fetch.Batch{weighted(9, 10, 1, 8)})
	require.Len(t, solo, 1)
	assert.Equal(t, []float64{8}, solo[0].ScaleFactors)
}
