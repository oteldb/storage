package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// TestAggPushdownCheck pins each rejection to the condition it names. The reason is a span
// attribute an operator reads instead of the source, so "not safe" alone is not enough: partial
// coverage and overlap call for opposite responses, and the part count says whether one straggler
// or the whole layout is at fault.
func TestAggPushdownCheck(t *testing.T) {
	t.Parallel()

	span := func(lo, hi int64) *part { return &part{minTime: lo, maxTime: hi} }

	tests := []struct {
		name   string
		plan   *enginePlan
		reason pushdownReason
		parts  int
	}{
		{
			name:   "no parts",
			plan:   &enginePlan{start: 0, end: 100},
			reason: pushdownOK,
		},
		{
			name:   "covered and disjoint",
			plan:   &enginePlan{start: 0, end: 100, liveParts: []*part{span(0, 10), span(20, 30)}},
			reason: pushdownOK,
		},
		{
			name:   "part starts before the range",
			plan:   &enginePlan{start: 10, end: 100, liveParts: []*part{span(0, 20), span(30, 40)}},
			reason: pushdownPartialCoverage,
			parts:  1,
		},
		{
			name:   "part ends after the range",
			plan:   &enginePlan{start: 0, end: 30, liveParts: []*part{span(0, 20), span(25, 40)}},
			reason: pushdownPartialCoverage,
			parts:  1,
		},
		{
			name:   "every part straddles the range",
			plan:   &enginePlan{start: 10, end: 30, liveParts: []*part{span(0, 20), span(25, 40)}},
			reason: pushdownPartialCoverage,
			parts:  2,
		},
		{
			name:   "parts overlap in time",
			plan:   &enginePlan{start: 0, end: 100, liveParts: []*part{span(0, 30), span(20, 50)}},
			reason: pushdownOverlappingParts,
			parts:  1,
		},
		{
			name: "three-way overlap counts each offender",
			plan: &enginePlan{
				start: 0, end: 100,
				liveParts: []*part{span(0, 30), span(20, 50), span(40, 60)},
			},
			reason: pushdownOverlappingParts,
			parts:  2,
		},
		{
			name: "head samples overlap a part",
			plan: &enginePlan{
				start: 0, end: 100,
				liveParts: []*part{span(0, 30)},
				headB: map[signal.SeriesID]*fetch.Batch{
					{Lo: 1}: {Timestamps: []int64{25, 40}},
				},
			},
			reason: pushdownOverlappingParts,
			parts:  1,
		},
		{
			name: "head samples after the parts stay safe",
			plan: &enginePlan{
				start: 0, end: 100,
				liveParts: []*part{span(0, 30)},
				headB: map[signal.SeriesID]*fetch.Batch{
					{Lo: 1}: {Timestamps: []int64{40, 50}},
				},
			},
			reason: pushdownOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := aggPushdownCheck(tt.plan)
			assert.Equal(t, tt.reason, got.reason)
			assert.Equal(t, tt.parts, got.parts)
			assert.Equal(t, tt.reason == pushdownOK, got.safe())
		})
	}
}

// TestWindowerPushdownGridUnusable checks the misaligned window reports its own reason rather than
// borrowing the part layout's: with no fine grid there is nothing to fold from a sidecar, whatever
// the parts look like.
func TestWindowerPushdownGridUnusable(t *testing.T) {
	t.Parallel()

	plan := &enginePlan{start: 0, end: 100, liveParts: []*part{{minTime: 0, maxTime: 10}}}

	w := newWindower(plan, WindowSpec{Step: 30, Window: 70})
	assert.Nil(t, w.grid)
	assert.Equal(t, pushdownGridUnusable, w.push.reason)
	assert.Zero(t, w.push.parts)
	assert.False(t, w.safe)

	aligned := newWindower(plan, WindowSpec{Step: 30, Window: 90})
	assert.NotNil(t, aligned.grid)
	assert.Equal(t, pushdownOK, aligned.push.reason)
	assert.True(t, aligned.safe)
}
