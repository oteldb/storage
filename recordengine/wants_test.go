package recordengine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// A want is what the read policy keys on, and it is bounded: the lost part's own time range. A query
// that does not reach into it is answerable here in full, so reporting the whole shard unanswerable
// would cost availability the loss does not justify.
func TestWantOverlapIsBoundedByTheLostPart(t *testing.T) {
	t.Parallel()

	be := backend.Memory()
	e := newRepairEngine(t, be, answerAlways(bucketindex.WantIncomplete, nil))

	assert.False(t, e.HasWants())
	assert.False(t, e.WantOverlaps(0, 1_000), "a healthy engine disclaims nothing")

	loseFirstOfTwo(t, e, be) // the part holding the sample at 100

	require.True(t, e.HasWants())
	assert.True(t, e.WantOverlaps(0, 150), "a read reaching the lost part is short")
	assert.True(t, e.WantOverlaps(100, 100), "and so is one landing exactly on it")
	assert.False(t, e.WantOverlaps(200, 400), "a read past it is served here")
}

// TestCommittedHoleLetsReadsThrough is what bounds the policy: a want no owner can satisfy becomes a
// hole, the hole discharges the want, and the engine stops disclaiming. Without this, acknowledging
// a loss would leave the shard permanently unreadable for that range.
func TestCommittedHoleLetsReadsThrough(t *testing.T) {
	t.Parallel()

	be := backend.Memory()
	e := newRepairEngine(t, be, answerAlways(bucketindex.WantAbsent, nil))

	loseFirstOfTwo(t, e, be)
	require.True(t, e.WantOverlaps(0, 150))

	mergeTimes(t, e, 3) // holeConfirmations passes of definitive absence

	require.Len(t, e.Holes(), 1)
	assert.False(t, e.HasWants(), "the hole discharged the obligation")
	assert.False(t, e.WantOverlaps(0, 150), "so the read policy lets the window through again")
}
