package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeShapeAccounting(t *testing.T) {
	t.Parallel()

	const capBytes = 1 << 30

	// Two parts above capBytes/minMergeMultiplier are sealed; the geometric strays below it are
	// eligible but score too low for any run of them to qualify.
	parts := []*part{
		partOfSize(0, 900<<20),
		partOfSize(1, 800<<20),
		partOfSize(2, 1<<20),
		partOfSize(3, 4<<20),
		partOfSize(4, 16<<20),
	}

	e := &Engine{parts: parts}
	e.lastMergeCap.Store(capBytes)

	sh := e.MergeShape()
	assert.Equal(t, 5, sh.Parts)
	assert.Equal(t, 2, sh.Sealed, "the two parts at the cap are sealed")
	assert.Equal(t, 3, sh.Backlog, "the strays remain mergeable")
	assert.Zero(t, sh.Candidates, "but no run of them qualifies — the stuck state")
	assert.Equal(t, int64(capBytes), sh.CapBytes)
	assert.Less(t, sh.BestMultiplier, sh.MinMultiplier, "the best run scores below the guard")
	assert.Equal(t, mergeIdleRounds, sh.WaiveAfter)
	assert.Equal(t, sh.Backlog, e.MergeBacklog())

	// Once the waiver is reached the same set is selectable.
	e.idleMerges.Store(mergeIdleRounds)
	assert.Equal(t, 3, e.MergeShape().Candidates, "after idling, the strays are candidates")
}

// TestMergeShapeNoCap pins the pre-first-merge state: with no cap observed yet nothing is sealed,
// so the backlog is the whole part set rather than silently zero.
func TestMergeShapeNoCap(t *testing.T) {
	t.Parallel()

	e := &Engine{parts: []*part{partOfSize(0, 1<<30), partOfSize(1, 1<<20)}}

	sh := e.MergeShape()
	assert.Zero(t, sh.Sealed)
	assert.Equal(t, 2, sh.Backlog)
	assert.Zero(t, sh.CapBytes)
}

// TestMergeIdleForce pins what Force does: it selects the run the engine would have taken on its
// own once the write-amplification guard is waived — not a wider one, and never a sealed part.
func TestMergeIdleForce(t *testing.T) {
	t.Parallel()

	const capBytes = 1 << 30

	e := &Engine{}
	assert.Zero(t, e.mergeIdle(MergeOptions{}))
	assert.Equal(t, mergeIdleRounds, e.mergeIdle(MergeOptions{Force: true}))

	strays := []*part{partOfSize(0, 1<<20), partOfSize(1, 16<<20), partOfSize(2, 900<<20)}

	require.Nil(t, selectMergeParts(strays, MergeOptions{}, capBytes, e.mergeIdle(MergeOptions{})),
		"the plain cycle declines")

	got := selectMergeParts(strays, MergeOptions{Force: true}, capBytes, e.mergeIdle(MergeOptions{Force: true}))
	require.NotEmpty(t, got, "a forced merge takes the best run anyway")
	assert.Equal(t, strays[:2], got, "the sealed part stays out — forcing waives the heuristic, not the cap")
}
