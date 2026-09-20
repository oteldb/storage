package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMergePoolFollowsTheBudget covers the three shapes of [Options.MergeMemoryBytes] at the one
// place they become a scheduling decision. The opt-out case is the one worth a test: an unbounded
// pool is not a very large pool — every merge would reserve the whole of it and they would run one
// at a time, turning "no memory bound" into "no concurrency".
func TestMergePoolFollowsTheBudget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure int64
		wantPool  bool
	}{
		{"a configured budget is enforced", 1 << 30, true},
		{"a derived budget is enforced", 0, true},
		{"opting out installs no pool", -1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, err := InMemory(WithMergeMemory(tt.configure))
			require.NoError(t, err)

			t.Cleanup(func() { _ = s.Close(context.Background()) })

			if !tt.wantPool {
				assert.Nil(t, s.mergePool, "an unbounded budget must not serialize merges")

				return
			}

			require.NotNil(t, s.mergePool)
			assert.Positive(t, s.mergePool.Total())
		})
	}
}

// TestAdmitMergeWithoutAPoolNeverBlocks is the same property from the engines' side: they call the
// callback unconditionally, so it must be a no-op when nothing bounds them.
func TestAdmitMergeWithoutAPoolNeverBlocks(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithMergeMemory(-1))
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Held all at once on purpose: with no pool, nothing may block however much is outstanding.
	held := make([]func(), 0, 64)

	for range 64 {
		release, ok, err := s.admitMerge(ctx, 1<<60, true)
		require.NoError(t, err)
		require.True(t, ok)

		held = append(held, release)
	}

	for _, release := range held {
		release()
	}
}

// TestMergePoolCapsConcurrentHolders drives the real pool rather than a recorder: it is the only
// test that shows the division holding. Background callers decline once the budget is committed,
// which is what keeps the maintenance loop from parking, and the bytes come back on release.
func TestMergePoolCapsConcurrentHolders(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithMergeMemory(256 << 20))
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx := context.Background()

	// A quarter of the budget each: four fit, the fifth does not.
	const share = 64 << 20

	held := make([]func(), 0, 4)

	for i := range 4 {
		release, ok, err := s.admitMerge(ctx, share, false)
		require.NoError(t, err)
		require.True(t, ok, "holder %d must fit in the budget", i)

		held = append(held, release)
	}

	_, ok, err := s.admitMerge(ctx, share, false)
	require.NoError(t, err, "a committed budget is a deferral, not an error")
	assert.False(t, ok, "the budget admits exactly what it was divided into")

	held[0]()

	release, ok, err := s.admitMerge(ctx, share, false)
	require.NoError(t, err)
	require.True(t, ok, "a released share is admittable again")

	held = append(held[1:], release)
	for _, release := range held {
		release()
	}
}

// TestOperatorMergeWaitsForABusyBudget covers the blocking half against the real pool: an
// operator-requested merge queues rather than declining, and is released when a holder finishes.
func TestOperatorMergeWaitsForABusyBudget(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithMergeMemory(64 << 20))
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx := context.Background()

	held, ok, err := s.admitMerge(ctx, 64<<20, false)
	require.NoError(t, err)
	require.True(t, ok)

	admitted := make(chan struct{})

	go func() {
		release, ok, err := s.admitMerge(ctx, 64<<20, true)
		assert.NoError(t, err)
		assert.True(t, ok)
		close(admitted)
		release()
	}()

	require.Eventually(t, func() bool { return s.mergePool.Waiting() == 1 },
		time.Second, 50*time.Microsecond, "a forced merge queues instead of declining")

	held()

	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("the queued operator merge was never admitted")
	}
}
