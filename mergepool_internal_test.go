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
		release, err := s.admitMerge(ctx, 1<<60)
		require.NoError(t, err)

		held = append(held, release)
	}

	for _, release := range held {
		release()
	}
}
