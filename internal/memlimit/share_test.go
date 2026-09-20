package memlimit_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/internal/memlimit"
)

// TestMergeBudgetSources covers the three ways the total allowance is decided. The detected-budget
// case is left to the caller's configuration here, since the process limit is not the test's to set.
func TestMergeBudgetSources(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(math.MaxInt64), memlimit.MergeBudget(-1), "a negative value opts out")
	assert.Equal(t, int64(1<<30), memlimit.MergeBudget(1<<30), "a configured value is taken as given")
	assert.Positive(t, memlimit.MergeBudget(0), "0 derives one rather than disabling merging")
}

// TestMergeShareOptOutStaysUnbounded: opting out must not be divided into a finite number by the
// concurrency, or the opt-out would silently become a bound.
func TestMergeShareOptOutStaysUnbounded(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(math.MaxInt64), memlimit.MergeShare(-1, 8, 2))
}

// TestMergeConcurrencyFollowsMemory is the rule that replaced dividing a memory budget by a core
// count: how many merges may run is a question about memory, with the core count only as a ceiling.
// A small-memory node therefore runs few merges with a usable allowance each, rather than one per
// core that each hold too little to keep up with ingest.
func TestMergeConcurrencyFollowsMemory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configured int64
		cpuLimit   int
		want       int
	}{
		{"a tiny budget runs one merge", 8 << 20, 16, 1},
		{"exactly one minimum", 64 << 20, 16, 1},
		{"memory binds below the core count", 256 << 20, 16, 4},
		{"the core count binds on a roomy node", 64 << 30, 16, 16},
		{"a single core is still a ceiling", 64 << 30, 1, 1},
		{"opting out leaves the cores in charge", -1, 8, 8},
		{"a nonsensical core count still yields one", 1 << 30, 0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, memlimit.MergeConcurrency(tt.configured, tt.cpuLimit))
		})
	}
}

// TestMergeShareNeverFallsBelowTheMinimumItDerived ties the two together: the concurrency is chosen
// so the share it produces is usable, so deriving one from the other must not undercut the floor.
func TestMergeShareNeverFallsBelowTheMinimumItDerived(t *testing.T) {
	t.Parallel()

	for _, budget := range []int64{16 << 20, 64 << 20, 256 << 20, 3 << 30, 64 << 30} {
		n := memlimit.MergeConcurrency(budget, 16)
		share := memlimit.MergeShare(budget, n, 1)

		assert.GreaterOrEqual(t, share*int64(n), budget/2,
			"the concurrency must not strand most of the budget")
		assert.Positive(t, share)

		if budget >= 64<<20 {
			assert.GreaterOrEqual(t, share, int64(64<<20),
				"every admitted merge gets at least the minimum the concurrency was derived from")
		}
	}
}
