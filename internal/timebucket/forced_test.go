package timebucket_test

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/timebucket"
)

// forcedBefore forces every span starting before cutoff, the shape retention gives.
func forcedBefore(cutoff int64) func(span) bool { return func(s span) bool { return s.lo < cutoff } }

func sealedFrom(n int64) func(span) bool { return func(s span) bool { return s.size >= n } }

func TestForced(t *testing.T) {
	t.Parallel()

	a := span{lo: 10, hi: 20, size: 4}
	b := span{lo: 5, hi: 30, size: 4}
	c := span{lo: 1, hi: 40, size: 4}
	small := span{lo: 50, hi: 60, size: 1}
	mid := span{lo: 50, hi: 70, size: 3}
	big := span{lo: 50, hi: 80, size: 100}
	later := span{lo: 2 * hour, hi: 2*hour + 1, size: 1}
	straddler := span{lo: day - 1, hi: day + 1, size: 1}
	oldStraddler := span{lo: -hour, hi: hour, size: 1}

	cutoff := forcedBefore(45)
	never := sealedFrom(1 << 40)

	for _, tt := range []struct {
		name     string
		src      []span
		forced   func(span) bool
		sealed   func(span) bool
		capBytes int64
		maxParts int
		want     []span
	}{
		{"nothing forced", []span{small, mid}, forcedBefore(0), never, 0, 0, nil},
		{"uncapped takes the whole bucket", []span{a, small, b, mid, c}, cutoff, never, 0, 0, []span{a, small, b, mid, c}},
		{"cap keeps the oldest forced", []span{a, b, c}, cutoff, never, 8, 0, []span{b, c}},
		{"part count keeps the oldest forced", []span{a, b, c}, cutoff, never, 0, 1, []span{c}},
		{"oversized forced part goes alone", []span{a, b, c}, cutoff, never, 1, 0, []span{c}},
		{"neighbors fill what remains, smallest first", []span{a, mid, small, big}, cutoff, never, 8, 0, []span{a, mid, small}},
		{"neighbors stop at the first that does not fit", []span{a, mid, small}, cutoff, never, 6, 0, []span{a, small}},
		{"deferred forced work leaves room only for smaller neighbors", []span{a, b, small}, cutoff, never, 5, 0, []span{b, small}},
		{"sealed neighbors stay out", []span{a, mid, small}, cutoff, sealedFrom(3), 0, 0, []span{a, small}},
		{"a newer bucket's forced part waits", []span{later, a}, forcedBefore(3 * hour), never, 0, 0, []span{a}},
		{"a newer forced straddler waits", []span{a, straddler}, forcedBefore(2 * day), never, 0, 0, []span{a}},
		{"the oldest forced straddler goes alone", []span{a, oldStraddler}, forcedBefore(day), never, 0, 0, []span{oldStraddler}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, timebucket.Forced(tt.src, bounds, sizeOf, tt.forced, tt.sealed, tt.capBytes, tt.maxParts))
		})
	}
}

// TestForcedDrains is the convergence property over random buckets: repeatedly rewriting what
// [timebucket.Forced] picks keeps every multi-part merge within the cap and the part count, and
// drains the forced set.
func TestForcedDrains(t *testing.T) {
	t.Parallel()

	const cutoff = 3 * hour

	for seed := range uint64(64) {
		rng := rand.New(rand.NewPCG(seed, seed^0x9e37))
		capBytes := 1 + rng.Int64N(64)
		maxParts := rng.IntN(5)

		src := make([]span, 1+rng.IntN(32))
		for i := range src {
			lo := rng.Int64N(4 * hour)
			src[i] = span{lo: lo, hi: lo + rng.Int64N(hour/2), size: 1 + rng.Int64N(32)}
		}

		forced := forcedBefore(cutoff)

		for cycle := 0; ; cycle++ {
			require.Less(t, cycle, 1000, "seed %d: the forced set never drained", seed)

			got := timebucket.Forced(src, bounds, sizeOf, forced, sealedFrom(capBytes), capBytes, maxParts)
			if len(got) == 0 {
				break
			}

			var total int64
			for _, s := range got {
				total += s.size
			}

			if len(got) > 1 {
				assert.LessOrEqual(t, total, capBytes, "seed %d cycle %d", seed, cycle)

				if maxParts > 0 {
					assert.LessOrEqual(t, len(got), maxParts, "seed %d cycle %d", seed, cycle)
				}
			}

			src = rewrite(src, got, cutoff)
		}

		for _, s := range src {
			assert.False(t, forced(s), "seed %d: %+v still forced", seed, s)
		}
	}
}

// rewrite replaces picked with one part holding what survives the cutoff, as a retention merge does.
func rewrite(src, picked []span, cutoff int64) []span {
	out := make([]span, 0, len(src))
	merged := span{lo: 1 << 62, hi: -1 << 62}

	for _, s := range src {
		if !slices.Contains(picked, s) {
			out = append(out, s)

			continue
		}

		merged.lo, merged.hi = min(merged.lo, max(s.lo, cutoff)), max(merged.hi, s.hi)
		merged.size += s.size
	}

	if merged.hi >= merged.lo {
		out = append(out, merged)
	}

	return out
}
