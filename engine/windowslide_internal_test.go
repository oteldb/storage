package engine

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bruteSlide is the O(steps × entries) definition the sliding accumulator has to reproduce: for
// every step-aligned t, fold the entries with t-window < end ≤ t.
func bruteSlide(ents []windowEnt, step, window, phase, end int64) []WindowAgg {
	if len(ents) == 0 {
		return nil
	}

	var out []WindowAgg

	first := phase + ceilStep(ents[0].end-phase, step)
	for t := first; t <= min(ents[len(ents)-1].end+window-1, end); t += step {
		var a SeriesAgg

		for _, e := range ents {
			if e.end <= t-window || e.end > t {
				continue
			}

			if a.Count == 0 {
				a.Min, a.Max = e.min, e.max
			}

			a.Count += e.count
			a.Sum += e.sum
			a.Min, a.Max = min(a.Min, e.min), max(a.Max, e.max)
		}

		if a.Count > 0 {
			out = append(out, WindowAgg{End: t, SeriesAgg: a})
		}
	}

	return out
}

// TestWindowSliderMatchesBrute is the property test for the accumulator itself: random entry
// streams — with gaps, ties and duplicate step boundaries — must slide to exactly what the
// definition folds, at every overlap factor. It also checks the slider is reusable: one instance
// runs every case, so a leftover deque front or tail would surface as a wrong extremum.
func TestWindowSliderMatchesBrute(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(1, 2))

	var s windowSlider

	for range 200 {
		var (
			step = int64(1 + rnd.IntN(4))
			n    = 1 + rnd.IntN(40)
			ents = make([]windowEnt, 0, n)
			end  = int64(0)
		)

		for range n {
			end += int64(rnd.IntN(6)) * step // gaps, and repeats that must not double-count
			if end == 0 || (len(ents) > 0 && ents[len(ents)-1].end == end) {
				end += step
			}

			v := float64(rnd.IntN(11) - 5) // a small alphabet, so ties are common
			ents = append(ents, windowEnt{end: end, count: 1 + int64(rnd.IntN(3)), sum: v, min: v, max: v})
		}

		for _, mult := range []int64{1, 2, 3, 8, 25} {
			window := step * mult
			last := ents[len(ents)-1].end
			phase := int64(rnd.IntN(int(step))) // an arbitrary evaluation-grid anchor, as PromQL has

			got := s.slide(ents, step, window, phase, last+window)
			want := bruteSlide(ents, step, window, phase, last+window)

			require.Lenf(t, got, len(want), "step=%d window=%d phase=%d", step, window, phase)

			for i := range want {
				assert.Equalf(t, want[i].End, got[i].End, "step=%d window=%d window %d end", step, window, i)
				assert.Equalf(t, want[i].Count, got[i].Count, "step=%d window=%d window %d count", step, window, i)
				assert.InDeltaf(t, want[i].Sum, got[i].Sum, 1e-9, "step=%d window=%d window %d sum", step, window, i)
				assert.InDeltaf(t, want[i].Min, got[i].Min, 0, "step=%d window=%d window %d min", step, window, i)
				assert.InDeltaf(t, want[i].Max, got[i].Max, 0, "step=%d window=%d window %d max", step, window, i)
			}
		}
	}
}

// TestWindowSliderClipsToEnd pins the request-end clamp: windows that would extend past the fetched
// data are not reported, however far the window reaches.
func TestWindowSliderClipsToEnd(t *testing.T) {
	t.Parallel()

	ents := []windowEnt{{end: 10, count: 1, sum: 1, min: 1, max: 1}}

	var s windowSlider

	got := s.slide(ents, 10, 50, 0, 30)
	require.Len(t, got, 3) // t = 10, 20, 30 — not the 40 and 50 the window would still cover

	assert.Equal(t, int64(30), got[len(got)-1].End)
	assert.Empty(t, s.slide(ents, 10, 50, 0, 0), "an end before the first entry yields nothing")
}
