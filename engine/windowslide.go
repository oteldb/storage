package engine

import "math"

// windowEnt is one contribution to the sliding accumulator: an aggregate that enters a window when
// the evaluation timestamp reaches end, and leaves it once the window's open lower bound passes it.
// A fine bucket (b, b+step] enters at b+step; a single sample enters at its own timestamp. Every
// entry holds at least one sample, so an empty window is an empty span of entries.
type windowEnt struct {
	end   int64
	count int64
	sum   float64

	min, max float64
}

// windowSlider walks a series' entries once and emits one aggregate per evaluation step. Count and
// sum slide by arithmetic — add what enters, subtract what leaves — but an extremum cannot be
// subtracted back out: dropping the current minimum would force a rescan of the whole window. So
// each extremum rides a **monotonic deque** of entry indices instead, front to back in increasing
// (min) or decreasing (max) order: an arriving entry pops every tail entry it dominates, because
// such an entry is no better *and* expires no later, so it can never be a later window's extremum
// again. The front is then always the current window's answer. Each entry is pushed once and popped
// once, so a step stays O(1) amortized however many windows overlap.
//
// The zero value is ready. Reusing one slider across the series of a call keeps both deques'
// backing arrays.
type windowSlider struct {
	// mins/maxs hold entry indices; the live deque is the slice from the matching front index, which
	// advances on expiry rather than reslicing (so the backing array's head is not given up).
	mins, maxs   []int
	minLo, maxLo int
}

// slide folds ents — ascending by end, each holding at least one sample — into one aggregate per
// step-aligned evaluation timestamp t whose window (t-window, t] is non-empty, ascending. An entry
// is in the window exactly when t-window < end ≤ t.
//
// Runs of empty windows are skipped rather than walked, so a sparse series costs its samples, not
// its span. end bounds the evaluation grid at the request's end: a window past it would be missing
// the data that follows, so it is not reported.
func (s *windowSlider) slide(ents []windowEnt, step, window, end int64) []WindowAgg {
	if len(ents) == 0 {
		return nil
	}

	s.reset()

	// The evaluation timestamps that can see any entry: from the first step at or after the earliest
	// entry's end, through the last step that still holds the latest entry (t-window < end).
	lo := ceilStep(ents[0].end, step)

	latest := ents[len(ents)-1].end

	horizon := latest + window - 1
	if horizon < latest { // window so wide it overflows; there is no time past the end of time
		horizon = math.MaxInt64
	}

	hi := bucketStart(min(horizon, end), step)

	var (
		dst  []WindowAgg
		acc  SeriesAgg
		head int // entries already added
		tail int // entries already expired
	)

	for t := lo; t <= hi; t += step {
		// An entry enters the window once t reaches its end, and stays until the lower bound does.
		for head < len(ents) && ents[head].end <= t {
			acc.Count += ents[head].count
			acc.Sum += ents[head].sum
			s.push(ents, head)
			head++
		}

		// An entry whose end has fallen to the window's open lower bound has left it: (t-window, t].
		for tail < head && ents[tail].end <= t-window {
			acc.Count -= ents[tail].count
			acc.Sum -= ents[tail].sum
			tail++
		}

		s.expire(tail)

		if acc.Count > 0 {
			dst = append(dst, WindowAgg{
				End: t,
				SeriesAgg: SeriesAgg{
					Count: acc.Count,
					Sum:   acc.Sum,
					Min:   ents[s.mins[s.minLo]].min,
					Max:   ents[s.maxs[s.maxLo]].max,
				},
			})

			continue
		}

		acc.Sum = 0 // the window emptied: drop whatever rounding the add/subtract pairs left behind

		if head == len(ents) {
			break // every entry has expired and none is left to enter
		}

		if next := ceilStep(ents[head].end, step); next > t {
			t = next - step // jump the gap; the loop's post statement lands us on next
		}
	}

	return dst
}

func (s *windowSlider) reset() {
	s.mins, s.maxs = s.mins[:0], s.maxs[:0]
	s.minLo, s.maxLo = 0, 0
}

// push adds entry i to both deques, dropping the tail entries it dominates.
func (s *windowSlider) push(ents []windowEnt, i int) {
	for len(s.mins) > s.minLo && ents[s.mins[len(s.mins)-1]].min >= ents[i].min {
		s.mins = s.mins[:len(s.mins)-1]
	}

	s.mins = append(s.mins, i)

	for len(s.maxs) > s.maxLo && ents[s.maxs[len(s.maxs)-1]].max <= ents[i].max {
		s.maxs = s.maxs[:len(s.maxs)-1]
	}

	s.maxs = append(s.maxs, i)
}

// expire drops the deque fronts that have left the window (every entry before tail).
func (s *windowSlider) expire(tail int) {
	for len(s.mins) > s.minLo && s.mins[s.minLo] < tail {
		s.minLo++
	}

	for len(s.maxs) > s.maxLo && s.maxs[s.maxLo] < tail {
		s.maxLo++
	}
}

// ceilStep rounds ts up to a multiple of step.
func ceilStep(ts, step int64) int64 {
	b := bucketStart(ts, step)
	if b == ts {
		return b
	}

	return b + step
}
