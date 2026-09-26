package timebucket

import (
	"maps"
	"slices"
)

// MaxOpenWriters bounds the day-keyed writers one merge holds open: a month of days, so a batch of
// straddlers spanning weeks is still written in one pass without finishing a day early.
const MaxOpenWriters = 32

// Router hands a merge's output rows to one writer per top-level bucket, so a merge whose rows span
// several days writes a part per day in a single pass over its inputs.
//
// At most MaxOpen writers are open, and together they hold less than ResidentLimit (≤ 0 ⇒
// unbounded) after every run [Router.Append] routes: past either bound the writer holding the most is
// finished early. A day can therefore end up in several parts, each of which still fits the day, and
// the ladder merges them later. The limit is checked per run rather than per series, so one series
// spanning every open day overshoots it by one run, not by one writer's worth per day.
type Router[W any] struct {
	Open          func(bucket int64) (W, error)
	Finish        func(W) error
	Resident      func(W) int64
	MaxOpen       int
	ResidentLimit int64

	open map[int64]W
	// peak is the most the open writers held together, seen as each run landed; run the most one
	// run added.
	peak, run int64
}

// Append routes one run of rows, all in the top-level bucket holding ts, to that day's writer: add
// appends it and reports whether the writer is full, which finishes it. The open writers are then
// shed back under ResidentLimit.
func (r *Router[W]) Append(ts int64, add func(W) (full bool, err error)) error {
	w, err := r.Writer(ts)
	if err != nil {
		return err
	}

	before := r.Resident(w)

	full, err := add(w)
	if err != nil {
		return err
	}

	r.run = max(r.run, r.Resident(w)-before)

	if full {
		if err := r.Seal(ts); err != nil {
			return err
		}
	}

	return r.Shed()
}

// Peak returns the most the open writers held together as a run landed, and the most one run added
// to a writer: the first exceeds ResidentLimit by at most the second.
func (r *Router[W]) Peak() (total, run int64) { return r.peak, r.run }

// Writer returns the writer for the top-level bucket holding ts, opening it if needed.
func (r *Router[W]) Writer(ts int64) (W, error) {
	b := Of(ts, Top())
	if w, ok := r.open[b]; ok {
		return w, nil
	}

	var zero W

	if r.MaxOpen > 0 && len(r.open) >= r.MaxOpen {
		if err := r.finishLargest(); err != nil {
			return zero, err
		}
	}

	w, err := r.Open(b)
	if err != nil {
		return zero, err
	}

	if r.open == nil {
		r.open = make(map[int64]W)
	}

	r.open[b] = w

	return w, nil
}

// Seal finishes the writer for the bucket holding ts, if one is open.
func (r *Router[W]) Seal(ts int64) error {
	b := Of(ts, Top())

	w, ok := r.open[b]
	if !ok {
		return nil
	}

	delete(r.open, b)

	return r.Finish(w)
}

// Shed finishes the largest writers while the open ones together hold ResidentLimit or more.
func (r *Router[W]) Shed() error {
	if r.ResidentLimit <= 0 {
		return nil
	}

	for first := true; len(r.open) > 0; first = false {
		var total int64
		for _, w := range r.open {
			total += r.Resident(w)
		}

		if first {
			r.peak = max(r.peak, total)
		}

		if total < r.ResidentLimit {
			return nil
		}

		if err := r.finishLargest(); err != nil {
			return err
		}
	}

	return nil
}

// Close finishes every open writer, oldest bucket first.
func (r *Router[W]) Close() error {
	for _, b := range slices.Sorted(maps.Keys(r.open)) {
		w := r.open[b]
		delete(r.open, b)

		if err := r.Finish(w); err != nil {
			return err
		}
	}

	return nil
}

// Drop hands every open writer to release unfinished, for a merge that failed.
func (r *Router[W]) Drop(release func(W)) {
	for b, w := range r.open {
		delete(r.open, b)
		release(w)
	}
}

func (r *Router[W]) finishLargest() error {
	var (
		largest int64
		size    int64 = -1
	)

	for b, w := range r.open {
		if s := r.Resident(w); s > size || (s == size && b < largest) {
			largest, size = b, s
		}
	}

	w := r.open[largest]
	delete(r.open, largest)

	return r.Finish(w)
}

// Runs calls fn once per run of the ascending ts inside one top-level bucket, with the run's
// [lo, hi) index range.
func Runs(ts []int64, fn func(lo, hi int) error) error {
	for lo := 0; lo < len(ts); {
		end := End(ts[lo], Top())

		hi := lo + 1
		for hi < len(ts) && ts[hi] <= end {
			hi++
		}

		if err := fn(lo, hi); err != nil {
			return err
		}

		lo = hi
	}

	return nil
}
