package timebucket_test

import (
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/timebucket"
)

type writer struct {
	bucket int64
	rows   int64
}

// recorder is a Router over writers counting rows, recording the buckets it finishes in order.
type recorder struct {
	timebucket.Router[*writer]

	finished []int64
	live     map[*writer]struct{}
	failOpen error
}

// held is what the open writers hold together.
func (r *recorder) held() int64 {
	var n int64
	for w := range r.live {
		n += w.rows
	}

	return n
}

func newRecorder(maxOpen int, limit int64) *recorder {
	r := &recorder{live: map[*writer]struct{}{}}
	r.Router = timebucket.Router[*writer]{
		Open: func(b int64) (*writer, error) {
			if r.failOpen != nil {
				return nil, r.failOpen
			}

			w := &writer{bucket: b}
			r.live[w] = struct{}{}

			return w, nil
		},
		Finish: func(w *writer) error {
			delete(r.live, w)
			r.finished = append(r.finished, w.bucket)

			return nil
		},
		Resident:      func(w *writer) int64 { return w.rows },
		MaxOpen:       maxOpen,
		ResidentLimit: limit,
	}

	return r
}

func (r *recorder) add(t *testing.T, ts, rows int64) {
	t.Helper()

	w, err := r.Writer(ts)
	require.NoError(t, err)

	w.rows += rows
}

func TestRouterWriterPerDay(t *testing.T) {
	t.Parallel()

	r := newRecorder(0, 0)
	r.add(t, day+hour, 1)
	r.add(t, 2*hour, 1)
	r.add(t, day+2*hour, 1)

	require.NoError(t, r.Close())
	assert.Equal(t, []int64{0, day}, r.finished, "one writer per day, finished oldest first")
}

func TestRouterSeal(t *testing.T) {
	t.Parallel()

	r := newRecorder(0, 0)
	r.add(t, hour, 1)
	require.NoError(t, r.Seal(2*hour))
	require.NoError(t, r.Seal(day), "sealing a day with no writer is a no-op")
	r.add(t, 3*hour, 1)

	require.NoError(t, r.Close())
	assert.Equal(t, []int64{0, 0}, r.finished, "a sealed day opens a fresh writer")
}

func TestRouterMaxOpenFinishesLargest(t *testing.T) {
	t.Parallel()

	r := newRecorder(2, 0)
	r.add(t, 0, 5)
	r.add(t, day, 1)
	r.add(t, 2*day, 1)

	assert.Equal(t, []int64{0}, r.finished, "opening a third day finishes the largest")
	require.NoError(t, r.Close())
	assert.Equal(t, []int64{0, day, 2 * day}, r.finished)
}

func TestRouterShed(t *testing.T) {
	t.Parallel()

	r := newRecorder(0, 10)
	r.add(t, 0, 4)
	r.add(t, day, 5)
	require.NoError(t, r.Shed())
	assert.Empty(t, r.finished, "under the limit")

	r.add(t, 2*day, 3)
	require.NoError(t, r.Shed())
	assert.Equal(t, []int64{day}, r.finished, "over the limit the largest goes first, until under it")

	unbounded := newRecorder(0, 0)
	unbounded.add(t, 0, 1<<40)
	require.NoError(t, unbounded.Shed())
	assert.Empty(t, unbounded.finished)
}

// TestRouterAppendShedsPerRun is the aggregate bound: one series spanning every open day must not
// fill each day's writer before the limit is checked, so after every run the open writers hold
// under the limit and at their peak exceed it by at most one run.
func TestRouterAppendShedsPerRun(t *testing.T) {
	t.Parallel()

	for _, days := range []int64{17, 32} {
		const (
			limit   = 100
			runRows = 7
		)

		r := newRecorder(timebucket.MaxOpenWriters, limit)

		for series := range 20 {
			for d := range days {
				require.NoError(t, r.Append(d*day+int64(series), func(w *writer) (bool, error) {
					w.rows += runRows

					return false, nil
				}))

				require.Less(t, r.held(), int64(limit), "%d days, series %d, day %d", days, series, d)
			}
		}

		peak, run := r.Peak()
		assert.Equal(t, int64(runRows), run)
		assert.LessOrEqual(t, peak, int64(limit+runRows), "%d days", days)
	}
}

func TestRouterAppendSealsFullWriter(t *testing.T) {
	t.Parallel()

	r := newRecorder(0, 0)
	require.NoError(t, r.Append(hour, func(*writer) (bool, error) { return true, nil }))
	assert.Equal(t, []int64{0}, r.finished)

	errAdd := errors.New("add")
	require.ErrorIs(t, r.Append(hour, func(*writer) (bool, error) { return false, errAdd }), errAdd)

	r.failOpen = errAdd
	require.ErrorIs(t, r.Append(day, func(*writer) (bool, error) { return false, nil }), errAdd)
}

func TestRouterDrop(t *testing.T) {
	t.Parallel()

	r := newRecorder(0, 0)
	r.add(t, 0, 1)
	r.add(t, day, 1)

	var dropped int

	r.Drop(func(*writer) { dropped++ })
	assert.Equal(t, 2, dropped)
	require.NoError(t, r.Close())
	assert.Empty(t, r.finished, "a dropped writer is never finished")
}

func TestRouterErrors(t *testing.T) {
	t.Parallel()

	errOpen := errors.New("open")
	r := newRecorder(1, 0)
	r.failOpen = errOpen

	_, err := r.Writer(0)
	require.ErrorIs(t, err, errOpen)

	errFinish := errors.New("finish")
	r = newRecorder(1, 1)
	r.Finish = func(*writer) error { return errFinish }
	r.add(t, 0, 2)

	_, err = r.Writer(day)
	require.ErrorIs(t, err, errFinish, "evicting for a new day surfaces the finish error")

	r.add(t, 0, 2)
	require.ErrorIs(t, r.Shed(), errFinish)

	r.add(t, 0, 0)
	require.ErrorIs(t, r.Seal(0), errFinish)

	r.add(t, 0, 1)
	require.ErrorIs(t, r.Close(), errFinish)
}

func TestRuns(t *testing.T) {
	t.Parallel()

	ts := []int64{hour, 2 * hour, day, day + hour, 3 * day}

	var got [][2]int

	require.NoError(t, timebucket.Runs(ts, func(lo, hi int) error {
		got = append(got, [2]int{lo, hi})

		return nil
	}))
	assert.Equal(t, [][2]int{{0, 2}, {2, 4}, {4, 5}}, got)

	errStop := errors.New("stop")
	require.ErrorIs(t, timebucket.Runs(ts, func(int, int) error { return errStop }), errStop)
	require.NoError(t, timebucket.Runs(nil, func(int, int) error { return errStop }))
}
