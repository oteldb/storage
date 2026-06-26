package scale_test

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/scale"
	"github.com/oteldb/storage/signal"
)

// windowFetcher is a fake [fetch.Fetcher] over a single series' samples: each Fetch returns the
// samples whose timestamp falls in the request window, and records the windows it was asked for.
type windowFetcher struct {
	id  uint64
	ts  []int64
	val []float64

	calls   atomic.Int64
	mu      sync.Mutex
	windows [][2]int64 // (start, end) of each Fetch call
}

func (f *windowFetcher) Fetch(_ context.Context, r fetch.Request) (fetch.Iterator, error) {
	f.calls.Add(1)

	f.mu.Lock()
	f.windows = append(f.windows, [2]int64{r.Start, r.End})
	f.mu.Unlock()

	b := &fetch.Batch{ID: signal.SeriesID{Lo: f.id}}
	for i, t := range f.ts {
		if t >= r.Start && t <= r.End {
			b.Timestamps = append(b.Timestamps, t)
			b.Values = append(b.Values, f.val[i])
		}
	}

	if len(b.Timestamps) == 0 {
		return fetch.NewSliceIterator(nil), nil
	}

	return fetch.NewSliceIterator([]*fetch.Batch{b}), nil
}

func (f *windowFetcher) seenWindows() [][2]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	w := make([][2]int64, len(f.windows))
	copy(w, f.windows)
	sort.Slice(w, func(i, j int) bool { return w[i][0] < w[j][0] })

	return w
}

func drainFetch(t *testing.T, f fetch.Fetcher, r fetch.Request) []*fetch.Batch {
	t.Helper()
	it, err := f.Fetch(context.Background(), r)
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	return got
}

func TestSplitFetchesAlignedSubWindowsAndMerges(t *testing.T) {
	t.Parallel()

	inner := &windowFetcher{id: 7, ts: []int64{0, 10, 20, 30, 40}, val: []float64{0, 1, 2, 3, 4}}
	sf := scale.SplitFetcher{Inner: inner, Interval: 20}

	got := drainFetch(t, sf, fetch.Request{Start: 0, End: 49})

	require.Len(t, got, 1, "the split sub-windows merge back into one series")
	assert.Equal(t, []int64{0, 10, 20, 30, 40}, got[0].Timestamps, "all samples present, time-ordered")
	assert.Equal(t, []float64{0, 1, 2, 3, 4}, got[0].Values)

	// The window [0,49] aligns to a width-20 grid: [0,19], [20,39], [40,49].
	assert.Equal(t, [][2]int64{{0, 19}, {20, 39}, {40, 49}}, inner.seenWindows())
}

func TestSplitAlignmentIsIndependentOfRequestStart(t *testing.T) {
	t.Parallel()

	// A window not starting on a grid line still aligns its sub-windows to multiples of the
	// interval — only the first/last are clamped — so overlapping queries share sub-windows.
	inner := &windowFetcher{id: 1, ts: []int64{}, val: []float64{}}
	sf := scale.SplitFetcher{Inner: inner, Interval: 100}

	_ = drainFetch(t, sf, fetch.Request{Start: 150, End: 420})

	assert.Equal(t, [][2]int64{{150, 199}, {200, 299}, {300, 399}, {400, 420}}, inner.seenWindows())
}

func TestSplitPassThroughForNarrowOrDisabled(t *testing.T) {
	t.Parallel()

	// Window within a single sub-interval ⇒ exactly one inner Fetch with the original bounds.
	inner := &windowFetcher{id: 1, ts: []int64{5}, val: []float64{1}}
	sf := scale.SplitFetcher{Inner: inner, Interval: 100}
	_ = drainFetch(t, sf, fetch.Request{Start: 0, End: 50})
	assert.Equal(t, [][2]int64{{0, 50}}, inner.seenWindows(), "narrow window passes through unsplit")

	// Interval ≤ 0 disables splitting entirely.
	inner2 := &windowFetcher{id: 1, ts: []int64{5}, val: []float64{1}}
	sf2 := scale.SplitFetcher{Inner: inner2, Interval: 0}
	_ = drainFetch(t, sf2, fetch.Request{Start: 0, End: 1000})
	assert.Equal(t, [][2]int64{{0, 1000}}, inner2.seenWindows(), "Interval≤0 is a pass-through")
}

func TestSplitPropagatesError(t *testing.T) {
	t.Parallel()

	sf := scale.SplitFetcher{Inner: errFetcher{}, Interval: 10}
	_, err := sf.Fetch(context.Background(), fetch.Request{Start: 0, End: 100})
	require.Error(t, err, "a sub-window fetch error fails the whole split")
}

type errFetcher struct{}

func (errFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	return nil, assert.AnError
}
