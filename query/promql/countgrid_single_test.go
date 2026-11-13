package promql

import (
	"context"
	"math"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

type singleGridFixture struct {
	counts         map[string][]int
	err            error
	request        fetch.Request
	grid           fetch.WindowGrid
	group          []byte
	grids, fetches int
}

func (f *singleGridFixture) CountGrid(_ context.Context, r fetch.Request, g fetch.WindowGrid, group []byte) (map[string][]int, error) {
	f.grids++
	f.request = r
	f.grid = g
	f.group = group
	return f.counts, f.err
}

func (f *singleGridFixture) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	f.fetches++
	return fetch.NewSliceIterator(nil), nil
}

func TestSingleCountGrid(t *testing.T) {
	t.Parallel()
	f := &singleGridFixture{counts: map[string][]int{"": {5}}}
	query, err := NewQueryable(f, "tenant").Querier(0, 20)
	require.NoError(t, err)
	q := query.(*querier)
	n, err := q.CountSeries(t.Context(), 5, 10)
	require.NoError(t, err)
	require.EqualValues(t, 5, n)
	require.Equal(t, fetch.WindowGrid{Start: 10000000, Step: 1, Width: 5000001, NumSteps: 1}, f.grid)
	require.Equal(t, int64(5000000), f.request.Start)
	require.Equal(t, int64(10000000), f.request.End)
	require.NotNil(t, f.request.Scope)
	require.Nil(t, f.group)
	require.Zero(t, f.fetches)
	f.counts = map[string][]int{"api": {3}, "worker": {2}, "zero": {0}}
	got, err := q.CountSeriesBy(t.Context(), 5, 10, "job")
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"api": 3, "worker": 2}, got)
	require.Equal(t, []byte("job"), f.group)
}

func TestSingleCountGridFallbacks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                 string
		counts               map[string][]int
		err                  error
		start, end           int64
		wantError, wantFetch bool
	}{
		{name: "unsupported", err: fetch.ErrCountGridUnsupported, start: 0, end: 10, wantFetch: true},
		{name: "wide-window", start: math.MinInt64, end: math.MaxInt64, wantFetch: true},
		{name: "invalid-shape", counts: map[string][]int{"": {1, 2}}, start: 0, end: 10, wantError: true},
		{name: "unexpected-group", counts: map[string][]int{"job": {1}}, start: 0, end: 10, wantError: true},
		{name: "negative", counts: map[string][]int{"": {-1}}, start: 0, end: 10, wantError: true},
		{name: "read-error", err: errors.New("read failed"), start: 0, end: 10, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &singleGridFixture{counts: tc.counts, err: tc.err}
			query, err := NewQueryable(f, "tenant").Querier(0, 20)
			require.NoError(t, err)
			_, err = query.(*querier).CountSeries(t.Context(), tc.start, tc.end)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.wantFetch {
				require.Equal(t, 1, f.fetches)
			} else {
				require.Zero(t, f.fetches)
			}
		})
	}
}
