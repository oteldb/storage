package promql

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

type gridCounterFixture struct {
	fetch.Fetcher

	request fetch.Request
	group   []byte
	calls   int
}

func (f *gridCounterFixture) CountGrid(_ context.Context, r fetch.Request, _ fetch.WindowGrid, group []byte) (map[string][]int, error) {
	f.calls++
	f.request, f.group = r, group
	return map[string][]int{"a": {1, 0, 2}}, nil
}

type gridWrapper struct{ fetch.Fetcher }

func (f gridWrapper) Unwrap() fetch.Fetcher { return f.Fetcher }

func TestCountSeriesGridAdapter(t *testing.T) {
	t.Parallel()
	f := &gridCounterFixture{}
	q, err := NewQueryable(gridWrapper{f}, "tenant").Querier(5, 1000)
	require.NoError(t, err)
	counter := q.(*querier)
	grid := fetch.WindowGrid{Start: 100, Step: 100, Width: 200, NumSteps: 3}
	matcher := labels.MustNewMatcher(labels.MatchEqual, "instance", "host-1")
	got, err := counter.CountSeriesGrid(t.Context(), grid, "job", matcher)
	require.NoError(t, err)
	require.Equal(t, map[string][]uint64{"a": {1, 0, 2}}, got)
	require.Equal(t, []byte("job"), f.group)
	require.Equal(t, int64(5_000_000), f.request.Start)
	require.Equal(t, int64(1_000_000_000), f.request.End)
	require.Equal(t, "tenant", string(f.request.Tenant))
	require.Len(t, f.request.Matchers, 1)
	require.NotNil(t, f.request.Matchers[0].Spec)
	_, err = counter.CountSeriesGrid(t.Context(), grid, "__kind__", matcher)
	require.ErrorIs(t, err, fetch.ErrCountGridUnsupported)
	_, err = counter.CountSeriesGrid(t.Context(), grid, "job", labels.MustNewMatcher(labels.MatchEqual, "__kind__", "0"))
	require.ErrorIs(t, err, fetch.ErrCountGridUnsupported)
	require.Equal(t, 1, f.calls)
	q, err = NewQueryable(struct{ fetch.Fetcher }{f}, "tenant").Querier(5, 1000)
	require.NoError(t, err)
	_, err = q.(*querier).CountSeriesGrid(t.Context(), grid, "job", matcher)
	require.ErrorIs(t, err, fetch.ErrCountGridUnsupported)
	require.Nil(t, fetch.GridCounterOf(fetch.Merge(f, f)), "fanout needs its own exact count implementation")
}
