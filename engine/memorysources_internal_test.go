package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
)

func TestMemoryRunAllReadShapes(t *testing.T) {
	t.Parallel()
	e := New(Config{Backend: backend.Memory(), Prefix: "run-read"})
	s := apiSeries()
	e.head.registerSeries(s)
	run, err := newMemoryRun([]memoryRunSeries{{id: s.Hash(), end: 3}},
		[]int64{10, 10, 20}, []float64{2, 3, 1}, nil, nil)
	require.NoError(t, err)
	path := newMemoryDirectoryPath(nil)
	view := &memoryReadView{}
	view.inherit(nil)
	view.active = path.insert(nil, 1, run)
	view.registry = e.head.series.View()
	path.close()
	run.release()
	e.readView = view
	t.Cleanup(func() { view.release() })
	req := fetch.Request{Start: 0, End: 30}
	iterator, err := e.Fetch(t.Context(), req)
	require.NoError(t, err)
	batch, err := iterator.Next(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20}, batch.Timestamps)
	require.Equal(t, []float64{3, 1}, batch.Values)
	require.NoError(t, iterator.Close())
	count, err := e.Count(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	groups, err := e.CountBy(t.Context(), req, []byte("job"))
	require.NoError(t, err)
	require.Equal(t, map[string]int{"api": 1}, groups)
	listed, err := e.Series(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	names, err := e.LabelNames(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, []string{"job"}, names)
	values, err := e.LabelValues(t.Context(), req, []byte("job"))
	require.NoError(t, err)
	require.Equal(t, []string{"api"}, values)
	grid, err := e.CountGrid(t.Context(), req, fetch.WindowGrid{Start: 10, Step: 10, Width: 10, NumSteps: 3}, []byte("job"))
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"api": {1, 1, 0}}, grid)
	aggregates, err := e.AggregateRange(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, int64(2), aggregates[s.Hash()].Rows)
	require.InDelta(t, 4, aggregates[s.Hash()].Sum, 0)
	steps, err := e.AggregateStep(t.Context(), req, 10)
	require.NoError(t, err)
	require.NotEmpty(t, steps[s.Hash()])
	windows, err := e.AggregateWindow(t.Context(), req, WindowSpec{Step: 10, Window: 20, Anchor: 0})
	require.NoError(t, err)
	require.NotEmpty(t, windows[s.Hash()])
}
