package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func knownHeadEngine(t *testing.T, be backend.Backend) *Engine {
	t.Helper()
	e := New(Config{Backend: be, Prefix: "known", DiskSeries: true, SeparateSeries: true, MetricSeriesChunks: true, KnownSeriesHead: true, AggregateStats: true, OOOWindow: 10})
	t.Cleanup(func() { require.NoError(t, e.CloseWAL()) })
	return e
}
func knownHeadAppend(t *testing.T, e *Engine, s signal.Series, ts int64, v float64) {
	t.Helper()
	ok, err := e.Append(s, ts, v)
	require.NoError(t, err)
	require.True(t, ok)
}
func knownHeadRows(t *testing.T, e *Engine, r fetch.Request) []*fetch.Batch {
	t.Helper()
	it, err := e.Fetch(t.Context(), r)
	require.NoError(t, err)
	rows, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.NoError(t, it.Close())
	return rows
}

func TestKnownHeadLifecycle(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := knownHeadEngine(t, be)
	s := diskQuerySeries(signal.StringValue([]byte("a")))
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, s, 100, 2)
	require.Zero(t, e.head.series.Len())
	require.Equal(t, 1, e.head.known.len())
	require.Greater(t, e.residentSampleHeadBytes(), e.HeadBytes())
	r := fetch.Request{Start: 90, End: 110}
	check := func() {
		rows := knownHeadRows(t, e, r)
		require.Len(t, rows, 1)
		require.Equal(t, s.Hash(), rows[0].Series.Hash())
		require.Equal(t, []float64{2}, rows[0].Values)
		count, err := e.Count(t.Context(), r)
		require.NoError(t, err)
		require.Equal(t, 1, count)
		values, err := e.LabelValues(t.Context(), r, []byte("host"))
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, values)
	}
	check()
	for _, value := range []string{"a", "b"} {
		eq := fetch.EqualMatcher{Name: "host", Value: value}
		q := r
		q.Matchers = []fetch.Matcher{{Name: []byte("host"), Match: eq.Predicate(), Spec: &eq}}
		count, err := e.Count(t.Context(), q)
		require.NoError(t, err)
		if value == "a" {
			require.Equal(t, 1, count)
		} else {
			require.Zero(t, count)
		}
	}
	ok, err := e.Append(s, 89, 3)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, e.Flush(t.Context()))
	require.Nil(t, e.head.known)
	check()
	other := knownHeadEngine(t, be)
	require.NoError(t, other.LoadParts(t.Context()))
	require.Len(t, knownHeadRows(t, other, r), 1)
}

func TestKnownHeadMixedAndCanonicalConflict(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	s := benchSeries(1)
	unknown := benchSeries(2)
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	_, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{100}, []float64{2}, nil, func(int) signal.Series { return unknown }, AppendLimits{})
	require.ErrorContains(t, err, "canonical identity conflict")
	require.Zero(t, e.HeadSampleCount())
	require.Nil(t, e.head.known)
	knownHeadAppend(t, e, s, 100, 2)
	knownHeadAppend(t, e, unknown, 100, 3)
	require.Equal(t, 1, e.head.series.Len())
	require.Equal(t, 1, e.head.known.len())
	require.NoError(t, e.Flush(t.Context()))
	require.Equal(t, 2, e.SeriesCount())
	require.Len(t, knownHeadRows(t, e, fetch.Request{Start: 90, End: 110}), 2)
}

func TestKnownHeadFlushFailure(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := knownHeadEngine(t, be)
	s := benchSeries(1)
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, s, 100, 2)
	failure := errors.New("known output failed")
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: failure, Match: func(op faultbackend.Op) bool { return strings.HasSuffix(op.Key, "/sidx") }})
	require.ErrorIs(t, e.Flush(t.Context()), failure)
	require.Equal(t, 1, e.HeadSampleCount())
	require.Len(t, knownHeadRows(t, e, fetch.Request{Start: 90, End: 110}), 1)
	be.Reset()
	require.NoError(t, e.Flush(t.Context()))
	require.Nil(t, e.head.known)
}

func TestKnownHeadFrozenAndLastWrite(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	s := benchSeries(1)
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, s, 100, 2)
	entered, done := make(chan struct{}), make(chan struct{})
	first := true
	e.cfg.FlushAdmission = func(context.Context, int64) (func(), error) {
		if first {
			first = false
			close(entered)
			<-done
		}
		return func() {}, nil
	}
	errCh := make(chan error, 1)
	go func() { errCh <- e.Flush(t.Context()) }()
	<-entered
	knownHeadAppend(t, e, s, 100, 3)
	rows := knownHeadRows(t, e, fetch.Request{Start: 90, End: 110})
	require.Len(t, rows, 1)
	require.Equal(t, []float64{3}, rows[0].Values)
	close(done)
	require.NoError(t, <-errCh)
	require.NoError(t, e.Flush(t.Context()))
	rows = knownHeadRows(t, e, fetch.Request{Start: 90, End: 110})
	require.Equal(t, []float64{3}, rows[0].Values)
}

func TestKnownHeadWALReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	be := backend.Memory()
	e := knownHeadEngine(t, be)
	e.cfg.WAL = w
	s := benchSeries(1)
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, s, 100, 2)
	require.NoError(t, e.CloseWAL())
	// Existing formats keep the new buffer independently replayable without the old index.
	standalone := New(Config{})
	require.NoError(t, standalone.Replay(t.Context(), dir))
	require.Equal(t, 1, standalone.HeadSampleCount())
	other := knownHeadEngine(t, be)
	require.NoError(t, other.LoadParts(t.Context()))
	require.NoError(t, other.Replay(t.Context(), dir))
	require.Zero(t, other.head.series.Len())
	require.Equal(t, 1, other.head.known.len())
	require.Len(t, knownHeadRows(t, other, fetch.Request{Start: 90, End: 110}), 1)
}

func TestKnownHeadInvalidConfig(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := New(Config{Backend: be, KnownSeriesHead: true})
	_, err := e.Append(benchSeries(1), 1, 1)
	require.ErrorContains(t, err, "requires separate series")
	keys, err := be.List(t.Context(), "")
	require.NoError(t, err)
	require.Empty(t, keys)
}

func TestKnownHeadReadShapesAndPressure(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	s := diskQuerySeries(signal.StringValue([]byte("a")))
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, s, 100, 2)
	r := fetch.Request{Start: 90, End: 110}
	grid, err := e.CountGrid(t.Context(), r, fetch.WindowGrid{Start: 100, Step: 10, Width: 10, NumSteps: 2}, []byte("host"))
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"a": {1, 0}}, grid)
	agg, err := e.AggregateRange(t.Context(), r)
	require.NoError(t, err)
	require.InDelta(t, float64(2), agg[s.Hash()].Sum, 0)
	e.cfg.ResidentHeadBytes = e.residentSampleHeadBytes()
	before := e.PartCount()
	knownHeadAppend(t, e, s, 101, 3)
	require.Greater(t, e.PartCount(), before)
	require.Zero(t, e.head.series.Len())
	require.Equal(t, 1, e.head.known.len())
	require.NoError(t, e.Merge(t.Context(), 90))
	_, err = e.PruneIdentitiesWith(t.Context(), PruneOptions{Force: true})
	require.NoError(t, err)
	require.Equal(t, 1, e.SeriesCount())
	require.Len(t, knownHeadRows(t, e, r), 1)
}

func TestKnownHeadCompletedOutputFailureReleasesPins(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := knownHeadEngine(t, be)
	s := benchSeries(1)
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	for i := range 8 {
		knownHeadAppend(t, e, s, int64(100+i), float64(i))
	}
	e.cfg.MaxPartBytes = 2 * partRowBytes
	snapshot := e.head.known.snapshot
	before := snapshot.refs.Load()
	writes := 0
	failure := errors.New("second known output failed")
	be.Add(faultbackend.Rule{Kind: faultbackend.Write, Err: failure, Match: func(op faultbackend.Op) bool {
		if strings.HasSuffix(op.Key, "/sidx") {
			writes++
			return writes == 2
		}
		return false
	}})
	require.ErrorIs(t, e.Flush(t.Context()), failure)
	require.Equal(t, before, snapshot.refs.Load())
	require.Equal(t, 8, e.HeadSampleCount())
	be.Reset()
	require.NoError(t, e.Flush(t.Context()))
	require.Nil(t, e.head.known)
}

func TestKnownHeadWALFailureDoesNotPublish(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	e := knownHeadEngine(t, backend.Memory())
	e.cfg.WAL = w
	s := benchSeries(1)
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	before := e.seriesTable.refs.Load()
	require.NoError(t, w.Close())
	_, err = e.Append(s, 100, 2)
	require.Error(t, err)
	require.Nil(t, e.head.known)
	require.Zero(t, e.HeadSampleCount())
	require.Equal(t, before, e.seriesTable.refs.Load())
}

func TestKnownHeadCatalogFailureKeepsReplay(t *testing.T) {
	t.Parallel()
	be := newCleanupFaultBackend()
	e := knownHeadEngine(t, be)
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	e.cfg.WAL = w
	s := benchSeries(1)
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, s, 100, 2)
	failure := errors.New("catalog failed")
	be.Add(faultbackend.Rule{Kind: faultbackend.CompareAndSwap, Err: failure})
	require.ErrorIs(t, e.Flush(t.Context()), failure)
	require.Len(t, knownHeadRows(t, e, fetch.Request{Start: 90, End: 110}), 1)
	require.NoError(t, w.Sync())
	recovered := New(Config{})
	require.NoError(t, recovered.Replay(t.Context(), dir))
	require.Equal(t, 1, recovered.HeadSampleCount())
	be.Reset()
}

func TestKnownHeadMetadataCompaction(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	a, b := benchSeries(1), benchSeries(2)
	knownHeadAppend(t, e, a, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, b, 10, 2)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, a, 100, 3)
	old := e.head.known.snapshot
	changed, err := e.compactSeriesRuns(t.Context(), false)
	require.NoError(t, err)
	require.True(t, changed)
	require.NotSame(t, old, e.seriesTable)
	require.Len(t, knownHeadRows(t, e, fetch.Request{Start: 90, End: 110}), 1)
	require.NoError(t, e.Flush(t.Context()))
	require.Nil(t, e.head.known)
	require.Len(t, knownHeadRows(t, e, fetch.Request{Start: 90, End: 110}), 1)
}

func TestKnownHeadMissingCatalogRefusesFlush(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	s := benchSeries(1)
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, s, 100, 2)
	e.mu.Lock()
	prior := e.seriesTable
	e.seriesTable = newSeriesSnapshot(nil)
	e.mu.Unlock()
	require.ErrorContains(t, e.Flush(t.Context()), "lacks known head identity")
	require.Equal(t, 1, e.HeadSampleCount())
	require.Len(t, knownHeadRows(t, e, fetch.Request{Start: 90, End: 110}), 1)
	e.mu.Lock()
	e.seriesTable.release()
	e.seriesTable = prior
	e.mu.Unlock()
	require.NoError(t, e.Flush(t.Context()))
}

func TestKnownHeadQueryBudgetReleasesPin(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	s := benchSeries(1)
	knownHeadAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(t.Context()))
	knownHeadAppend(t, e, s, 100, 2)
	before := e.head.known.snapshot.refs.Load()
	for _, limit := range []int64{1, 4096} {
		budget := readbudget.New(limit)
		ctx := readbudget.With(t.Context(), budget)
		_, err := e.Fetch(ctx, fetch.Request{Start: 90, End: 110})
		require.ErrorIs(t, err, readbudget.ErrExceeded)
		require.Equal(t, limit, budget.Remaining())
		require.Equal(t, before, e.head.known.snapshot.refs.Load())
	}
}

func TestKnownHeadPreparedBatch(t *testing.T) {
	t.Parallel()
	e := knownHeadEngine(t, backend.Memory())
	dir := t.TempDir()
	w, err := wal.Create(dir, 0)
	require.NoError(t, err)
	e.cfg.WAL = w
	const n = 1024
	ids := make([]signal.SeriesID, n)
	ts := make([]int64, n)
	values := make([]float64, n)
	for i := range n {
		ids[i] = benchSeries(i).Hash()
		ts[i] = 10
		values[i] = 1
	}
	materialize := benchSeries
	result, err := e.AppendBatch(ids, ts, values, nil, materialize, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, n, result.Accepted)
	require.NoError(t, e.Flush(t.Context()))
	for i := range ts {
		ts[i] = 100
	}
	result, err = e.AppendBatch(ids, ts, values, nil, materialize, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, n, result.Accepted)
	require.Zero(t, e.head.series.Len())
	require.Equal(t, n, e.head.known.len())
	require.Equal(t, n, e.SeriesCount())
	require.NoError(t, e.Flush(t.Context()))
	require.Nil(t, e.head.known)
}
