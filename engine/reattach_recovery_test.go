package engine

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/faultbackend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func TestReattachPreservesSnapshotAndWAL(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	files, err := backendfile.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	be := faultbackend.Wrap(files)
	w, err := wal.Create(filepath.Join(dir, "wal"), 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	e := New(Config{Backend: be, WAL: w, Prefix: "metrics", MetricBlockRows: 4,
		FlushAdmission: func(context.Context, int64) (func(), error) { return func() {}, nil },
	})
	a, b, c, fresh := stagedTestSeries("a"), stagedTestSeries("b"), stagedTestSeries("c"), stagedTestSeries("fresh")
	appendPoint := func(s signal.Series, ts int64, weight float64) {
		res, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{ts}, []float64{float64(ts)}, []float64{weight},
			func(int) signal.Series { return s }, AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, 1, res.Accepted)
	}
	appendPoint(a, 10, 1)
	appendPoint(b, 10, 2)
	appendPoint(c, 10, 1)
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	rule := gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") })
	rule.Err = context.Canceled
	be.Add(rule)
	flushCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		done <- e.Flush(flushCtx)
	}()
	t.Cleanup(func() { release(); <-finished })
	gate.Await(t)
	appendPoint(a, 20, 4)
	appendPoint(fresh, 20, 1)
	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, it.Close()) })
	cancel()
	release()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Nil(t, e.flushing)
	require.EqualValues(t, 5*SampleBytes, e.HeadBytes())
	appendPoint(a, 30, 1)
	snapshot, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	wantSnapshot := map[signal.SeriesID][]reattachPoint{
		a.Hash(): {{10, 1}, {20, 4}}, b.Hash(): {{10, 2}}, c.Hash(): {{10, 1}}, fresh.Hash(): {{20, 1}},
	}
	assertReattachedBatches(t, snapshot, wantSnapshot)
	be.Reset()
	require.NoError(t, w.Close())
	restored := New(Config{Backend: files, Prefix: "metrics", MetricBlockRows: 4})
	require.NoError(t, restored.LoadParts(ctx))
	require.NoError(t, restored.Replay(ctx, filepath.Join(dir, "wal")))
	wantAll := map[signal.SeriesID][]reattachPoint{
		a.Hash(): {{10, 1}, {20, 4}, {30, 1}}, b.Hash(): {{10, 2}}, c.Hash(): {{10, 1}}, fresh.Hash(): {{20, 1}},
	}
	read := func(store *Engine) []*fetch.Batch {
		it, err := store.Fetch(ctx, fetch.Request{Start: 0, End: 100})
		require.NoError(t, err)
		batches, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		return batches
	}
	assertReattachedBatches(t, read(restored), wantAll)
	require.NoError(t, restored.Flush(ctx))
	reopened := New(Config{Backend: files, Prefix: "metrics", MetricBlockRows: 4})
	require.NoError(t, reopened.LoadParts(ctx))
	assertReattachedBatches(t, read(reopened), wantAll)
}

func assertReattachedBatches(t *testing.T, batches []*fetch.Batch, want map[signal.SeriesID][]reattachPoint) {
	t.Helper()
	require.Len(t, batches, len(want))
	for _, batch := range batches {
		points, ok := want[batch.ID]
		require.True(t, ok)
		require.Len(t, batch.Timestamps, len(points))
		for i, p := range points {
			require.Equal(t, p.ts, batch.Timestamps[i])
			require.InDelta(t, float64(p.ts), batch.Values[i], 1e-12)
			require.InDelta(t, p.weight, batch.ScaleFactor(i), 1e-12)
		}
	}
}
