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

func TestReattachCloseAfterCompletedRun(t *testing.T) {
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
		FlushMemoryBytes: 1024,
		FlushAdmission:   func(context.Context, int64) (func(), error) { return func() {}, nil },
	})
	a, b, c, d, fresh := stagedTestSeries("a"), stagedTestSeries("b"), stagedTestSeries("c"),
		stagedTestSeries("d"), stagedTestSeries("fresh")
	appendPoint := func(s signal.Series, ts int64, weight float64) {
		res, err := e.AppendBatch([]signal.SeriesID{s.Hash()}, []int64{ts}, []float64{float64(ts)}, []float64{weight},
			func(int) signal.Series { return s }, AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, 1, res.Accepted)
	}
	appendPoint(a, 10, 1)
	appendPoint(b, 10, 2)
	appendPoint(c, 10, 1)
	appendPoint(d, 10, 3)

	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	var completedPrefix string
	rule := gate.Rule(faultbackend.Write, func(op faultbackend.Op) bool {
		prefix, _, ok := strings.Cut(op.Key, "/c/")
		if !ok {
			return false
		}
		if completedPrefix == "" {
			completedPrefix = prefix
			return false
		}
		return prefix != completedPrefix
	})
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
	// A 1 KiB target admits one identity per run; each frozen identity has one sample.
	// Reaching the second part therefore means the first bounded run finished.
	completed, err := openPart(ctx, files, completedPrefix, e.cfg.Obs.Corruption, e.readCompressors)
	require.NoError(t, err)
	completed.acquire()
	t.Cleanup(completed.release)
	decoded, err := decodePart(ctx, completed)
	require.NoError(t, err)
	require.Equal(t, []int64{10}, decoded.ts)

	appendPoint(a, 20, 4)
	appendPoint(fresh, 20, 1)
	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, it.Close()) })
	cancel()
	release()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Nil(t, e.flushing)
	require.EqualValues(t, 6*SampleBytes, e.HeadBytes())
	require.Empty(t, e.parts, "completed runs remain unpublished when a later run fails")
	appendPoint(a, 30, 1)
	be.Reset()
	require.NoError(t, e.Close(ctx))
	require.Zero(t, e.HeadBytes())
	require.GreaterOrEqual(t, len(e.parts), 4, "Close must finish multiple bounded runs")

	snapshot, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	assertReattachedBatches(t, snapshot, map[signal.SeriesID][]reattachPoint{
		a.Hash(): {{10, 1}, {20, 4}}, b.Hash(): {{10, 2}}, c.Hash(): {{10, 1}},
		d.Hash(): {{10, 3}}, fresh.Hash(): {{20, 1}},
	})
	wantAll := map[signal.SeriesID][]reattachPoint{
		a.Hash(): {{10, 1}, {20, 4}, {30, 1}}, b.Hash(): {{10, 2}}, c.Hash(): {{10, 1}},
		d.Hash(): {{10, 3}}, fresh.Hash(): {{20, 1}},
	}
	reopened := New(Config{Backend: files, Prefix: "metrics", MetricBlockRows: 4})
	t.Cleanup(func() { require.NoError(t, reopened.Close(context.Background())) })
	require.NoError(t, reopened.LoadParts(ctx))
	read := func() []*fetch.Batch {
		it, err := reopened.Fetch(ctx, fetch.Request{Start: 0, End: 100})
		require.NoError(t, err)
		batches, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		return batches
	}
	assertReattachedBatches(t, read(), wantAll)
	require.NoError(t, reopened.Replay(ctx, filepath.Join(dir, "wal")))
	require.Zero(t, reopened.HeadBytes(), "Close must publish the reattached samples before checkpointing")
	assertReattachedBatches(t, read(), wantAll)
}
