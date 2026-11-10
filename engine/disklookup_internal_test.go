package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
)

func newDiskLookupEngine(t *testing.T) (*Engine, *faultbackend.Backend, *faultfs.FS) {
	t.Helper()
	e, fsys := newFaultWALEngine(t, 10)
	be := faultbackend.Wrap(backend.Memory())
	cfg := e.cfg
	cfg.Backend, cfg.Prefix, cfg.DiskSeries, cfg.MetricSeriesChunks = be, "lookup", true, true
	e = New(cfg)
	t.Cleanup(func() { _ = e.CloseWAL() })
	for i := range 2 {
		_, err := e.Append(benchSeries(i), 100, 1)
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Zero(t, e.head.series.Len())
	_, _, err := e.parts[0].diskSeriesMetadata(t.Context(), benchSeries(0).Hash())
	require.NoError(t, err)
	e.head.disk.cache.clear()
	return e, be, fsys
}

func awaitDiskAppend(t *testing.T, done <-chan parallelAppendResult) parallelAppendResult {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-time.After(30 * time.Second):
		t.Fatal("append did not finish")
		return parallelAppendResult{}
	}
}

func TestDiskLookupOutsideWriter(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{
		"unrelated append", "same series", "flush", "reset", "reset with stale error", "reload",
		"fenced", "lookup failure", "failed WAL", "close WAL",
	} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			e, be, fsys := newDiskLookupEngine(t)
			original := e.parts[0]
			ids, ts, values := make([]signal.SeriesID, 1024), make([]int64, 1024), make([]float64, 1024)
			for i := range ids {
				ids[i], ts[i] = benchSeries(0).Hash(), 90
				if operation == "failed WAL" || operation == "close WAL" {
					ts[i] = 1000
				}
			}
			gate := faultbackend.NewGate()
			release := sync.OnceFunc(gate.Release)
			rule := gate.Rule(faultbackend.Read, nil)
			if operation == "reset with stale error" || operation == "lookup failure" {
				rule.Err = backend.ErrNotExist
			}
			be.Add(rule)
			done := make(chan parallelAppendResult, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				p := preparedFor(ids)
				defer p.memory.release()
				r, err := e.appendPrepared(ids, ts, values, nil, func(int) signal.Series { return benchSeries(0) }, AppendLimits{MaxSeries: 2}, p)
				done <- parallelAppendResult{r, err}
			}()
			t.Cleanup(func() { release(); <-finished })
			gate.Await(t)
			require.EqualValues(t, 1, original.refs.Load())
			require.True(t, e.mu.TryLockWriter(), "historical I/O must not retain writer ownership")
			e.mu.UnlockWriter()
			other := make(chan parallelAppendResult, 1)
			go func() {
				var err error
				switch operation {
				case "unrelated append":
					_, err = e.Append(benchSeries(2), 200, 2)
				case "same series", "flush":
					_, err = e.Append(benchSeries(0), 200, 2)
					if err == nil && operation == "flush" {
						err = e.Flush(t.Context())
					}
				case "reset", "reset with stale error":
					err = e.Reset(t.Context())
				case "reload":
					err = e.LoadParts(t.Context())
				case "fenced":
					e.mu.LockWriter()
					e.head.disk.complete = false
					e.mu.UnlockWriter()
				case "failed WAL":
					failNextWALWrite(fsys)
				case "close WAL":
					err = e.CloseWAL()
				case "lookup failure":
				}
				other <- parallelAppendResult{err: err}
			}()
			r := awaitDiskAppend(t, other)
			require.NoError(t, r.err)
			release()
			r = awaitDiskAppend(t, done)
			require.Zero(t, original.refs.Load())
			switch operation {
			case "same series", "flush":
				require.NoError(t, r.err)
				require.Equal(t, AppendResult{RejectedOOO: 1024}, r.result)
				require.EqualValues(t, 2, e.Stats().Series)
			case "fenced":
				require.ErrorIs(t, r.err, bucketindex.ErrFenced)
				require.Zero(t, e.Stats().HeadSamples)
			case "lookup failure":
				require.ErrorIs(t, r.err, backend.ErrNotExist)
				require.Zero(t, e.Stats().HeadSamples)
			case "failed WAL", "close WAL":
				require.Error(t, r.err)
				require.Zero(t, e.Stats().HeadSamples)
				entry, hit := e.head.disk.cache.get(ids[0])
				require.True(t, hit)
				require.EqualValues(t, 100, entry.newest)
			default:
				require.NoError(t, r.err)
				require.Equal(t, 1024, r.result.Accepted)
				count := int64(2)
				switch operation {
				case "unrelated append":
					count = 3
				case "reset", "reset with stale error":
					count = 1
				}
				require.Equal(t, count, e.Stats().Series)
			}
			require.Nil(t, e.head.disk.pending)
		})
	}
}

func TestDiskLookupRechecksCapacity(t *testing.T) {
	t.Parallel()
	e, be, _ := newDiskLookupEngine(t)
	ids, ts, values := make([]signal.SeriesID, 1024), make([]int64, 1024), make([]float64, 1024)
	for i := range ids {
		ids[i], ts[i] = benchSeries(i%2*3).Hash(), 101
	}
	gate := faultbackend.NewGate()
	release := sync.OnceFunc(gate.Release)
	be.Add(gate.Rule(faultbackend.Read, nil))
	done, finished := make(chan parallelAppendResult, 1), make(chan struct{})
	go func() {
		defer close(finished)
		p := preparedFor(ids)
		defer p.memory.release()
		r, err := e.appendPrepared(ids, ts, values, nil, func(i int) signal.Series { return benchSeries(i % 2 * 3) },
			AppendLimits{MaxSeries: 3, MaxInFlightBytes: 4 * SampleBytes}, p)
		done <- parallelAppendResult{r, err}
	}()
	t.Cleanup(func() { release(); <-finished })
	gate.Await(t)
	_, err := e.Append(benchSeries(2), 200, 2)
	require.NoError(t, err)
	release()
	r := awaitDiskAppend(t, done)
	require.NoError(t, r.err)
	require.Equal(t, AppendResult{Accepted: 3, RejectedCardinality: 2, RejectedBytes: 1019}, r.result)
	require.EqualValues(t, 3, e.Stats().Series)
	require.EqualValues(t, 4, e.Stats().HeadSamples)
}
