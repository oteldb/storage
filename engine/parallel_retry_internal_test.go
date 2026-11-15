package engine

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func TestTransientSnapshotGetsOneParallelRetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		invalidations int
		advanceTarget bool
		failWAL       bool
		maxBytes      int64
		want          AppendResult
		materialized  int32
		ordered       int32
	}{
		{name: "refresh and preserve limits", invalidations: 1, maxBytes: 4 * SampleBytes,
			want: AppendResult{Accepted: 4, RejectedBytes: 1020}, materialized: 1},
		{name: "recheck newest watermark", invalidations: 1, advanceTarget: true,
			want: AppendResult{RejectedOOO: 1024}},
		{name: "bounded retry exhaustion", invalidations: 2,
			want: AppendResult{Accepted: 1024}, materialized: 1, ordered: 1},
		{name: "failed WAL after retry", invalidations: 1, failWAL: true, materialized: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				e, _, fs := newDiskLookupEngine(t)
				ids, ts, values := make([]signal.SeriesID, 1024), make([]int64, 1024), make([]float64, 1024)
				for i := range ids {
					ids[i], ts[i], values[i] = benchSeries(0).Hash(), 150, 3
				}
				var captures, ordered, materialized atomic.Int32
				e.parallel.hooks = &parallelHeadHooks{
					captured: func() {
						attempt := captures.Add(1)
						if int(attempt) <= tc.invalidations {
							series := benchSeries(1)
							if tc.advanceTarget {
								series = benchSeries(0)
							}
							_, err := e.Append(series, int64(200+attempt), 2)
							require.NoError(t, err)
							require.NoError(t, e.Flush(t.Context()))
						} else if tc.failWAL {
							failNextWALWrite(fs)
						}
					},
					ordered: func([]signal.SeriesID) { ordered.Add(1) },
				}
				p := preparedFor(ids)
				defer p.memory.release()
				result, err := e.appendPrepared(ids, ts, values, nil, func(int) signal.Series {
					materialized.Add(1)
					return benchSeries(0)
				}, AppendLimits{MaxSeries: 2, MaxInFlightBytes: tc.maxBytes}, p)
				if tc.failWAL {
					require.ErrorIs(t, err, errInjectedWAL)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tc.want, result)
				require.EqualValues(t, 2, captures.Load(), "only one fresh parallel retry is allowed")
				require.Equal(t, tc.ordered, ordered.Load(), "transient first misses must not force ordered ingestion")
				require.Equal(t, tc.materialized, materialized.Load(), "identity materialization is not repeated")
				require.Equal(t, 2, e.SeriesCount(), "historical registration must not consume cardinality twice")
				require.Equal(t, tc.want.Accepted, e.HeadSampleCount())
				replayed := replayedHead(t, fs)
				var replayedSamples int
				for _, buf := range replayed.samples {
					replayedSamples += len(buf.ts)
				}
				require.Equal(t, tc.want.Accepted, replayedSamples, "checkpoint keeps exactly the post-rotation accepted samples")
				require.Zero(t, e.parallel.pending.Load())
				require.Zero(t, e.parallel.pendingSeries.Load())
				require.Nil(t, e.head.disk.pending)
			})
		})
	}
}

func TestTransientPressureRetriesOutsideWriterOwnership(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e, fs := newFaultWALEngine(t, 10)
		be := &pressureGateBackend{Backend: backend.Memory(), entered: make(chan struct{}), resume: make(chan struct{})}
		resume := sync.OnceFunc(func() { close(be.resume) })
		defer resume()
		e.cfg.Backend, e.cfg.Prefix, e.cfg.DiskSeries = be, "retry-pressure", true
		e.cfg.ResidentHeadBytes = 1 << 30
		e.head.disk = newDiskHeadState()
		for i := range 64 {
			_, err := e.Append(benchSeries(i), 100, float64(i))
			require.NoError(t, err)
		}
		e.cfg.ResidentHeadBytes = e.head.residentIdentityBytes() + e.head.inFlightBytes() + SampleBytes
		var captures, ordered, materialized atomic.Int32
		e.parallel.hooks = &parallelHeadHooks{
			captured: func() {
				if captures.Add(1) == 1 {
					_, err := e.Append(benchSeries(1), 110, 2)
					require.NoError(t, err)
				}
			},
			ordered: func([]signal.SeriesID) { ordered.Add(1) },
		}
		id := benchSeries(0).Hash()
		done := make(chan parallelAppendResult, 1)
		go func() {
			p := preparedFor([]signal.SeriesID{id})
			defer p.memory.release()
			r, err := e.appendPrepared([]signal.SeriesID{id}, []int64{200}, []float64{3}, nil,
				func(int) signal.Series { materialized.Add(1); return benchSeries(0) }, AppendLimits{MaxSeries: 64}, p)
			done <- parallelAppendResult{result: r, err: err}
		}()
		<-be.entered
		writerFree := e.mu.TryLockWriter()
		if writerFree {
			e.mu.UnlockWriter()
		}
		q := preparedFor([]signal.SeriesID{id})
		defer q.memory.release()
		q.prepareOwnership()
		stripeDone := make(chan struct{})
		go func() {
			e.parallel.ownership.acquire(q, nil)
			e.parallel.ownership.release(q)
			close(stripeDone)
		}()
		synctest.Wait()
		stripeFree := false
		select {
		case <-stripeDone:
			stripeFree = true
		default:
		}
		resume()
		synctest.Wait()
		r := <-done
		<-stripeDone
		require.NoError(t, r.err)
		require.Equal(t, AppendResult{Accepted: 1}, r.result)
		require.True(t, writerFree, "pressure waiting must not retain lifecycle/writer ownership")
		require.True(t, stripeFree, "pressure waiting must release same-series ownership")
		require.EqualValues(t, 2, captures.Load())
		require.Zero(t, ordered.Load(), "released pressure must retry parallel ingestion")
		require.EqualValues(t, 1, materialized.Load())
		require.Zero(t, e.parallel.pending.Load())
		require.Zero(t, e.parallel.pendingSeries.Load())
		require.Equal(t, []int64{200}, headTS(replayedHead(t, fs), benchSeries(0)))
		require.NoError(t, e.CloseWAL())
	})
}
