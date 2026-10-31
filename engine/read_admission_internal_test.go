package engine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"
)

func TestReadersDuringStagedAdmission(t *testing.T) {
	t.Parallel()
	for _, prepared := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			name := "ordinary"
			if prepared {
				name = "prepared"
			}
			if fail {
				name += "/failed WAL"
			} else {
				name += "/success"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				e, fsys := newFaultWALEngine(t, 50)
				e.cfg.Backend, e.cfg.Prefix = backend.Memory(), "metrics"
				known, fresh := stagedTestSeries("known"), stagedTestSeries("fresh")
				ok, err := e.Append(known, 100, 1)
				require.NoError(t, err)
				require.True(t, ok)
				entered, release := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				t.Cleanup(unblock)
				materialize := func(int) signal.Series {
					close(entered)
					<-release
					return fresh
				}
				ids := []signal.SeriesID{known.Hash(), fresh.Hash()}
				type result struct {
					res AppendResult
					err error
				}
				done := make(chan result, 1)
				go func() {
					var out result
					if prepared {
						pool := newBatchPreparations()
						p := pool.acquire()
						p.prepare(ids)
						out.res, out.err = e.appendPrepared(ids, []int64{1000, 1000}, []float64{2, 2}, nil,
							materialize, AppendLimits{}, p)
						pool.release(p)
					} else {
						out.res, out.err = e.AppendBatch(ids, []int64{1000, 1000}, []float64{2, 2}, nil,
							materialize, AppendLimits{})
					}
					done <- out
				}()
				wait := sync.OnceValue(func() result { return <-done })
				t.Cleanup(func() { unblock(); _ = wait() })
				<-entered
				assertReadableDuringWAL(t, e, 1)
				assert.EqualValues(t, 100, e.Stats().MaxTime)
				if fail {
					failNextWALWrite(fsys)
				}
				unblock()
				out := wait()
				if fail {
					require.ErrorIs(t, out.err, errInjectedWAL)
					assert.Equal(t, AppendResult{}, out.res)
					assert.EqualValues(t, 1, e.Stats().Series)
					accepted, err := e.Append(known, 90, 3)
					require.NoError(t, err)
					require.True(t, accepted, "failed staging cannot advance the committed watermark")
					_, err = e.AppendBatch(ids, []int64{1000, 1000}, []float64{2, 2}, nil,
						func(int) signal.Series { return fresh }, AppendLimits{})
					require.NoError(t, err)
				} else {
					require.NoError(t, out.err)
					assert.Equal(t, 2, out.res.Accepted)
				}
				assert.Equal(t, snapshotHead(e.head), snapshotHead(replayedHead(t, fsys)))
			})
		}
	}
}

func TestStatsActiveSampleAccounting(t *testing.T) {
	t.Parallel()
	e := New(Config{})
	s := stagedTestSeries("samples")
	for i := range 3 {
		accepted, err := e.Append(s, int64(i), float64(i))
		require.NoError(t, err)
		require.True(t, accepted)
	}
	assert.EqualValues(t, 3, e.Stats().HeadSamples)
	e.mu.LockWriter()
	detached, bytes, since := e.head.detach()
	e.mu.UnlockWriter()
	assert.Zero(t, e.Stats().HeadSamples, "frozen bytes are not active samples")
	assert.EqualValues(t, 3*SampleBytes, e.Stats().HeadBytes)
	_, err := e.Append(s, 4, 4)
	require.NoError(t, err)
	assert.EqualValues(t, 1, e.Stats().HeadSamples)
	e.mu.LockWriter()
	e.head.reattach(detached, bytes, since)
	e.mu.UnlockWriter()
	assert.EqualValues(t, 4, e.Stats().HeadSamples)
	require.NoError(t, e.Reset(t.Context()))
	assert.Zero(t, e.Stats().HeadSamples)
}
