package engine

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
)

func TestPreparedGroupCommit(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"none", "write", "sync"} {
		fail := failure != "none"
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			e, fs := newFaultWALEngine(t, 0)
			e.cfg.WAL.SetSync(true)
			identities := []signal.Series{stagedTestSeries("a"), stagedTestSeries("b"), stagedTestSeries("c")}
			for i, s := range identities {
				if i > 0 {
					e.head.registerSeries(s)
					continue
				}
				_, err := e.Append(s, 0, 0)
				require.NoError(t, err)
			}
			beforeCalls := len(fs.Calls())
			ready, release, queued := make(chan struct{}), make(chan struct{}), make(chan struct{}, 3)
			resume := sync.OnceFunc(func() { close(release) })
			t.Cleanup(resume)
			var groups atomic.Int32
			e.parallel.hooks = &parallelHeadHooks{
				queued: func() { queued <- struct{}{} },
				beforeWAL: func() {
					switch groups.Add(1) {
					case 1:
						close(ready)
						<-release
					case 2:
						if fail {
							op := faultfs.OpWrite
							if failure == "sync" {
								op = faultfs.OpSync
							}
							fs.Add(faultfs.Rule{
								Op:    op,
								Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") },
								Err:   errInjectedWAL, Short: 3, Times: 1,
							})
						}
					}
				},
			}
			done := make(chan parallelAppendResult, 3)
			appendSeries := func(i int) {
				done <- appendPreparedTest(e, []signal.SeriesID{identities[i].Hash()}, []int64{1}, nil, AppendLimits{})
			}
			go appendSeries(0)
			<-ready
			go appendSeries(1)
			go appendSeries(2)
			for range 3 {
				<-queued
			}
			require.False(t, e.mu.TryLockWriter(), "queued callers retain lifecycle leases")
			resume()
			failures := 0
			for range 3 {
				r := <-done
				if r.err != nil {
					require.ErrorIs(t, r.err, errInjectedWAL)
					require.Equal(t, AppendResult{}, r.result)
					failures++
				} else {
					require.Equal(t, 1, r.result.Accepted)
				}
			}
			require.EqualValues(t, 2, groups.Load(), "followers share one WAL append")
			require.Zero(t, e.parallel.pending.Load())
			require.Equal(t, []int64{0, 1}, headTS(e.head, identities[0]))
			if fail {
				require.Equal(t, 2, failures)
				for _, s := range identities[1:] {
					require.Nil(t, headTS(e.head, s))
				}
			} else {
				require.Zero(t, failures)
			}
			if !fail {
				writes, syncs := 0, 0
				for _, call := range fs.Calls()[beforeCalls:] {
					if !strings.HasSuffix(call.Name, ".wal") {
						continue
					}
					if call.Op == faultfs.OpWrite {
						writes++
					}
					if call.Op == faultfs.OpSync {
						syncs++
					}
				}
				require.Equal(t, 2, writes)
				require.Equal(t, 2, syncs)
			}
			recovered := fs.Crash()
			replayed := replayedHead(t, recovered)
			for _, s := range identities {
				require.Equal(t, headTS(e.head, s), headTS(replayed, s))
			}
			e.parallel.hooks = nil
			if fail {
				for _, s := range identities[1:] {
					_, err := e.Append(s, 1, 1)
					require.NoError(t, err)
				}
			}
			require.NoError(t, e.SyncWAL())
			require.NoError(t, e.CloseWAL())
		})
	}
}

func TestPreparedNoSyncBypassesGroup(t *testing.T) {
	t.Parallel()
	e, _ := newFaultWALEngine(t, 0)
	s := stagedTestSeries("a")
	_, err := e.Append(s, 0, 0)
	require.NoError(t, err)
	e.parallel.hooks = &parallelHeadHooks{queued: func() { t.Error("page-cache writes entered fsync grouping") }}
	r := appendPreparedTest(e, []signal.SeriesID{s.Hash()}, []int64{1}, nil, AppendLimits{})
	require.NoError(t, r.err)
	require.Equal(t, 1, r.result.Accepted)
	require.Equal(t, []int64{0, 1}, headTS(e.head, s))
	require.Nil(t, e.parallel.commits.changed)
}

func TestPreparedGroupDrainsBeforeClose(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 0)
	e.cfg.WAL.SetSync(true)
	identities := []signal.Series{stagedTestSeries("a"), stagedTestSeries("b"), stagedTestSeries("c")}
	for _, s := range identities {
		_, err := e.Append(s, 0, 0)
		require.NoError(t, err)
	}
	ready, release, queued := make(chan struct{}), make(chan struct{}), make(chan struct{}, 3)
	resume := sync.OnceFunc(func() { close(release) })
	t.Cleanup(resume)
	var calls atomic.Int32
	e.parallel.hooks = &parallelHeadHooks{
		queued: func() { queued <- struct{}{} },
		beforeWAL: func() {
			if calls.Add(1) == 1 {
				close(ready)
				<-release
			}
		},
	}
	done := make(chan parallelAppendResult, 3)
	appendSeries := func(i int) {
		done <- appendPreparedTest(e, []signal.SeriesID{identities[i].Hash()}, []int64{1}, nil, AppendLimits{})
	}
	go appendSeries(0)
	<-ready
	go appendSeries(1)
	go appendSeries(2)
	for range 3 {
		<-queued
	}
	closed := make(chan error, 1)
	go func() { closed <- e.CloseWAL() }()
	require.False(t, e.mu.TryLockWriter())
	resume()
	for range 3 {
		r := <-done
		require.NoError(t, r.err)
		require.Equal(t, 1, r.result.Accepted)
	}
	require.NoError(t, <-closed)
	replayed := replayedHead(t, fs.Crash())
	for _, s := range identities {
		require.Equal(t, []int64{0, 1}, headTS(replayed, s))
	}
}
