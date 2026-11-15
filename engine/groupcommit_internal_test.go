package engine

import (
	"strconv"
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
	for _, syncWrites := range []bool{true, false} {
		for _, failure := range []string{"none", "write", "sync"} {
			if !syncWrites && failure == "sync" {
				continue
			}
			for _, cold := range []bool{false, true} {
				name := "sync=" + strconv.FormatBool(syncWrites) + "/" + failure + "/cold=" + strconv.FormatBool(cold)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					testPreparedGroupCommit(t, failure, cold, syncWrites)
				})
			}
		}
	}
}

func testPreparedGroupCommit(t *testing.T, failure string, cold, syncWrites bool) {
	t.Helper()
	fail := failure != "none"
	e, fs := newFaultWALEngine(t, 0)
	e.cfg.WAL.SetSync(syncWrites)
	t.Cleanup(func() { _ = e.CloseWAL() })
	identities := []signal.Series{stagedTestSeries("a"), stagedTestSeries("b"), stagedTestSeries("c")}
	for i := range identities {
		s := identities[i]
		if i > 0 {
			if !cold {
				e.head.registerSeries(s)
			}
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
		if cold && i > 0 {
			s := identities[i]
			done <- appendColdTest(e, s, 1, AppendLimits{}, func() signal.Series { return s })
			return
		}
		done <- appendPreparedTest(e, []signal.SeriesID{identities[i].Hash()}, []int64{1}, nil, AppendLimits{})
	}
	go appendSeries(0)
	<-ready
	if e.parallel.commits.changed == nil {
		resume()
		r := <-done
		require.NoError(t, r.err)
		t.Fatal("the first ready caller must enter bounded group commit before any follower arrives")
	}
	go appendSeries(1)
	go appendSeries(2)
	for range 3 {
		<-queued
	}
	writerFree := e.mu.TryLockWriter()
	if writerFree {
		e.mu.UnlockWriter()
	}
	require.False(t, writerFree, "queued callers retain lifecycle leases")
	select {
	case premature := <-done:
		done <- premature
		t.Error("a queued batch was acknowledged before its WAL group completed")
	default:
	}
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
	require.Zero(t, e.parallel.pendingSeries.Load())
	require.Equal(t, []int64{0, 1}, headTS(e.head, identities[0]))
	if fail {
		require.Equal(t, 2, failures)
		for i := range identities[1:] {
			s := identities[i+1]
			require.Nil(t, headTS(e.head, s))
			if cold {
				require.False(t, e.head.series.Has(s.Hash()), "failed group must not publish cold identities")
			}
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
		wantSyncs := 0
		if syncWrites {
			wantSyncs = 2
		}
		require.Equal(t, wantSyncs, syncs)
	}
	if !syncWrites {
		// Short-write repair syncs its replacement independently of the write policy.
		if !fail {
			for _, call := range fs.Calls()[beforeCalls:] {
				require.NotEqual(t, faultfs.OpSync, call.Op, "SyncNone must not add implicit synchronization")
			}
		}
		require.NoError(t, e.SyncWAL())
	}
	recovered := fs.Crash()
	replayed := replayedHead(t, recovered)
	for i := range identities {
		s := identities[i]
		require.Equal(t, headTS(e.head, s), headTS(replayed, s))
	}
	e.parallel.hooks = nil
	if fail {
		for i := range identities[1:] {
			s := identities[i+1]
			_, err := e.Append(s, 1, 1)
			require.NoError(t, err)
		}
	}
	require.NoError(t, e.SyncWAL())
	require.NoError(t, e.CloseWAL())
}

func TestPreparedNoSyncGroupPreservesSyncPolicy(t *testing.T) {
	t.Parallel()
	e, fs := newFaultWALEngine(t, 0)
	e.cfg.WAL.SetSync(false)
	t.Cleanup(func() { _ = e.CloseWAL() })
	s := stagedTestSeries("a")
	_, err := e.Append(s, 0, 0)
	require.NoError(t, err)
	beforeCalls := len(fs.Calls())
	var queued atomic.Int32
	e.parallel.hooks = &parallelHeadHooks{queued: func() { queued.Add(1) }}
	r := appendPreparedTest(e, []signal.SeriesID{s.Hash()}, []int64{1}, nil, AppendLimits{})
	require.NoError(t, r.err)
	require.Equal(t, 1, r.result.Accepted)
	require.EqualValues(t, 1, queued.Load(), "a lone caller must use the queue without waiting for peers")
	require.NotNil(t, e.parallel.commits.changed)
	for _, call := range fs.Calls()[beforeCalls:] {
		require.NotEqual(t, faultfs.OpSync, call.Op)
	}
	require.NoError(t, e.SyncWAL())
	require.Equal(t, []int64{0, 1}, headTS(replayedHead(t, fs.Crash()), s))
	require.Zero(t, e.parallel.pending.Load())
	require.Zero(t, e.parallel.pendingSeries.Load())
}

func TestPreparedGroupDrainsBeforeClose(t *testing.T) {
	t.Parallel()
	for _, syncWrites := range []bool{true, false} {
		t.Run(strconv.FormatBool(syncWrites), func(t *testing.T) {
			t.Parallel()
			testPreparedGroupDrainsBeforeClose(t, syncWrites)
		})
	}
}

func testPreparedGroupDrainsBeforeClose(t *testing.T, syncWrites bool) {
	t.Helper()
	e, fs := newFaultWALEngine(t, 0)
	e.cfg.WAL.SetSync(syncWrites)
	t.Cleanup(func() { _ = e.CloseWAL() })
	identities := []signal.Series{stagedTestSeries("a"), stagedTestSeries("b"), stagedTestSeries("c")}
	for i := range identities {
		s := identities[i]
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
	if e.parallel.commits.changed == nil {
		resume()
		r := <-done
		require.NoError(t, r.err)
		t.Fatal("the first ready caller must enter bounded group commit before any follower arrives")
	}
	go appendSeries(1)
	go appendSeries(2)
	for range 3 {
		<-queued
	}
	closed := make(chan error, 1)
	go func() { closed <- e.CloseWAL() }()
	writerFree := e.mu.TryLockWriter()
	if writerFree {
		e.mu.UnlockWriter()
	}
	require.False(t, writerFree)
	resume()
	for range 3 {
		r := <-done
		require.NoError(t, r.err)
		require.Equal(t, 1, r.result.Accepted)
	}
	require.NoError(t, <-closed)
	replayed := replayedHead(t, fs.Crash())
	for i := range identities {
		s := identities[i]
		require.Equal(t, []int64{0, 1}, headTS(replayed, s))
	}
}
