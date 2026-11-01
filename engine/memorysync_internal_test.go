package engine

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestMemoryWALSyncAllowsWarmFinalizationAndPreparation(t *testing.T) {
	t.Parallel()
	e, fsys := newFaultWALEngine(t, 0)
	first, second := stagedTestSeries("first"), stagedTestSeries("second")
	e.head.registerSeries(first)
	e.head.registerSeries(second)
	enableMemoryPublicationTest(t, e)
	published, prepared := make(chan struct{}), make(chan struct{})
	finish := make(chan struct{})
	finishPublication := sync.OnceFunc(func() { close(finish) })
	t.Cleanup(finishPublication)
	e.warmHooks = &memoryWarmHooks{
		published: func(ids []signal.SeriesID) {
			if ids[0] == first.Hash() {
				close(published)
				<-finish
			}
		},
		reserved: func(ids []signal.SeriesID) {
			if ids[0] == second.Hash() {
				close(prepared)
			}
		},
	}
	firstDone := make(chan error, 1)
	go func() { _, err := e.Append(first, 10, 1); firstDone <- err }()
	<-published
	gate := faultfs.NewGate()
	releaseSync := sync.OnceFunc(gate.Release)
	t.Cleanup(releaseSync)
	fsys.Add(gate.Rule(faultfs.OpSync, func(call faultfs.Call) bool { return strings.HasSuffix(call.Name, ".wal") }))
	synced := make(chan error, 1)
	go func() { synced <- e.SyncWAL() }()
	gate.Await(t)
	secondDone := make(chan error, 1)
	go func() { _, err := e.Append(second, 20, 2); secondDone <- err }()
	<-prepared
	finishPublication()
	require.NoError(t, <-firstDone, "already logged writes finalize while fsync is blocked")
	count, err := e.Count(t.Context(), fetch.Request{Start: 0, End: 30})
	require.NoError(t, err)
	require.Equal(t, 1, count, "the second write still waits for physical WAL ownership")
	releaseSync()
	require.NoError(t, <-synced)
	require.NoError(t, <-secondDone)
	replayed := replayedHead(t, fsys)
	require.Equal(t, []int64{10}, headTS(replayed, first))
	require.Equal(t, []int64{20}, headTS(replayed, second))
}
