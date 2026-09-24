package enginetest

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/faultbackend"
)

func objectCount(t *testing.T, be backend.Backend, prefix string) int {
	t.Helper()

	keys, err := be.List(context.Background(), prefix)
	require.NoError(t, err)

	return len(keys)
}

// resetWaitsForInFlightFlush: Reset drains an in-flight flush instead of racing it. The flush's
// publish phase must not re-add its part (and its stale sequence) into the emptied engine, and the
// rows it detached from the head must not stay visible through the flushing buffers.
func resetWaitsForInFlightFlush(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	gate := faultbackend.NewGate()
	be.Add(gate.RuleAll(faultbackend.Write, func(op faultbackend.Op) bool { return backendtest.IsPartObject(op.Key) }))
	e := k.open(t, be)

	e.Append(t, api(100, 1))

	done := make(chan error, 1)
	go func() { done <- e.Flush(ctx) }()

	gate.Await(t) // the flush has detached the head and is writing its part

	reset := make(chan error, 1)
	go func() { reset <- e.Reset(ctx) }()

	gate.Release()
	require.NoError(t, <-done)
	require.NoError(t, <-reset)

	require.Equal(t, 0, e.PartCount(), "the flushed part must not survive the reset")
	require.Empty(t, rows(t, e, apiStream), "the drained flush's rows go with it")
	require.Zero(t, objectCount(t, be, k.Prefix+"/"), "reset deletes the engine's objects")
}

// resetKeepsPartsUnderRead: Reset does not delete the objects of a part a concurrent fetch has
// acquired, or the reader would get ErrNotExist mid-scan. They are retired instead and deleted by
// the deferred reclaim once the reader drains.
func resetKeepsPartsUnderRead(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	gate := faultbackend.NewGate()
	e := k.open(t, be)

	// Enough distinct values that the value column is a real object the fetch must read (a
	// single-valued column is const-encoded into the manifest and needs no I/O at all).
	want := make([]Row, 0, 64)
	for i := range int64(64) {
		want = append(want, api(i+1, i))
	}

	e.Append(t, want...)
	require.NoError(t, e.Flush(ctx))

	dirs := k.partDirs(ctx, t, be)
	require.Len(t, dirs, 1)

	// Armed only now, so the flush's own read-back was not held.
	be.Add(gate.RuleAll(faultbackend.Read, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))

	type result struct {
		rows []Row
		err  error
	}

	got := make(chan result, 1)

	go func() {
		r, err := e.Read(ctx, apiStream)
		got <- result{rows: r, err: err}
	}()

	gate.Await(t) // the fetch holds the part and is reading its columns

	require.NoError(t, e.Reset(ctx))
	require.Positive(t, objectCount(t, be, k.Prefix+"/"+dirs[0]+"/"), "a part a fetch is reading must outlive the reset")

	be.Reset()
	gate.Release()

	r := <-got
	require.NoError(t, r.err)
	require.Equal(t, want, r.rows, "the in-flight fetch must still complete")

	// The reader has drained: the next maintenance cycle reclaims what Reset deferred.
	require.NoError(t, e.Flush(ctx)) // empty head ⇒ a pure reclaim pass
	require.Zero(t, objectCount(t, be, k.Prefix+"/"))
}

// concurrentFlushIsSerialized exercises the single-flusher guard: concurrent Flush calls (which the
// exported API allows, and Close makes reachable) share the flush buffers and the part sequence off
// the engine lock. Run with -race.
func concurrentFlushIsSerialized(t *testing.T, k Kind) {
	t.Helper()

	ctx := context.Background()
	e := k.open(t, backend.Memory())

	const flushers = 8

	var wg sync.WaitGroup

	for i := range int64(flushers) {
		e.Append(t, api(i+1, i))

		wg.Go(func() { require.NoError(t, e.Flush(ctx)) })
	}

	wg.Wait()

	require.Len(t, rows(t, e, apiStream), flushers, "every appended row survives concurrent flushes")
	require.Positive(t, e.PartCount())
	require.LessOrEqual(t, e.PartCount(), flushers, "a flush that found an empty head publishes nothing")
}
