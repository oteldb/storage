package recordengine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/query/fetch"
)

func objectCount(t *testing.T, be backend.Backend, prefix string) int {
	t.Helper()

	keys, err := be.List(context.Background(), prefix)
	require.NoError(t, err)

	return len(keys)
}

// TestResetWaitsForInFlightFlush verifies Reset drains an in-flight flush instead of racing it: the
// flush's publish phase must not re-add its part (and its stale sequence) into the emptied engine,
// and the records it detached from the head must not stay visible through e.flushing.
func TestResetWaitsForInFlightFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	gate := faultbackend.NewGate()
	be.Add(gate.RuleAll(faultbackend.Write, func(op faultbackend.Op) bool { return backendtest.IsPartObject(op.Key) }))
	e := newEngine(t, be)

	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "discarded"}))

	done := make(chan error, 1)
	go func() { done <- e.Flush(ctx) }()

	gate.Await(t) // the flush has detached the head and is writing its part

	reset := make(chan error, 1)
	go func() { reset <- e.Reset(ctx) }()

	gate.Release()
	require.NoError(t, <-done)
	require.NoError(t, <-reset)

	require.Equal(t, 0, e.PartCount(), "the flushed part must not survive the reset")
	require.Empty(t, streamBodies(t, e), "records detached by the drained flush must be discarded too")
	require.Zero(t, objectCount(t, be, "t/recs/"), "reset deletes the engine's objects")
}

// TestResetKeepsPartsUnderRead verifies Reset does not delete the objects of a part a concurrent
// fetch has acquired — the reader would get ErrNotExist mid-scan. They are retired instead and
// deleted by the deferred reclaim once the reader drains.
func TestResetKeepsPartsUnderRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := faultbackend.Wrap(backend.Memory())
	gate := faultbackend.NewGate()
	e := newEngine(t, be)

	// Enough distinct bodies that the body column is a real object the fetch must read (a
	// single-valued column is const-encoded into the manifest and needs no I/O at all).
	recs := make([]rrec, 0, 64)
	want := make([]string, 0, 64)

	for i := range 64 {
		body := fmt.Sprintf("read-me-%d", i)
		recs = append(recs, rrec{ts: int64(i + 1), body: body})
		want = append(want, body)
	}

	ingest(t, e, mkBatch("api", recs...))
	require.NoError(t, e.Flush(ctx))

	dirs := backendtest.PartDirs(ctx, t, be, enginePrefix)
	require.Len(t, dirs, 1)

	// Armed only now, so the flush's own read-back was not held.
	be.Add(gate.RuleAll(faultbackend.Read, func(op faultbackend.Op) bool { return strings.Contains(op.Key, "/c/") }))

	type result struct {
		bodies []string
		err    error
	}

	got := make(chan result, 1)

	go func() {
		it, err := e.Fetch(ctx, req("api"))
		if err != nil {
			got <- result{err: err}

			return
		}

		batches, err := fetch.Drain(ctx, it)
		if err != nil {
			got <- result{err: err}

			return
		}

		var out []string
		for _, b := range batches {
			out = append(out, bodies(b)...)
		}

		got <- result{bodies: out}
	}()

	gate.Await(t) // the fetch holds the part and is reading its columns

	require.NoError(t, e.Reset(ctx))
	require.Positive(t, objectCount(t, be, "t/recs/"+dirs[0]+"/"),
		"a part a fetch is reading must outlive the reset")

	be.Reset()
	gate.Release()

	r := <-got
	require.NoError(t, r.err)
	require.Equal(t, want, r.bodies, "the in-flight fetch must still complete")

	// The reader has drained: the next maintenance cycle reclaims what Reset deferred.
	require.NoError(t, e.Flush(ctx)) // empty head ⇒ a pure reclaim pass
	require.Zero(t, objectCount(t, be, "t/recs/"))
}

// TestConcurrentFlushIsSerialized exercises the single-flusher guard: concurrent Flush calls (which
// the exported API allows, and Close makes reachable) share e.flushBuf, e.bloomBuf and the part
// sequence off the engine lock. Run with -race.
func TestConcurrentFlushIsSerialized(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := newEngine(t, be)

	const flushers = 8

	var wg sync.WaitGroup

	for i := range flushers {
		ingest(t, e, mkBatch("api", rrec{ts: int64(i + 1), body: "row"}))

		wg.Go(func() { require.NoError(t, e.Flush(ctx)) })
	}

	wg.Wait()

	require.Len(t, streamBodies(t, e), flushers, "every ingested record survives concurrent flushes")
	require.Positive(t, e.PartCount())
	require.LessOrEqual(t, e.PartCount(), flushers, "a flush that found an empty head publishes nothing")
}
