package engine_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

// gateWrites blocks the first Write of a part object until released, holding a flush open in its
// off-lock build phase.
type gateWrites struct {
	backend.Backend

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gateWrites) Write(ctx context.Context, key string, data []byte) error {
	if isPartObject(key) {
		g.once.Do(func() { close(g.entered) })
		<-g.release
	}

	return g.Backend.Write(ctx, key, data)
}

// gateReads blocks the first read of a part column once armed, holding a fetch inside its lock-free
// part read (where it has the part acquired).
type gateReads struct {
	backend.Backend

	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gateReads) Read(ctx context.Context, key string) ([]byte, error) {
	if g.armed.Load() && strings.Contains(key, "/c/") {
		g.once.Do(func() { close(g.entered) })
		<-g.release
	}

	return g.Backend.Read(ctx, key)
}

func objectCount(t *testing.T, be backend.Backend, prefix string) int {
	t.Helper()

	keys, err := be.List(context.Background(), prefix)
	require.NoError(t, err)

	return len(keys)
}

// TestResetWaitsForInFlightFlush verifies Reset drains an in-flight flush instead of racing it: the
// flush's publish phase must not re-add its part (and its stale sequence) into the emptied engine,
// and the samples it detached from the head must not stay visible through the flushing buffers.
func TestResetWaitsForInFlightFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &gateWrites{Backend: backend.Memory(), entered: make(chan struct{}), release: make(chan struct{})}
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)

	done := make(chan error, 1)
	go func() { done <- e.Flush(ctx) }()

	<-be.entered // the flush has detached the head and is writing its part

	reset := make(chan error, 1)
	go func() { reset <- e.Reset(ctx) }()

	close(be.release)
	require.NoError(t, <-done)
	require.NoError(t, <-reset)

	require.Equal(t, 0, e.PartCount(), "the flushed part must not survive the reset")
	require.Empty(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1000,
		Matchers: []fetch.Matcher{eqMatcher("job", "api")}}), "the drained flush's samples go with it")
	require.Zero(t, objectCount(t, be, "default/metrics/"), "reset deletes the engine's objects")
}

// TestResetKeepsPartsUnderRead verifies Reset does not delete the objects of a part a concurrent
// fetch has acquired — the reader would get ErrNotExist mid-scan. They are retired instead and
// deleted by the deferred reclaim once the reader drains.
func TestResetKeepsPartsUnderRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := &gateReads{Backend: backend.Memory(), entered: make(chan struct{}), release: make(chan struct{})}
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	// Enough distinct values that the columns are real objects the fetch must read (a single-valued
	// column is const-encoded into the manifest and needs no I/O at all).
	s := mkSeries("job", "api")

	want := make([]int64, 0, 64)
	for i := range 64 {
		mustAppend(t, e, s, int64(i+1), float64(i))
		want = append(want, int64(i+1))
	}

	require.NoError(t, e.Flush(ctx))

	dirs := partDirs(t, be, "default/metrics")
	require.Len(t, dirs, 1)

	be.armed.Store(true)

	type result struct {
		ts  []int64
		err error
	}

	got := make(chan result, 1)

	go func() {
		it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
		if err != nil {
			got <- result{err: err}

			return
		}

		batches, err := fetch.Drain(ctx, it)
		if err != nil {
			got <- result{err: err}

			return
		}

		var ts []int64
		for _, b := range batches {
			ts = append(ts, b.Timestamps...)
		}

		got <- result{ts: ts}
	}()

	<-be.entered // the fetch holds the part and is reading its columns

	require.NoError(t, e.Reset(ctx))
	require.Positive(t, objectCount(t, be, "default/metrics/"+dirs[0]+"/"),
		"a part a fetch is reading must outlive the reset")

	be.armed.Store(false)
	close(be.release)

	r := <-got
	require.NoError(t, r.err)
	require.Equal(t, want, r.ts, "the in-flight fetch must still complete")

	// The reader has drained: the next maintenance cycle reclaims what Reset deferred.
	require.NoError(t, e.Flush(ctx)) // empty head ⇒ a pure reclaim pass
	require.Zero(t, objectCount(t, be, "default/metrics/"))
}

// TestConcurrentFlushIsSerialized exercises the single-flusher guard: concurrent Flush calls (which
// the exported API allows, and Close makes reachable) mutate the parts slice and the part sequence
// off the engine lock. Run with -race.
func TestConcurrentFlushIsSerialized(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := engine.New(engine.Config{Backend: be, Prefix: "default/metrics"})

	const flushers = 8

	s := mkSeries("job", "api")

	var wg sync.WaitGroup

	for i := range flushers {
		mustAppend(t, e, s, int64(i+1), float64(i))

		wg.Go(func() { require.NoError(t, e.Flush(ctx)) })
	}

	wg.Wait()

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	require.Len(t, got[0].Timestamps, flushers, "every appended sample survives concurrent flushes")
	require.Positive(t, e.PartCount())
	require.LessOrEqual(t, e.PartCount(), flushers, "a flush that found an empty head publishes nothing")
}
