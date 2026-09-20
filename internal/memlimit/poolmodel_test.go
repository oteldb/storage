package memlimit_test

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/memlimit"
)

// The pool's scenario tests each pin one arrangement its author thought of. This one pins the
// invariants against every arrangement a fuzzer reaches, which is the difference that matters: of
// the defects review found in this package, the one that mattered — a canceled head waiter leaving
// the waiter behind it asleep — was a liveness failure with *correct* accounting. An oracle that
// only checked bytes would have passed it. So the oracle checks three things:
//
//	accounting — free + held == total, and free never goes negative
//	membership — the same set of callers is granted and the same number queued as in the model
//	liveness  — once every goroutine is durably blocked, no queued request fits in free
//
// The last is the one with teeth. It is only checkable because the whole fuzz body runs inside a
// [synctest] bubble: synctest.Wait returns when every goroutine in the bubble is durably blocked,
// so "the pool has settled" is an observable state rather than a sleep.
//
// What it cannot reach: anything about who *calls* the pool. The per-merge share, the cap
// arithmetic and the maintenance loop's fairness are not expressible here.

// modelPool is the obvious implementation: a counter and a FIFO queue, no concurrency.
type modelPool struct {
	total int64
	free  int64
	queue []int64 // sizes, in arrival order
}

func (m *modelPool) clamp(n int64) int64 { return min(max(n, 1), m.total) }

func (m *modelPool) tryAcquire(n int64) bool {
	n = m.clamp(n)
	if len(m.queue) > 0 || m.free < n {
		return false
	}

	m.free -= n

	return true
}

func (m *modelPool) release(n int64) {
	m.free += m.clamp(n)
	m.drain()
}

// cancelAt removes the i-th queued request, then drains: a request that leaves without moving bytes
// can still unblock the one behind it.
func (m *modelPool) cancelAt(i int) {
	m.queue = append(m.queue[:i:i], m.queue[i+1:]...)
	m.drain()
}

// drain serves the queue head while it fits, returning how many it served.
func (m *modelPool) drain() int {
	n := 0

	for len(m.queue) > 0 && m.free >= m.queue[0] {
		m.free -= m.queue[0]
		m.queue = m.queue[1:]
		n++
	}

	return n
}

// handle is one outstanding Acquire in the real pool.
type handle struct {
	n       int64
	cancel  context.CancelFunc
	done    chan struct{} // closed when the Acquire call returns
	release func()        // non-nil once granted
	err     error
}

func TestPoolMatchesTheModel(t *testing.T) {
	t.Parallel()

	// A handful of hand-picked sequences; the fuzzer explores from here.
	for name, ops := range map[string][]byte{
		"fill and drain":    {opAcquire, 40, opAcquire, 60, opRelease, 0, opRelease, 1},
		"queue then cancel": {opAcquire, 100, opAcquire, 100, opCancel, 1, opRelease, 0},
		// The missed wakeup: 40 is granted (60 free), 100 queues because it does not fit, 50 queues
		// behind it. Canceling the *head* moves no bytes, so nothing but abandon itself can notice
		// that 50 now fits. Index 0 of the outstanding Acquires is the 100 — the 40 was granted and
		// is no longer outstanding.
		"cancel the head":    {opAcquire, 40, opAcquire, 100, opAcquire, 50, opCancel, 0},
		"try against queue":  {opAcquire, 100, opAcquire, 100, opTry, 1, opRelease, 0},
		"oversize runs solo": {opAcquire, 255, opTry, 1, opRelease, 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) { runPoolOps(t, ops) })
		})
	}
}

func FuzzPoolMatchesTheModel(f *testing.F) {
	f.Add([]byte{opAcquire, 40, opAcquire, 60, opRelease, 0})
	f.Add([]byte{opAcquire, 100, opAcquire, 100, opCancel, 1, opRelease, 0})
	f.Add([]byte{opAcquire, 40, opAcquire, 100, opAcquire, 50, opCancel, 1, opRelease, 0})
	f.Add([]byte{opTry, 30, opTry, 30, opTry, 30, opTry, 30, opRelease, 0})

	f.Fuzz(func(t *testing.T, ops []byte) {
		synctest.Test(t, func(t *testing.T) { runPoolOps(t, ops) })
	})
}

const (
	opAcquire = iota
	opTry
	opRelease
	opCancel
	opCount
)

// poolTotal is small so a byte-sized request straddles it: requests above it exercise the clamp.
const poolTotal = 100

// runPoolOps drives the real pool and the model through ops in lockstep, checking the invariants
// after each one. It must run inside a [synctest] bubble.
func runPoolOps(t *testing.T, ops []byte) {
	t.Helper()

	// Keep the sequences short enough that a failure is readable.
	const maxOps = 64

	pool := memlimit.NewPool(poolTotal)
	model := &modelPool{total: poolTotal, free: poolTotal}

	var (
		live []*handle // outstanding Acquires, in arrival order
		held []*handle // granted and not yet released
	)

	defer func() {
		// Unblock anything still queued so the bubble can end.
		for _, h := range live {
			h.cancel()
		}
	}()

	for i := 0; i+1 < len(ops) && i < maxOps*2; i += 2 {
		op, arg := int(ops[i])%opCount, int64(ops[i+1])

		switch op {
		case opAcquire:
			h := startAcquire(t, pool, arg)
			live = append(live, h)
			model.queue = append(model.queue, model.clamp(arg))
			model.drain()

		case opTry:
			release, ok := pool.TryAcquire(arg)
			require.Equal(t, model.tryAcquire(arg), ok, "TryAcquire(%d) disagreed with the model", arg)

			if ok {
				held = append(held, &handle{n: model.clamp(arg), release: release})
			}

		case opRelease:
			if len(held) == 0 {
				continue
			}

			h := held[int(arg)%len(held)]
			held = removeHandle(held, h)
			h.release()
			model.release(h.n)

		case opCancel:
			if len(live) == 0 {
				continue
			}

			h := live[int(arg)%len(live)]
			h.cancel()
		}

		synctest.Wait() // every goroutine is durably blocked; the pool has settled

		live, held = reap(t, live, held, model)
		checkInvariants(t, pool, model, held)
	}
}

// startAcquire launches a blocking Acquire and returns its handle.
func startAcquire(t *testing.T, p *memlimit.Pool, n int64) *handle {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	h := &handle{n: n, cancel: cancel, done: make(chan struct{})}

	go func() {
		defer close(h.done)

		h.release, h.err = p.Acquire(ctx, n)
	}()

	return h
}

// reap moves every Acquire that has returned out of live: granted ones into held, canceled ones
// out of the model's queue.
func reap(t *testing.T, live, held []*handle, model *modelPool) ([]*handle, []*handle) {
	t.Helper()

	remaining := live[:0:0]

	for idx, h := range live {
		select {
		case <-h.done:
		default:
			remaining = append(remaining, h)

			continue
		}

		if h.err != nil {
			// A canceled waiter: if the model still queues it, drop it and let the queue drain.
			if i := indexOfSize(model.queue, model.clamp(h.n), idx); i >= 0 {
				model.cancelAt(i)
			}

			continue
		}

		held = append(held, &handle{n: model.clamp(h.n), release: h.release})
	}

	return remaining, held
}

// indexOfSize finds a queued request of size n, preferring the one at hint.
func indexOfSize(queue []int64, n int64, hint int) int {
	if hint < len(queue) && queue[hint] == n {
		return hint
	}

	for i, q := range queue {
		if q == n {
			return i
		}
	}

	return -1
}

func removeHandle(hs []*handle, want *handle) []*handle {
	for i, h := range hs {
		if h == want {
			return append(hs[:i:i], hs[i+1:]...)
		}
	}

	return hs
}

func checkInvariants(t *testing.T, pool *memlimit.Pool, model *modelPool, held []*handle) {
	t.Helper()

	var sum int64
	for _, h := range held {
		sum += h.n
	}

	// I1/I2: the bytes out on loan are exactly the ones the model says are gone, and never more
	// than the budget. A queued request holds nothing, so the sum covers every byte not free.
	require.LessOrEqual(t, sum, pool.Total(), "concurrent holders exceeded the budget")
	require.Equal(t, pool.Total()-model.free, sum, "free bytes disagree with what is held")

	// Membership: the same number of callers are queued.
	require.Equal(t, len(model.queue), pool.Waiting(), "queue depth disagreed with the model")

	// I3 liveness, the one with teeth: everything has settled, so nothing that fits may still wait.
	if len(model.queue) > 0 {
		require.Less(t, model.free, model.queue[0],
			"a queued request fits in the free bytes and was not granted")
	}
}
