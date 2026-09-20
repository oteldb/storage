package memlimit_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/memlimit"
)

func TestPoolAdmitsUpToTotal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := memlimit.NewPool(300)

	a, err := p.Acquire(ctx, 100)
	require.NoError(t, err)

	b, err := p.Acquire(ctx, 200)
	require.NoError(t, err)

	// The pool is now full; a third caller must wait rather than overcommit.
	tight, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	_, err = p.Acquire(tight, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	a()

	c, err := p.Acquire(ctx, 100)
	require.NoError(t, err, "released bytes are admittable again")

	b()
	c()
}

func TestPoolNilAndZeroAdmitEverything(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for name, p := range map[string]*memlimit.Pool{
		"nil":      nil,
		"zero":     memlimit.NewPool(0),
		"negative": memlimit.NewPool(-1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Zero(t, p.Total())

			for range 4 {
				release, err := p.Acquire(ctx, 1<<40)
				require.NoError(t, err)

				release()
			}
		})
	}
}

// TestPoolOversizedRequestRunsAlone is the anti-deadlock rule: a merge asking for more than the
// whole budget must still run, serialized, rather than wait forever for bytes that cannot exist.
func TestPoolOversizedRequestRunsAlone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := memlimit.NewPool(100)

	big, err := p.Acquire(ctx, 1<<30)
	require.NoError(t, err)

	tight, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	_, err = p.Acquire(tight, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the oversized holder occupies the whole pool")

	big()

	release, err := p.Acquire(ctx, 1)
	require.NoError(t, err)

	release()
}

func TestPoolReleaseIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := memlimit.NewPool(100)

	release, err := p.Acquire(ctx, 100)
	require.NoError(t, err)

	release()
	release()
	release()

	// A release counted three times would have inflated the pool to 300.
	a, err := p.Acquire(ctx, 100)
	require.NoError(t, err)

	tight, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	_, err = p.Acquire(tight, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	a()
}

// TestPoolServesInArrivalOrder pins the fairness rule. It matters because the alternative — letting
// a later small request jump a waiting large one — starves exactly the merge that most needs to run.
func TestPoolServesInArrivalOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := memlimit.NewPool(100)

	held, err := p.Acquire(ctx, 100)
	require.NoError(t, err)

	var (
		order   []int
		mu      sync.Mutex
		wg      sync.WaitGroup
		waiting atomic.Int32
	)

	for i := range 3 {
		wg.Go(func() {
			waiting.Add(1)

			release, err := p.Acquire(ctx, 100)
			require.NoError(t, err)

			mu.Lock()
			order = append(order, i)
			mu.Unlock()

			release()
		})

		// Serialize the arrivals: the queue's order is the thing under test, so the goroutines must
		// not race to join it.
		require.Eventually(t, func() bool { return waiting.Load() == int32(i+1) },
			time.Second, time.Millisecond)
		time.Sleep(5 * time.Millisecond)
	}

	held()
	wg.Wait()

	assert.Equal(t, []int{0, 1, 2}, order)
}

// TestPoolCanceledWaiterReturnsItsGrant covers the race the waiter's granted flag exists for: a
// release can hand bytes to a waiter at the exact moment that waiter's context dies. Go picks
// randomly between two ready select cases, so over these iterations the waiter takes both paths —
// and on the one where it was already granted, the bytes are its to return or the pool leaks them.
func TestPoolCanceledWaiterReturnsItsGrant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	for range 500 {
		p := memlimit.NewPool(100)

		held, err := p.Acquire(ctx, 100)
		require.NoError(t, err)

		racing, cancel := context.WithCancel(ctx)

		var wg sync.WaitGroup

		start := make(chan struct{})

		wg.Go(func() {
			release, err := p.Acquire(racing, 100)
			if err == nil {
				release()
			}
		})

		// Wait until the acquirer is actually queued, so the release and the cancel race the grant
		// itself rather than racing the waiter's arrival.
		require.Eventually(t, func() bool { return p.Waiting() == 1 }, time.Second, 50*time.Microsecond)

		wg.Go(func() {
			<-start
			held()
		})

		wg.Go(func() {
			<-start
			cancel()
		})

		close(start)
		wg.Wait()

		// Whoever won, the pool must be whole again.
		done, err := p.Acquire(ctx, 100)
		require.NoError(t, err, "a canceled waiter must not swallow its grant")

		done()
	}
}

func TestPoolTotalReportsItsSize(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(4096), memlimit.NewPool(4096).Total())
}
