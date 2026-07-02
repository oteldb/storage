package engine

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestDecodeBudgetBlocksAndReleases checks the core admission semantics: an acquire that would
// exceed the budget while something is in flight blocks until a release frees room.
func TestDecodeBudgetBlocksAndReleases(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)
	b.acquire(60) // used = 60

	done := make(chan struct{})
	go func() {
		b.acquire(60) // 60+60 > 100 with used>0 ⇒ blocks
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("second acquire should block while the budget is full")
	case <-time.After(50 * time.Millisecond):
	}

	b.release(60) // used = 0 ⇒ waiter admitted

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("acquire should proceed after release")
	}

	b.release(60)
}

// TestDecodeBudgetOverBudgetAdmittedAlone checks that a request larger than the whole budget runs
// when nothing else is in flight, rather than deadlocking on an unsatisfiable reservation.
func TestDecodeBudgetOverBudgetAdmittedAlone(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)

	done := make(chan struct{})
	go func() {
		b.acquire(500) // 500 > 100 but used == 0 ⇒ admitted
		b.release(500)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("over-budget request should be admitted alone")
	}
}

// TestDecodeBudgetDisabled checks the nil and zero-cap budgets are no-ops (no blocking, no panic).
func TestDecodeBudgetDisabled(t *testing.T) {
	t.Parallel()

	var nilBudget *DecodeBudget

	nilBudget.acquire(1 << 40)
	nilBudget.release(1 << 40)

	zero := NewDecodeBudget(0)
	zero.acquire(1 << 40)
	zero.release(1 << 40)

	assert.Zero(t, zero.used)
}

// TestDecodeBudgetConcurrentBounded runs many acquire/release pairs concurrently and checks the
// in-flight total never exceeds the cap once any reservation is committed (the bound the engine
// relies on), and that everyone makes progress.
func TestDecodeBudgetConcurrentBounded(t *testing.T) {
	t.Parallel()

	const limit, workers, each = 100, 16, 20

	b := NewDecodeBudget(limit)

	var over atomic.Bool

	done := make(chan struct{}, workers)
	for range workers {
		go func() {
			for range each {
				b.acquire(40) // 40 ≤ limit; at most two fit at once
				b.mu.Lock()
				if b.used > limit {
					over.Store(true)
				}
				b.mu.Unlock()
				b.release(40)
			}

			done <- struct{}{}
		}()
	}

	for range workers {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("workers did not finish — possible deadlock")
		}
	}

	assert.False(t, over.Load(), "in-flight reservations exceeded the cap")
}
