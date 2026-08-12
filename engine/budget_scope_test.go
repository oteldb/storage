package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// done reports whether f finished within the timeout. It is how these tests distinguish "blocked"
// from "slow" without sleeping for the happy path.
func done(t *testing.T, timeout time.Duration, f func()) bool {
	t.Helper()

	c := make(chan struct{})

	go func() {
		defer close(c)

		f()
	}()

	select {
	case <-c:
		return true
	case <-time.After(timeout):
		return false
	}
}

// TestQueryScopeSecondFetchDoesNotBlock is the regression guard for the deadlock a streaming
// consumer hits: with several fetches open at once, the second acquire is hold-and-wait against
// the query's own reservation, and acquire's admit-alone escape cannot fire because `used` is
// non-zero precisely because this query holds it.
func TestQueryScopeSecondFetchDoesNotBlock(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)
	ctx := WithQueryScope(context.Background())

	first := b.acquireFor(ctx, 80)
	require.NotNil(t, first)

	// 80 + 40 > 100, so an unscoped acquire would queue here forever: nothing can release while
	// this same query is the only holder.
	require.True(t, done(t, 5*time.Second, func() { b.acquireFor(ctx, 40) }),
		"a second fetch in the same query must not block on the first")

	assert.Equal(t, int64(120), b.inFlight(), "accounting stays exact even when queueing is skipped")
}

// TestQueryScopeConcurrentFirstAdmission covers the race the scope's own mutex exists for: two
// fetches opening at once must not both take the blocking path.
func TestQueryScopeConcurrentFirstAdmission(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)
	ctx := WithQueryScope(context.Background())

	require.True(t, done(t, 5*time.Second, func() {
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() { b.acquireFor(ctx, 60) })
		}
		wg.Wait()
	}), "concurrent fetches of one query must not deadlock against each other")

	assert.Equal(t, int64(480), b.inFlight())
}

// TestQueryScopeStillBoundsOtherQueries pins that the exception is scoped: a *different* query
// still queues, which is the whole purpose of the budget.
func TestQueryScopeStillBoundsOtherQueries(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)

	held := b.acquireFor(WithQueryScope(context.Background()), 80)
	require.NotNil(t, held)

	other := WithQueryScope(context.Background())
	assert.False(t, done(t, 200*time.Millisecond, func() { b.acquireFor(other, 40) }),
		"an unrelated query must still wait for the ceiling")

	// Releasing the first admits the waiter, so the test leaves nothing blocked.
	b.releaseFor(held, 80)
	assert.True(t, done(t, 5*time.Second, func() {
		for b.inFlight() < 40 {
			time.Sleep(time.Millisecond)
		}
	}), "the waiter must be admitted once the holder releases")
}

// TestQueryScopeReleaseRestoresBudget checks the scope's hold is fully returned, so a long-lived
// consumer does not leak reservation and starve later queries.
func TestQueryScopeReleaseRestoresBudget(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)
	ctx := WithQueryScope(context.Background())

	a := b.acquireFor(ctx, 60)
	c := b.acquireFor(ctx, 60)

	b.releaseFor(a, 60)
	b.releaseFor(c, 60)

	assert.Zero(t, b.inFlight())

	// With the scope drained, a fresh fetch under it takes the blocking path again rather than
	// inheriting a stale "already admitted".
	require.True(t, done(t, 5*time.Second, func() { b.acquireFor(ctx, 90) }))
	assert.Equal(t, int64(90), b.inFlight())
}

// TestQueryScopeUnscopedUnchanged pins that a caller who never opens a scope sees exactly the old
// behavior, including the admit-alone escape for an over-budget request.
func TestQueryScopeUnscopedUnchanged(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)

	require.True(t, done(t, 5*time.Second, func() { b.acquireFor(context.Background(), 500) }),
		"an over-budget fetch is still admitted alone")
	assert.Equal(t, int64(500), b.inFlight())
}

func TestWithQueryScopeIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := WithQueryScope(context.Background())
	assert.Same(t, queryScopeFrom(ctx), queryScopeFrom(WithQueryScope(ctx)),
		"nesting must not split one query's reservation in two")
}
