package engine

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
)

func TestDecodeBudgetTryAccounting(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		limit, amount int64
	}{
		{"fits", 100, 40}, {"oversized alone", 100, 150}, {"disabled", 0, 150},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := NewDecodeBudget(tc.limit)
			s := fetch.NewScope()
			require.NoError(t, b.tryAcquireFor(t.Context(), s, tc.amount))
			if tc.limit > 0 {
				assert.Equal(t, tc.amount, b.inFlight())
				// Later reads of an already-owned scope retain ordinary growth semantics.
				require.NoError(t, b.tryAcquireFor(t.Context(), s, 200))
				assert.Equal(t, tc.amount+200, b.inFlight())
				b.releaseFor(s, 200)
			}
			b.releaseFor(s, tc.amount)
			assert.Zero(t, b.inFlight())
			assert.Zero(t, b.waiting())
			assert.Zero(t, b.forcedAdmissions())
		})
	}
}

func TestDecodeBudgetTryRespectsFIFO(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := NewDecodeBudget(100)
		_, err := b.acquire(t.Context(), 60)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { _, err := b.acquire(ctx, 80); result <- err }()
		synctest.Wait()
		require.Equal(t, 1, b.waiting())
		// 20 would fit, but must not barge ahead of the queued 80-byte read.
		start := time.Now()
		s := fetch.NewScope()
		require.ErrorIs(t, b.tryAcquireFor(t.Context(), s, 20), fetch.ErrDecodeBusy)
		assert.Zero(t, time.Since(start))
		assert.Equal(t, int64(60), b.inFlight())
		assert.Equal(t, 1, b.waiting())
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		require.NoError(t, b.tryAcquireFor(t.Context(), s, 20), "failed try must unlock its scope")
		b.releaseFor(s, 20)
		b.release(60)
		assert.Zero(t, b.inFlight())
		assert.Zero(t, b.waiting())
		assert.Zero(t, b.forcedAdmissions())
	})
}

func TestDecodeBudgetTryScopeAdmissionInProgress(t *testing.T) {
	t.Parallel()
	b := NewDecodeBudget(100)
	s := fetch.NewScope()
	require.False(t, s.Enter())
	require.ErrorIs(t, b.tryAcquireFor(t.Context(), s, 40), fetch.ErrDecodeBusy)
	assert.Zero(t, b.inFlight())
	s.Abort()
	require.NoError(t, b.tryAcquireFor(t.Context(), s, 40))
	b.releaseFor(s, 40)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, b.tryAcquireFor(ctx, s, 40), context.Canceled)
	assert.Zero(t, b.inFlight())
}

func TestTryFetchBusyAndOrdinaryCancel(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := NewDecodeBudget(1)
		e, req, series := budgetEngine(t, b, Config{})
		t.Cleanup(func() { require.NoError(t, e.Close(t.Context())) })
		holder := req
		holder.Scope = fetch.NewScope()
		it, err := e.Fetch(t.Context(), holder)
		require.NoError(t, err)
		held := b.inFlight()
		require.Positive(t, held)
		candidate := req
		candidate.Scope = fetch.NewScope()
		start := time.Now()
		noit, err := e.TryFetch(t.Context(), candidate)
		require.ErrorIs(t, err, fetch.ErrDecodeBusy)
		require.Nil(t, noit)
		assert.Zero(t, time.Since(start))
		assert.Equal(t, held, b.inFlight())
		assert.Zero(t, b.waiting())
		assert.Zero(t, b.forcedAdmissions())

		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { _, err := e.Fetch(ctx, candidate); result <- err }()
		synctest.Wait()
		require.Equal(t, 1, b.waiting(), "ordinary fetch still waits")
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		assert.Equal(t, held, b.inFlight())
		require.NoError(t, it.Close())
		assert.Zero(t, b.inFlight())

		// A fresh try can now run oversized alone and returns the ordinary samples.
		next, err := e.TryFetch(t.Context(), candidate)
		require.NoError(t, err)
		assert.Equal(t, held, b.inFlight())
		batches, err := fetch.Drain(t.Context(), next)
		require.NoError(t, err)
		require.Len(t, batches, series)
		assert.Zero(t, b.inFlight())
		assert.Zero(t, b.waiting())
		assert.Zero(t, b.forcedAdmissions())
	})
}
