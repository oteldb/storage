package engine

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func reservationIDs(values ...uint64) []signal.SeriesID {
	ids := make([]signal.SeriesID, len(values))
	for i, value := range values {
		ids[i].Lo = value
	}
	return ids
}

func TestSeriesReservationsDisjointProgress(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var ledger seriesReservations
		first := newSeriesReservation(reservationIDs(2, 1, 2))
		require.NoError(t, ledger.acquire(t.Context(), first))
		require.Equal(t, reservationIDs(1, 2), first.ids)
		waiting := newSeriesReservation(reservationIDs(1))
		result := make(chan error, 1)
		go func() { result <- ledger.acquire(t.Context(), waiting) }()
		synctest.Wait()
		other := newSeriesReservation(reservationIDs(3, 4))
		require.NoError(t, ledger.acquire(t.Context(), other), "disjoint writes do not join the paused owner")
		ledger.release(other)
		select {
		case <-result:
			t.Fatal("conflicting write passed its owner")
		default:
		}
		ledger.release(first)
		require.NoError(t, <-result)
		ledger.release(waiting)
		for i := range ledger.stripes {
			require.Empty(t, ledger.stripes[i].ids)
		}
	})
}

func TestSeriesReservationFIFO(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var ledger seriesReservations
		owner := newSeriesReservation(reservationIDs(1))
		require.NoError(t, ledger.acquire(t.Context(), owner))
		first, second := newSeriesReservation(reservationIDs(1)), newSeriesReservation(reservationIDs(1))
		acquiredFirst, acquiredSecond := make(chan error, 1), make(chan error, 1)
		go func() { acquiredFirst <- ledger.acquire(t.Context(), first) }()
		synctest.Wait()
		go func() { acquiredSecond <- ledger.acquire(t.Context(), second) }()
		synctest.Wait()
		stripe := ledger.stripe(owner.ids[0])
		stripe.mu.Lock()
		queuedFirst, queuedLast := stripe.ids[owner.ids[0]].first, stripe.ids[owner.ids[0]].last
		stripe.mu.Unlock()
		require.Same(t, first, queuedFirst)
		require.Same(t, second, queuedLast)
		ledger.release(owner)
		synctest.Wait()
		require.NoError(t, <-acquiredFirst)
		select {
		case <-acquiredSecond:
			t.Fatal("later waiter overtook the queue head")
		default:
		}
		ledger.release(first)
		require.NoError(t, <-acquiredSecond)
		ledger.release(second)
	})
}

func TestSeriesReservationCancellationDropsPartialOwnership(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var ledger seriesReservations
		owner := newSeriesReservation(reservationIDs(2))
		require.NoError(t, ledger.acquire(t.Context(), owner))
		ctx, cancel := context.WithCancel(t.Context())
		waiting := newSeriesReservation(reservationIDs(2, 1))
		result := make(chan error, 1)
		go func() { result <- ledger.acquire(ctx, waiting) }()
		synctest.Wait()
		stripe := ledger.stripe(signal.SeriesID{Lo: 1})
		stripe.mu.Lock()
		partial := stripe.ids[signal.SeriesID{Lo: 1}].owner
		stripe.mu.Unlock()
		require.Same(t, waiting, partial)
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		require.Zero(t, waiting.held)
		other := newSeriesReservation(reservationIDs(1))
		require.NoError(t, ledger.acquire(t.Context(), other))
		ledger.release(other)
		ledger.release(owner)
		for i := range ledger.stripes {
			require.Empty(t, ledger.stripes[i].ids)
		}
	})
}
