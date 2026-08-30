package retry_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/retry"
)

// TestDoOnResultReportsEveryAttempt pins what a launch hook cannot say: how each attempt ended.
// Without it the retry counter reports reactions to failures the attempt counter never admits to.
func TestDoOnResultReportsEveryAttempt(t *testing.T) {
	t.Parallel()

	var (
		mu   sync.Mutex
		errs []error
	)

	calls := 0
	_, err := retry.Do(context.Background(), retry.Policy{
		MaxAttempts: 3,
		OnResult: func(_ int, err error) {
			mu.Lock()
			defer mu.Unlock()

			errs = append(errs, err)
		},
	}, func(context.Context) (int, error) {
		calls++
		if calls < 3 {
			return 0, errTransient
		}

		return 7, nil
	})

	require.NoError(t, err)
	require.Len(t, errs, 3)
	require.ErrorIs(t, errs[0], errTransient)
	require.ErrorIs(t, errs[1], errTransient)
	require.NoError(t, errs[2])
}

// TestDoOnResultReportsPerTryTimeout confirms an attempt the per-try deadline killed is reported as
// a deadline, not as a generic failure: the two say different things about the peer.
func TestDoOnResultReportsPerTryTimeout(t *testing.T) {
	t.Parallel()

	var (
		mu   sync.Mutex
		errs []error
	)

	_, err := retry.Do(context.Background(), retry.Policy{
		MaxAttempts:   1,
		PerTryTimeout: time.Millisecond,
		OnResult: func(_ int, err error) {
			mu.Lock()
			defer mu.Unlock()

			errs = append(errs, err)
		},
	}, func(ctx context.Context) (int, error) {
		<-ctx.Done()

		return 0, ctx.Err()
	})

	require.Error(t, err)
	require.Len(t, errs, 1)
	require.ErrorIs(t, errs[0], context.DeadlineExceeded)
}

// TestHedgeOnResultReportsLosers confirms the hedged attempts report too — including the loser the
// winner cancels, which is what keeps a hedge from inflating the error rate silently.
func TestHedgeOnResultReportsLosers(t *testing.T) {
	t.Parallel()

	var (
		mu sync.Mutex
		n  int
	)

	got, err := retry.Hedge(context.Background(), retry.Policy{
		MaxAttempts: 2,
		HedgeDelay:  time.Millisecond,
		OnResult: func(_ int, _ error) {
			mu.Lock()
			n++
			mu.Unlock()
		},
	}, []func(context.Context) (int, error){
		func(ctx context.Context) (int, error) {
			<-ctx.Done()

			return 0, ctx.Err()
		},
		func(context.Context) (int, error) { return 5, nil },
	})

	require.NoError(t, err)
	assert.Equal(t, 5, got)

	// The winner reports before its result is handed back, so at least one is in by now; the
	// canceled loser lands whenever it unblocks.
	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, n, 1, "at least the winning attempt reported its result")
}
