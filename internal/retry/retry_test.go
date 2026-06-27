package retry_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/retry"
)

var errTransient = errors.New("transient")

func TestDoSucceedsFirstTry(t *testing.T) {
	t.Parallel()

	var calls int32
	got, err := retry.Do(context.Background(), retry.Policy{MaxAttempts: 3}, func(context.Context) (int, error) {
		atomic.AddInt32(&calls, 1)

		return 42, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, got)
	assert.Equal(t, int32(1), calls, "no retry on success")
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	var calls int32
	got, err := retry.Do(context.Background(), retry.Policy{MaxAttempts: 5},
		func(context.Context) (string, error) {
			if atomic.AddInt32(&calls, 1) < 3 {
				return "", errTransient
			}

			return "ok", nil
		})

	require.NoError(t, err)
	assert.Equal(t, "ok", got)
	assert.Equal(t, int32(3), calls)
}

func TestDoStopsOnNonRetryable(t *testing.T) {
	t.Parallel()

	permanent := errors.New("permanent")

	var calls int32
	_, err := retry.Do(context.Background(), retry.Policy{
		MaxAttempts: 5,
		Retryable:   func(e error) bool { return errors.Is(e, errTransient) },
	}, func(context.Context) (int, error) {
		atomic.AddInt32(&calls, 1)

		return 0, permanent
	})

	require.ErrorIs(t, err, permanent)
	assert.Equal(t, int32(1), calls, "a non-retryable error is not retried")
}

func TestDoExhaustsAndReturnsLastErr(t *testing.T) {
	t.Parallel()

	var calls int32
	_, err := retry.Do(context.Background(), retry.Policy{MaxAttempts: 3},
		func(context.Context) (int, error) {
			atomic.AddInt32(&calls, 1)

			return 0, errTransient
		})

	require.ErrorIs(t, err, errTransient)
	assert.Equal(t, int32(3), calls)
}

func TestDoBackoffHonorsContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	var calls int32
	_, err := retry.Do(ctx, retry.Policy{MaxAttempts: 5, BaseBackoff: time.Hour},
		func(context.Context) (int, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				cancel() // cancel during the (1h) backoff before attempt 2
			}

			return 0, errTransient
		})

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(1), calls, "backoff wait aborts on cancel, no second attempt")
}

func TestDoPerTryTimeout(t *testing.T) {
	t.Parallel()

	var deadlines int32
	_, err := retry.Do(context.Background(), retry.Policy{MaxAttempts: 2, PerTryTimeout: 20 * time.Millisecond},
		func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				atomic.AddInt32(&deadlines, 1)

				return 0, ctx.Err()
			case <-time.After(time.Second):
				return 0, nil
			}
		})

	require.Error(t, err)
	assert.Equal(t, int32(2), deadlines, "each attempt saw its own per-try deadline")
}

// TestHedgeFastPathNoHedge: when the first attempt returns before HedgeDelay, no hedge launches.
func TestHedgeFastPathNoHedge(t *testing.T) {
	t.Parallel()

	var launched int32
	thunks := []func(context.Context) (int, error){
		func(context.Context) (int, error) { atomic.AddInt32(&launched, 1); return 1, nil },
		func(context.Context) (int, error) { atomic.AddInt32(&launched, 1); return 2, nil },
	}

	got, err := retry.Hedge(context.Background(), retry.Policy{HedgeDelay: 50 * time.Millisecond}, thunks)
	require.NoError(t, err)
	assert.Equal(t, 1, got)
	assert.Equal(t, int32(1), launched, "the fast first attempt means no hedge")
}

// TestHedgeSlowFirstWinsWithSecond: a slow first attempt is bypassed by the hedge, and the fast
// second wins — the tail-latency guarantee.
func TestHedgeSlowFirstWinsWithSecond(t *testing.T) {
	t.Parallel()

	var second int32
	thunks := []func(context.Context) (int, error){
		func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(2 * time.Second):
				return 1, nil
			}
		},
		func(context.Context) (int, error) { atomic.AddInt32(&second, 1); return 2, nil },
	}

	start := time.Now()
	got, err := retry.Hedge(context.Background(), retry.Policy{HedgeDelay: 30 * time.Millisecond}, thunks)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, 2, got, "the hedged second attempt won")
	assert.Equal(t, int32(1), second)
	assert.Less(t, elapsed, time.Second, "did not wait for the slow first attempt")
}

// TestHedgeFailoverIsImmediate: a fast failure promotes the next attempt without waiting for the
// hedge delay.
func TestHedgeFailoverIsImmediate(t *testing.T) {
	t.Parallel()

	thunks := []func(context.Context) (int, error){
		func(context.Context) (int, error) { return 0, errTransient },
		func(context.Context) (int, error) { return 7, nil },
	}

	start := time.Now()
	got, err := retry.Hedge(context.Background(), retry.Policy{HedgeDelay: time.Hour}, thunks)

	require.NoError(t, err)
	assert.Equal(t, 7, got)
	assert.Less(t, time.Since(start), time.Second, "failover did not wait for the (1h) hedge delay")
}

func TestHedgeNonRetryableShortCircuits(t *testing.T) {
	t.Parallel()

	permanent := errors.New("permanent")

	var launched int32
	thunks := []func(context.Context) (int, error){
		func(context.Context) (int, error) { atomic.AddInt32(&launched, 1); return 0, permanent },
		func(context.Context) (int, error) { atomic.AddInt32(&launched, 1); return 9, nil },
	}

	_, err := retry.Hedge(context.Background(), retry.Policy{
		HedgeDelay: time.Hour,
		Retryable:  func(e error) bool { return errors.Is(e, errTransient) },
	}, thunks)

	require.ErrorIs(t, err, permanent)
	assert.Equal(t, int32(1), launched, "a permanent error stops the hedge")
}

func TestHedgeAllFail(t *testing.T) {
	t.Parallel()

	thunks := retry.Repeat(func(context.Context) (int, error) { return 0, errTransient }, 3)

	_, err := retry.Hedge(context.Background(), retry.Policy{HedgeDelay: time.Millisecond}, thunks)
	require.ErrorIs(t, err, errTransient)
}

func TestHedgeRespectsContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	thunks := retry.Repeat(func(ctx context.Context) (int, error) {
		<-ctx.Done()

		return 0, ctx.Err()
	}, 2)

	_, err := retry.Hedge(ctx, retry.Policy{HedgeDelay: 10 * time.Millisecond}, thunks)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestHedgeEmpty(t *testing.T) {
	t.Parallel()

	_, err := retry.Hedge[int](context.Background(), retry.Policy{}, nil)
	require.Error(t, err)
}

// TestHedgeNoLeak ensures losing attempts are unblocked (ctx canceled) and do not block on send.
func TestHedgeNoLeak(t *testing.T) {
	t.Parallel()

	released := make(chan struct{})
	thunks := []func(context.Context) (int, error){
		func(context.Context) (int, error) { return 1, nil }, // wins immediately
		func(ctx context.Context) (int, error) {
			<-ctx.Done() // must be canceled when the first wins
			close(released)

			return 0, ctx.Err()
		},
	}

	// HedgeDelay 0 ⇒ pure failover; but the first succeeds, so the second never launches here.
	// Force both to launch by making the first slow-ish and delay tiny.
	thunks[0] = func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(40 * time.Millisecond):
			return 1, nil
		}
	}

	got, err := retry.Hedge(context.Background(), retry.Policy{HedgeDelay: 5 * time.Millisecond}, thunks)
	require.NoError(t, err)
	assert.Equal(t, 1, got)

	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("losing hedged attempt was not canceled")
	}
}
