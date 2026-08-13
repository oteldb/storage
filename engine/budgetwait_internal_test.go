package engine

import (
	"bytes"
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// TestDecodeBudgetAcquireCanceled checks the wait is cancellable and holds nothing on the way out:
// the reservation the caller never got must not be charged, and the queue must not keep the corpse.
func TestDecodeBudgetAcquireCanceled(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)

	_, err := b.acquire(context.Background(), 80)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	forced, err := b.acquire(ctx, 40)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, forced)
	assert.Equal(t, int64(80), b.inFlight(), "a canceled acquire must charge nothing")
	assert.Zero(t, b.waiting())

	b.release(80)

	_, err = b.acquire(context.Background(), 40)
	require.NoError(t, err, "the budget must still admit after a canceled wait")
	assert.Equal(t, int64(40), b.inFlight())
}

// TestDecodeBudgetCancelUnblocksWaiter is the shape of the reported deadlock at the budget level: a
// waiter parked behind a reservation only it could release. Canceling must free the goroutine.
func TestDecodeBudgetCancelUnblocksWaiter(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)

	_, err := b.acquire(context.Background(), 80)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)

	go func() {
		_, err := b.acquire(ctx, 40)
		errc <- err
	}()

	require.Eventually(t, func() bool { return b.waiting() == 1 }, 5*time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-errc:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("a canceled acquire must return")
	}

	assert.Equal(t, int64(80), b.inFlight())
	assert.Zero(t, b.waiting())
}

// TestDecodeBudgetAcquireForCanceledReleasesScope pins the scope-lock exit path: [fetch.Scope.Enter]
// holds the scope across the admission decision, so a failed acquire must Abort it — otherwise the
// query's next read deadlocks on its own scope instead of on the budget.
func TestDecodeBudgetAcquireForCanceledReleasesScope(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)
	scope := fetch.NewScope()

	_, err := b.acquire(context.Background(), 80)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = b.acquireFor(ctx, scope, 40)
	require.ErrorIs(t, err, context.Canceled)

	b.release(80)

	require.True(t, done(t, 5*time.Second, func() {
		_, _ = b.acquireFor(context.Background(), scope, 40)
	}), "the scope must be usable after a canceled acquire under it")
	assert.Equal(t, int64(40), b.inFlight())
}

// TestDecodeBudgetForcedAdmission checks the bounded wait: a waiter that sits for the force interval
// while the budget does not drain at all is admitted over the ceiling, counted, and accounted
// exactly (so the overshoot is returned like any other reservation).
func TestDecodeBudgetForcedAdmission(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)
	b.forceAfter = 5 * time.Millisecond

	_, err := b.acquire(context.Background(), 80)
	require.NoError(t, err)

	forced, err := b.acquire(context.Background(), 40)
	require.NoError(t, err)
	assert.True(t, forced, "a stalled wait must be force-admitted, not blocked forever")
	assert.Equal(t, int64(120), b.inFlight(), "the forced reservation is charged like any other")
	assert.Equal(t, int64(1), b.forcedAdmissions())
	assert.Zero(t, b.waiting())

	b.release(80)
	b.release(40)
	assert.Zero(t, b.inFlight())
}

// TestDecodeBudgetNoLeakUnderCancellation is the accounting invariant behind the fix: whichever exit
// a waiter takes — admitted, force-admitted, or canceled — the budget must end at zero with an
// empty queue. A leaked reservation would reintroduce the wedge slowly.
func TestDecodeBudgetNoLeakUnderCancellation(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(100)
	b.forceAfter = time.Millisecond

	var wg sync.WaitGroup

	for range 16 {
		wg.Go(func() {
			for range 20 {
				ctx, cancel := context.WithCancel(context.Background())
				if rand.IntN(2) == 0 {
					cancel()
				}

				if _, err := b.acquire(ctx, 60); err == nil {
					b.release(60)
				}

				cancel()
			}
		})
	}

	wg.Wait()

	assert.Zero(t, b.inFlight(), "every acquire path must return exactly what it reserved")
	assert.Zero(t, b.waiting())
}

// budgetEngine returns an engine with one flushed part (so a fetch has a non-zero decode estimate)
// reserving from b, plus the request matching every series in it.
func budgetEngine(t *testing.T, b *DecodeBudget, cfg Config) (*Engine, fetch.Request, int) {
	t.Helper()

	const series, samples = 8, 4

	cfg.Backend = backend.Memory()
	cfg.Prefix = "t/budgetwait"
	cfg.DecodeBudget = b

	e := New(cfg)

	for s := range series {
		ser := signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("m"))},
			signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte{byte('a' + s)})},
		)}
		for k := range samples {
			ok, err := e.Append(ser, int64(100+s*1000+k*10), float64(s*10+k))
			require.NoError(t, err)
			require.True(t, ok)
		}
	}

	require.NoError(t, e.Flush(t.Context()))

	req := fetch.Request{Start: 0, End: 1 << 40, Matchers: []fetch.Matcher{{
		Name:  []byte("__name__"),
		Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), []byte("m")) },
	}}}

	return e, req, series
}

// TestFetchDecodeBudgetCancelUnwedges reproduces the reported deadlock end to end — several
// concurrent unscoped fetches queued behind a reservation none of them can release — and pins that
// it is now recoverable: canceling returns an error and charges nothing, and the engine serves
// later queries normally once the holder closes.
func TestFetchDecodeBudgetCancelUnwedges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	// A 1-byte ceiling: the first fetch is admitted alone, every later one must queue.
	b := NewDecodeBudget(1)
	e, req, series := budgetEngine(t, b, Config{})

	held, err := e.Fetch(ctx, req) // Scope: nil — the footgun in the issue.
	require.NoError(t, err)

	inFlight := b.inFlight()
	require.Positive(t, inFlight, "the open fetch must hold a reservation")

	const waiters = 4

	wctx, cancel := context.WithCancel(ctx)
	errs := make(chan error, waiters)

	for range waiters {
		go func() {
			_, err := e.Fetch(wctx, req)
			errs <- err
		}()
	}

	require.Eventually(t, func() bool { return b.waiting() == waiters }, 10*time.Second, time.Millisecond,
		"the concurrent fetches must be parked on the budget (the original wedge)")
	cancel()

	for range waiters {
		select {
		case err := <-errs:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("a canceled fetch must return")
		}
	}

	assert.Equal(t, inFlight, b.inFlight(), "canceled fetches must leave no reservation behind")
	assert.Zero(t, b.waiting())

	require.NoError(t, held.Close())
	assert.Zero(t, b.inFlight())

	it, err := e.Fetch(ctx, req)
	require.NoError(t, err, "the engine must still serve queries after the wedge")

	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	assert.Len(t, batches, series)
}

// TestFetchContextScopeSkipsQueue checks the context-carried scope: a caller that cannot thread a
// Scope through every Request installs one on the request context, and its later reads are charged
// without queueing — the same fix as Request.Scope, without the forced-admission stall.
func TestFetchContextScopeSkipsQueue(t *testing.T) {
	t.Parallel()

	b := NewDecodeBudget(1)
	e, req, _ := budgetEngine(t, b, Config{})
	ctx := fetch.WithScope(context.Background(), fetch.NewScope())

	held, err := e.Fetch(ctx, req)
	require.NoError(t, err)

	defer func() { require.NoError(t, held.Close()) }()

	require.True(t, done(t, 5*time.Second, func() {
		it, err := e.Fetch(ctx, req)
		assert.NoError(t, err)

		if it != nil {
			assert.NoError(t, it.Close())
		}
	}), "a second read under the context scope must not queue behind the first")

	assert.Zero(t, b.forcedAdmissions(), "the scope must admit it outright, not via the escape")
}

// TestFetchDecodeBudgetForcedAdmission checks the loud escape end to end: an unscoped fetch that
// would otherwise hold-and-wait against its own reservation is admitted past the force interval and
// counted on the operator-facing instrument.
func TestFetchDecodeBudgetForcedAdmission(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	reader := sdkmetric.NewManualReader()

	o, err := obs.New(obs.Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	require.NoError(t, err)

	b := NewDecodeBudget(1)
	b.forceAfter = 5 * time.Millisecond
	e, req, series := budgetEngine(t, b, Config{Obs: o})

	held, err := e.Fetch(ctx, req)
	require.NoError(t, err)

	defer func() { require.NoError(t, held.Close()) }()

	// The second fetch of the same (unscoped) caller can never be admitted by a release — the only
	// reservation is its own caller's. It must be force-admitted rather than block forever.
	it, err := e.Fetch(ctx, req)
	require.NoError(t, err)

	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	assert.Len(t, batches, series)

	assert.Equal(t, int64(1), b.forcedAdmissions())

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	assert.Equal(t, int64(1), forcedAdmissionCount(t, rm))
}

// forcedAdmissionCount sums the forced-admission counter across data points.
func forcedAdmissionCount(t *testing.T, rm metricdata.ResourceMetrics) int64 {
	t.Helper()

	const name = "storage.fetch.decode_budget_forced_admissions"

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}

			sum, ok := m.Data.(metricdata.Sum[int64])
			require.Truef(t, ok, "%s is not an int64 sum", name)

			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}

			return total
		}
	}

	t.Fatalf("counter %q not found", name)

	return 0
}
