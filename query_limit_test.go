package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// openLimited builds an ephemeral store whose per-query read bound is maxQueryBytes.
func openLimited(t *testing.T, maxQueryBytes int64) *Storage {
	t.Helper()

	ctx := context.Background()
	s, err := Open(ctx, Options{MaxQueryBytes: maxQueryBytes},
		WithBackend(backend.Memory()),
		WithDurability(DurabilityEphemeral),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	return s
}

// The record seam (traces/logs/profiles), which has no decode admission control of its own: a wide
// unselective read is exactly the shape that used to run until the process died.
func TestFacadeTraceFetchRefusesOverBudget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openLimited(t, 1) // one byte: any batch at all overruns it

	_, err := s.WriteTraces(ctx, traceBatch("api",
		spanSpec{traceID: "t1", spanID: "s1", name: "GET /users", start: 100, end: 150},
		spanSpec{traceID: "t1", spanID: "s2", parent: "s1", name: "db.query", start: 110, end: 120},
	))
	require.NoError(t, err)

	it, err := s.TraceFetcher("default").Fetch(ctx, fetch.Request{
		Tenant: "default",
		Signal: signal.Trace,
		Start:  0,
		End:    1_000,
	})
	require.NoError(t, err, "the budget is enforced while reading, not at plan time")

	_, err = fetch.Drain(ctx, it)
	assert.ErrorIs(t, err, fetch.ErrTooLarge)
}

// The metric seam. The decode budget already queues concurrent metric queries, but it admits an
// over-budget query alone rather than refusing it, so this bound is what makes one query fail.
func TestFacadeMetricFetchRefusesOverBudget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openLimited(t, 1)

	_, err := s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1, 2, 3}, []float64{1, 2, 3}))
	require.NoError(t, err)

	it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{
		Tenant: "default",
		Start:  0,
		End:    1_000_000_000,
	})
	require.NoError(t, err)

	_, err = fetch.Drain(ctx, it)
	assert.ErrorIs(t, err, fetch.ErrTooLarge)
}

// A generous budget must be invisible: the bound is a backstop, not a behavior change.
func TestFacadeFetchUnderBudgetIsUnchanged(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openLimited(t, 1<<30)

	_, err := s.WriteTraces(ctx, traceBatch("api",
		spanSpec{traceID: "t1", spanID: "s1", name: "GET /users", start: 100, end: 150},
	))
	require.NoError(t, err)

	it, err := s.TraceFetcher("default").Fetch(ctx, fetch.Request{
		Tenant: "default",
		Signal: signal.Trace,
		Start:  0,
		End:    1_000,
	})
	require.NoError(t, err)

	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	assert.NotEmpty(t, got, "a query well inside its budget reads normally")
}

// Opting out must install no limiter, so an embedder that bounds reads itself pays nothing.
func TestFacadeFetchUnboundedOptOut(t *testing.T) {
	t.Parallel()

	s := openLimited(t, -1)
	assert.Zero(t, s.maxQueryBytes, "a negative cap resolves to no limiter at all")
}
