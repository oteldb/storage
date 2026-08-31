package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
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

func writeTwoTraceStreams(t *testing.T, ctx context.Context, s *Storage) {
	t.Helper()

	_, err := s.WriteTraces(ctx, traceBatch("api",
		spanSpec{traceID: "t1", spanID: "s1", name: "GET /users", start: 100, end: 150},
		spanSpec{traceID: "t1", spanID: "s2", parent: "s1", name: "db.query", start: 110, end: 120},
	))
	require.NoError(t, err)
}

// The record engine refuses before it reads, which is the only point at which refusing is worth
// anything: past it, Fetch materializes the whole result set before returning an iterator.
func TestRecordFetchRefusesBeforeReading(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openLimited(t, 1<<30)

	writeTwoTraceStreams(t, ctx, s)
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Trace))

	// One byte of allowance: the estimate for any flushed part overruns it.
	budget := readbudget.New(1)
	bctx := readbudget.With(ctx, budget)

	_, err := s.TraceFetcher("default").Fetch(bctx, fetch.Request{
		Tenant: "default",
		Signal: signal.Trace,
		Start:  0,
		End:    1_000,
	})
	require.ErrorIs(t, err, readbudget.ErrExceeded,
		"the refusal comes from Fetch itself, not from draining an iterator")
	assert.Equal(t, int64(1), budget.Remaining(), "a refused read reserves nothing")
}

// A generous budget must be invisible, and the reservation must come back when the caller closes —
// otherwise one query permanently shrinks the next.
func TestRecordFetchReleasesOnClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openLimited(t, 1<<30)

	writeTwoTraceStreams(t, ctx, s)
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Trace))

	budget := readbudget.New(1 << 30)
	bctx := readbudget.With(ctx, budget)

	it, err := s.TraceFetcher("default").Fetch(bctx, fetch.Request{
		Tenant: "default",
		Signal: signal.Trace,
		Start:  0,
		End:    1_000,
	})
	require.NoError(t, err)

	got, err := fetch.Drain(bctx, it)
	require.NoError(t, err)
	assert.NotEmpty(t, got, "a query well inside its budget reads normally")

	// Drain closes the iterator, which is what hands the reservation back.
	assert.Equal(t, int64(1<<30), budget.Remaining(), "Close returns the whole reservation")
}

// Closing twice must not credit the budget twice, or a later query overdraws.
func TestRecordFetchDoubleCloseDoesNotDoubleCredit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openLimited(t, 1<<30)

	writeTwoTraceStreams(t, ctx, s)
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Trace))

	budget := readbudget.New(1 << 30)
	bctx := readbudget.With(ctx, budget)

	it, err := s.TraceFetcher("default").Fetch(bctx, fetch.Request{
		Tenant: "default",
		Signal: signal.Trace,
		Start:  0,
		End:    1_000,
	})
	require.NoError(t, err)

	require.NoError(t, it.Close())
	require.NoError(t, it.Close())

	assert.Equal(t, int64(1<<30), budget.Remaining(), "the reservation is returned exactly once")
}

func TestFacadeFetchUnboundedOptOut(t *testing.T) {
	t.Parallel()

	s := openLimited(t, -1)
	assert.Zero(t, s.maxQueryBytes, "a negative cap resolves to no limiter at all")
}

// WithQueryBudget is the seam an embedder installs at its request boundary so several fetches share
// one allowance instead of getting one each.
func TestFacadeWithQueryBudget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openLimited(t, 4096)

	qctx := s.WithQueryBudget(ctx)

	b := readbudget.From(qctx)
	require.NotNil(t, b, "the store hands out a budget at its configured limit")
	assert.Equal(t, int64(4096), b.Remaining())

	assert.Same(t, b, readbudget.From(s.WithQueryBudget(qctx)),
		"an already-budgeted context is not re-budgeted out from under the query")
}

// A caller-declared allowance may tighten this node's limit but never loosen it: the value arrives
// over the network.
func TestSeedBudgetHintOnlyLowers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openLimited(t, 4096)

	f := seedFetcher{inner: nopFetcher{}, obs: s.obs, maxQueryBytes: s.maxQueryBytes, signal: "trace"}

	for _, tc := range []struct {
		name string
		hint int64
		want int64
	}{
		{"lower wins", 1024, 1024},
		{"higher is ignored", 1 << 40, 4096},
		{"absent falls back", 0, 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hctx := readbudget.WithLimitHint(ctx, tc.hint)

			it, err := f.Fetch(hctx, fetch.Request{})
			require.NoError(t, err)

			got := it.(*capturedIterator).budget
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got.Remaining())
		})
	}
}

// nopFetcher captures the budget the seed installed on the context.
type nopFetcher struct{}

func (nopFetcher) Fetch(ctx context.Context, _ fetch.Request) (fetch.Iterator, error) {
	return &capturedIterator{budget: readbudget.From(ctx)}, nil
}

type capturedIterator struct {
	fetch.Iterator

	budget *readbudget.Budget
}
