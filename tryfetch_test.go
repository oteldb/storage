package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func TestFacadeTryFetch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		opts        []Option
		tenants     []signal.TenantID
		unsupported bool
	}{
		{name: "local", tenants: []signal.TenantID{"default"}},
		{name: "split", opts: []Option{WithQuerySplitInterval(100)}, tenants: []signal.TenantID{"default"}, unsupported: true},
		{name: "cached", opts: []Option{WithQueryCache(16)}, tenants: []signal.TenantID{"default"}, unsupported: true},
		{name: "multiple tenants", tenants: []signal.TenantID{"default", "default"}, unsupported: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			s, err := InMemory(tc.opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100, 200}, []float64{1, 2}))
			require.NoError(t, err)
			require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))
			f := s.Fetcher(tc.tenants...)
			trier, ok := f.(fetch.TryFetcher)
			require.True(t, ok)
			req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{nameMatcher("m")}}
			it, err := trier.TryFetch(ctx, req)
			if tc.unsupported {
				require.ErrorIs(t, err, fetch.ErrTryUnsupported)
				require.Nil(t, it)
				it, err = f.Fetch(ctx, req)
			}
			require.NoError(t, err)
			batches, err := fetch.Drain(ctx, it)
			require.NoError(t, err)
			require.Len(t, batches, 1)
			assert.Equal(t, []int64{100, 200}, batches[0].Timestamps)
		})
	}
}

type tryBudgetCapture struct{ nopFetcher }

func (f tryBudgetCapture) TryFetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	return f.Fetch(ctx, r)
}

func TestSeedTryFetchPreservesReadBudget(t *testing.T) {
	t.Parallel()
	s := openLimited(t, 4096)
	f := seedFetcher{inner: tryBudgetCapture{}, obs: s.obs, maxQueryBytes: 4096}
	for _, tc := range []struct {
		name string
		ctx  func(context.Context) context.Context
		want int64
	}{
		{"native limit", func(ctx context.Context) context.Context { return ctx }, 4096},
		{"tighter hint", func(ctx context.Context) context.Context { return readbudget.WithLimitHint(ctx, 1024) }, 1024},
		{"existing query", func(ctx context.Context) context.Context { return readbudget.With(ctx, readbudget.New(2048)) }, 2048},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := seedFetcher{inner: tryBudgetCapture{}, obs: s.obs, maxQueryBytes: 4096}
			it, err := f.TryFetch(tc.ctx(t.Context()), fetch.Request{})
			require.NoError(t, err)
			require.NotNil(t, it.(*capturedIterator).budget)
			assert.Equal(t, tc.want, it.(*capturedIterator).budget.Remaining())
		})
	}
	f.inner = nopFetcher{}
	it, err := f.TryFetch(t.Context(), fetch.Request{})
	require.ErrorIs(t, err, fetch.ErrTryUnsupported)
	require.Nil(t, it)
}
