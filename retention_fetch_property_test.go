package storage

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestRetentionFetchMatchesAgeOracle(t *testing.T) {
	t.Parallel()
	var age atomic.Int64
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Retention: tenant.Retention{MaxAge: time.Duration(age.Load())}}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	s.now = func() int64 { return 1000 }
	times := make([]int64, 100)
	values := make([]float64, len(times))
	for i := range times {
		times[i] = int64(i * 10)
		values[i] = float64(i)
	}
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "retention", times, values))
	require.NoError(t, err)
	f := s.Fetcher("default")
	rng := rand.New(rand.NewPCG(669, 667))
	for range 128 {
		window := time.Duration(rng.IntN(1200))
		age.Store(int64(window))
		start := int64(rng.IntN(1300) - 100)
		end := start + int64(rng.IntN(800))
		var want []int64
		for _, ts := range times {
			if ts >= start && ts <= end && (window == 0 || ts >= 1000-int64(window)) {
				want = append(want, ts)
			}
		}
		got := retentionTimestamps(t, f, signal.Metric, start, end)
		require.Equal(t, want, got, "age=%d range=[%d,%d]", window, start, end)
		n, err := fetch.CounterOf(f).Count(t.Context(), fetch.Request{Start: start, End: end})
		require.NoError(t, err)
		expected := 0
		if len(want) > 0 {
			expected = 1
		}
		require.Equal(t, expected, n)
	}
}

func TestRetentionFetchCacheUsesOnePolicySnapshot(t *testing.T) {
	t.Parallel()
	var querying atomic.Bool
	var calls atomic.Int64
	s, err := InMemory(WithQueryCache(16), WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		if querying.Load() && calls.Add(1) > 1 {
			return tenant.Policy{Retention: tenant.Retention{MaxAge: 300}}
		}
		return tenant.Policy{}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	s.now = func() int64 { return 1000 }
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "retention", []int64{100, 800}, []float64{1, 2}))
	require.NoError(t, err)
	f := s.Fetcher("default")
	querying.Store(true)
	it, err := f.Fetch(t.Context(), fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{cacheableNameMatcher("retention")}})
	require.NoError(t, err)
	got, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, []int64{100, 800}, got[0].Timestamps)
	require.EqualValues(t, 1, calls.Load(), "cache eligibility and its miss must use one policy snapshot")
}

func TestRetentionFetchSplitCacheUsesOnePolicySnapshot(t *testing.T) {
	t.Parallel()
	var querying atomic.Bool
	var calls atomic.Int64
	s, err := InMemory(WithQueryCache(16), WithQuerySplitInterval(200), WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		age := time.Duration(0)
		if querying.Load() {
			age = 300
			if calls.Add(1) > 1 {
				age = 100
			}
		}
		return tenant.Policy{Retention: tenant.Retention{MaxAge: age}}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	s.now = func() int64 { return 1000 }
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "retention", []int64{100, 700, 800, 900}, []float64{1, 7, 8, 9}))
	require.NoError(t, err)
	f := s.Fetcher("default")
	querying.Store(true)
	it, err := f.Fetch(t.Context(), fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{cacheableNameMatcher("retention")}})
	require.NoError(t, err)
	got, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, []int64{700, 800, 900}, got[0].Timestamps)
	require.EqualValues(t, 1, calls.Load(), "parallel split children share the top-level policy and clock snapshot")
}
