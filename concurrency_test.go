package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// TestMaintainParallelWithConcurrentReads exercises the parallel maintenance fan-out across many
// independent tenant engines while reads run concurrently — the race detector (go test -race) is the
// real assertion. It also confirms data survives a flush/merge cycle under contention.
func TestMaintainParallelWithConcurrentReads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Route each service to its own tenant so maintenance has many independent engines to fan out over.
	s, err := InMemory(
		WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
			v, _ := r.Attributes.Get([]byte("service.name"))
			return signal.TenantID(v.Str())
		}),
		WithMaintenanceConcurrency(4),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	const tenants = 12
	for i := range tenants {
		svc := fmt.Sprintf("svc-%02d", i)
		_, err := s.WriteMetrics(ctx, gaugeBatch(svc, "m", []int64{100, 200, 300}, []float64{1, 2, 3}))
		require.NoError(t, err)
	}

	// Hammer maintenance (parallel flush/merge across tenants) against concurrent cross-tenant reads.
	var wg sync.WaitGroup

	wg.Go(func() {
		for range 20 {
			s.maintain(ctx)
		}
	})

	wg.Go(func() {
		for range 20 {
			it, err := s.Fetcher().Fetch(ctx, fetch.Request{Start: 0, End: 1 << 60})
			if err != nil {
				continue
			}

			_, _ = fetch.Drain(ctx, it)
		}
	})

	wg.Wait()

	// Every tenant's three samples survive the flush/merge cycles.
	total := 0
	for i := range tenants {
		svc := fmt.Sprintf("svc-%02d", i)
		it, err := s.Fetcher(signal.TenantID(svc)).Fetch(ctx, fetch.Request{
			Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("m")},
		})
		require.NoError(t, err)

		batches, err := fetch.Drain(ctx, it)
		require.NoError(t, err)

		for _, b := range batches {
			total += len(b.Timestamps)
		}
	}

	assert.Equal(t, tenants*3, total, "all samples survive parallel maintenance")
}

// TestCrossTenantMergeParallel checks the parallel fetch.Merge fan-out federates equal-labeled
// series across tenants identically to the sequential version (order-independent dedup).
func TestCrossTenantMergeParallel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory(WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("service.name"))
		return signal.TenantID(v.Str())
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	// Same metric name in many tenants at distinct timestamps; a no-arg fetch fans out across all.
	const tenants = 10
	for i := range tenants {
		_, err := s.WriteMetrics(ctx, gaugeBatch(fmt.Sprintf("svc-%02d", i), "m", []int64{int64(i + 1)}, []float64{float64(i)}))
		require.NoError(t, err)
	}

	it, err := s.Fetcher().Fetch(ctx, fetch.Request{Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("m")}})
	require.NoError(t, err)

	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)

	total := 0
	for _, b := range batches {
		total += len(b.Timestamps)
	}

	assert.Equal(t, tenants, total, "every tenant's sample is present after the parallel cross-tenant merge")
}
