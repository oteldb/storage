package storage

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// TestWithDecodeMemoryOption checks the option plumbs through and Open builds the shared budget.
func TestWithDecodeMemoryOption(t *testing.T) {
	t.Parallel()

	var o Options
	WithDecodeMemory(1 << 20)(&o)
	assert.Equal(t, int64(1<<20), o.DecodeMemoryBytes)

	s, err := InMemory(WithDecodeMemory(1 << 20))
	require.NoError(t, err)
	assert.NotNil(t, s.decodeBudget)

	off, err := InMemory()
	require.NoError(t, err)
	assert.Nil(t, off.decodeBudget)
}

// TestDecodeMemorySharedAcrossTenants checks the decode-memory budget is one process-wide valve:
// with a budget far smaller than any query's footprint (forcing every query through the
// over-budget-admitted-alone path of the SHARED budget), concurrent queries across multiple tenant
// engines all complete without deadlock and return the same data as an unbudgeted store.
func TestDecodeMemorySharedAcrossTenants(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	byService := WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("service.name"))

		return signal.TenantID(v.Str())
	})

	s, err := InMemory(byService, WithDecodeMemory(1))
	require.NoError(t, err)

	const tenants = 4
	for i := range tenants {
		svc := "svc-" + strconv.Itoa(i)
		_, err := s.WriteMetrics(ctx, gaugeBatch(svc, "http.requests", []int64{100, 200}, []float64{1, 2}))
		require.NoError(t, err)
		// Flush so the query decodes a part (the budget covers part decode, not the head).
		require.NoError(t, mustEngine(s.engineFor(signal.TenantID(svc))).Flush(ctx))
	}

	var wg sync.WaitGroup
	errs := make(chan error, tenants*4)
	for i := range tenants * 4 {
		wg.Go(func() {
			svc := "svc-" + strconv.Itoa(i%tenants)
			eng, err := s.engineFor(signal.TenantID(svc))
			if err != nil {
				errs <- err

				return
			}

			it, err := eng.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")}})
			if err != nil {
				errs <- err

				return
			}

			batches, err := fetch.Drain(ctx, it)
			if err != nil {
				errs <- err

				return
			}

			if len(batches) != 1 ||
				!slices.Equal(batches[0].Timestamps, []int64{100, 200}) ||
				!slices.Equal(batches[0].Values, []float64{1, 2}) {
				errs <- errors.Errorf("unexpected result for %s: %d batches", svc, len(batches))
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
