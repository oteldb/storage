package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// TestFacadeCardinalityOverflow exercises the metric overflow remapper end-to-end: past the soft
// budget, distinct series collapse into one per-metric __overflow__ series, all points are accepted,
// and the overflow count surfaces in AdmissionStats.
func TestFacadeCardinalityOverflow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{MaxSeries: 1000, MaxSeriesSoft: 2}}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	// Five distinct series of the same metric "http.requests": 2 under budget, 3 overflow.
	var acc Accepted
	for i := range 5 {
		a, werr := s.WriteMetrics(ctx, gaugeBatch("svc"+strconv.Itoa(i), "http.requests", []int64{int64(i + 1)}, []float64{1}))
		require.NoError(t, werr)
		acc.Accepted += a.Accepted
		acc.Rejected += a.Rejected
	}

	assert.Equal(t, int64(5), acc.Accepted, "all points retained")
	assert.Equal(t, int64(0), acc.Rejected, "nothing shed — overflow absorbed the spike")

	st := s.AdmissionStats("default")
	assert.Equal(t, int64(3), st.Overflowed, "three series past the soft budget routed to overflow")
	assert.Equal(t, int64(0), st.Rejected())

	// The overflow series is queryable: __overflow__="true" carrying the redirected samples.
	it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 62,
		Matchers: []fetch.Matcher{{
			Name:  metricOverflowLabel,
			Match: func(v signal.Value) bool { return string(v.Str()) == "true" },
		}},
	})
	require.NoError(t, err)
	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, got, 1, "one collapsed overflow series")
	assert.Len(t, got[0].Timestamps, 3, "three redirected samples")
}
