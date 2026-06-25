package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestMaintainFlushesAndMerges(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	s.maintain(ctx) // flush + merge (no retention)

	eng := s.engineFor("default")
	assert.Equal(t, 1, eng.PartCount(), "head flushed to one part")

	batches := queryEngine(t, eng, nameMatcher("m"))
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{100, 200}, batches[0].Timestamps)
}

func TestMaintainAppliesRetention(t *testing.T) {
	t.Parallel()

	// A 1-minute retention with samples timestamped in the distant past ⇒ all dropped.
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Retention: tenant.Retention{MaxAge: time.Minute}}
	})))
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	s.maintain(ctx) // flush, then merge drops everything older than now-1m

	eng := s.engineFor("default")
	assert.Equal(t, 0, eng.PartCount(), "retention dropped every part")
	assert.Empty(t, queryEngine(t, eng, nameMatcher("m")))
}

func TestMaintainRetainsRecentData(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Retention: tenant.Retention{MaxAge: time.Hour}}
	})))
	require.NoError(t, err)

	ctx := context.Background()
	now := time.Now().UnixNano()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{now}, []float64{5}))
	require.NoError(t, err)

	s.maintain(ctx)

	eng := s.engineFor("default")
	assert.Equal(t, 1, eng.PartCount(), "recent sample retained")

	it, err := eng.Fetch(ctx, fetch.Request{Start: 0, End: now + 1, Matchers: []fetch.Matcher{nameMatcher("m")}})
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, batches, 1, "recent sample is within the retention window")
	assert.Equal(t, []float64{5}, batches[0].Values)
}

func TestMultiTenantIsolationThroughMerge(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("service.name"))

		return signal.TenantID(v.Str())
	}))
	require.NoError(t, err)

	ctx := context.Background()
	// Two services across two flushes each, then a compaction.
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100}, []float64{1}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("web", "m", []int64{100}, []float64{2}))
	require.NoError(t, err)
	s.maintain(ctx)
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{200}, []float64{11}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("web", "m", []int64{200}, []float64{22}))
	require.NoError(t, err)
	s.maintain(ctx)

	apiEng, webEng := s.engineFor("api"), s.engineFor("web")
	assert.Equal(t, 1, apiEng.SeriesCount())
	assert.Equal(t, 1, webEng.SeriesCount(), "web never sees api's series")

	api := queryEngine(t, apiEng, nameMatcher("m"))
	web := queryEngine(t, webEng, nameMatcher("m"))
	require.Len(t, api, 1)
	require.Len(t, web, 1)
	assert.NotEqual(t, api[0].ID, web[0].ID)
	assert.Equal(t, []float64{1, 11}, api[0].Values, "api's data only")
	assert.Equal(t, []float64{2, 22}, web[0].Values, "web's data only")
}

func TestBackgroundMaintenanceFlushes(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithFlushInterval(int64(5 * time.Millisecond)))
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100}, []float64{1}))
	require.NoError(t, err)

	// The background loop should flush the head without any explicit call.
	assert.Eventually(t, func() bool {
		return s.engineFor("default").PartCount() >= 1
	}, time.Second, 2*time.Millisecond)

	require.NoError(t, s.Close(ctx))
}

func TestCloseFlushesAllTenants(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("service.name"))

		return signal.TenantID(v.Str())
	}))
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100}, []float64{1}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("web", "m", []int64{100}, []float64{2}))
	require.NoError(t, err)

	apiEng, webEng := s.engineFor("api"), s.engineFor("web")
	require.NoError(t, s.Close(ctx))

	assert.Equal(t, 1, apiEng.PartCount(), "Close flushed tenant api")
	assert.Equal(t, 1, webEng.PartCount(), "Close flushed tenant web")
}
