package storage

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// The identity prune's thresholds (≥4096 identities, ≥¼ of them dead) are deliberately high enough
// that no ordinary test reaches them, so the engine-level tests call it directly. These drive the
// **real maintenance path** at a cardinality that actually trips them: the point is not that the
// prune works — that is covered in engine/ — but that the whole cycle (flush → merge → retention →
// prune) does it without losing data or erroring.

const (
	// churnSeries exceeds the prune's minimum identity count, so the background gate opens.
	churnSeries = 6000
	// liveSeries keep ingesting past the retention cutoff, so ~95% of the identities die.
	liveSeries = 300
)

// pruneStorage builds an in-memory store whose metric retention drops anything older than maxAge.
func pruneStorage(t *testing.T, maxAge time.Duration) *Storage {
	t.Helper()

	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Retention: tenant.Retention{MaxAge: maxAge}}
	})))
	require.NoError(t, err)

	return s
}

// writeChurn ingests one sample at ts for each of the given series indices.
func writeChurn(ctx context.Context, t *testing.T, s *Storage, from, to int, ts int64) {
	t.Helper()

	for i := from; i < to; i++ {
		_, err := s.WriteMetrics(ctx, gaugeBatch("api", "m_"+strconv.Itoa(i), []int64{ts}, []float64{1}))
		require.NoError(t, err)
	}
}

// metricStat returns the tenant's metric SignalStats from Inspect.
func metricStat(t *testing.T, s *Storage) SignalStats {
	t.Helper()

	for _, ts := range s.Inspect().Tenants {
		for i := range ts.Signals {
			if ts.Signals[i].Signal == signal.Metric {
				return ts.Signals[i]
			}
		}
	}

	t.Fatal("no metric signal in Inspect")

	return SignalStats{}
}

// TestMaintainPrunesIdentitiesEndToEnd is the whole cycle at a cardinality that opens the prune's
// gates: churned series age out, the live ones keep ingesting, and the maintenance loop must
// reclaim the dead identities without touching the data that is still there.
func TestMaintainPrunesIdentitiesEndToEnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := pruneStorage(t, 30*time.Minute)

	now := time.Now().UnixNano()
	old := now - int64(2*time.Hour)

	// A churned population, all of it older than the retention cutoff.
	writeChurn(ctx, t, s, 0, churnSeries, old)
	// The survivors: same series ids, re-ingested inside the window.
	writeChurn(ctx, t, s, 0, liveSeries, now-int64(time.Minute))

	// Measured before any maintenance: the head holds every identity ingested.
	before := metricStat(t, s)
	require.EqualValues(t, churnSeries, before.Series)
	require.Positive(t, before.IdentityBytes)

	s.maintain(ctx) // flush, then merge (retention drops the aged rows), then prune
	s.maintain(ctx) // a second cycle must be a no-op for identity, not another sweep

	after := metricStat(t, s)

	t.Logf("series %d → %d, identity bytes %d → %d",
		churnSeries, after.Series, before.IdentityBytes, after.IdentityBytes)

	assert.EqualValues(t, liveSeries, after.Series,
		"only the series still inside the retention window remain")
	assert.Less(t, after.IdentityBytes, before.IdentityBytes/2,
		"the reclaimed identity memory is visible to an operator")

	// The data that survived retention is intact and correct — the prune must not touch it.
	for _, i := range []int{0, liveSeries / 2, liveSeries - 1} {
		got := metricSamples(ctx, t, s, "m_"+strconv.Itoa(i))
		require.Len(t, got, 1, "live series m_%d still resolves", i)
		assert.Equal(t, []float64{1}, got[0].Values)
	}

	// A pruned series resolves to nothing rather than to an empty result with a live identity.
	assert.Empty(t, metricSamples(ctx, t, s, "m_"+strconv.Itoa(churnSeries-1)),
		"an aged-out series is gone from the index too")
}

// TestAdminPruneIdentitiesForces covers the operator trigger: the background thresholds are what
// make the prune rare, so an on-demand sweep must run regardless of them and report what it did.
func TestAdminPruneIdentitiesForces(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := pruneStorage(t, 30*time.Minute)

	now := time.Now().UnixNano()

	// Far below the prune's minimum identity count, so the background pass would skip this entirely.
	const small = 40

	writeChurn(ctx, t, s, 0, small, now-int64(2*time.Hour))
	writeChurn(ctx, t, s, 0, 4, now-int64(time.Minute))

	s.maintain(ctx)

	require.EqualValues(t, small, metricStat(t, s).Series, "the background pass left the dead identities")

	removed, err := s.Admin().PruneIdentities(ctx, "default")
	require.NoError(t, err)
	assert.Equal(t, small-4, removed, "the forced sweep reports what it reclaimed")
	assert.EqualValues(t, 4, metricStat(t, s).Series)

	// Idempotent: nothing left to remove, and no error.
	removed, err = s.Admin().PruneIdentities(ctx, "default")
	require.NoError(t, err)
	assert.Zero(t, removed)

	// The live series survived the forced sweep.
	got := metricSamples(ctx, t, s, "m_0")
	require.Len(t, got, 1)
	assert.Equal(t, []float64{1}, got[0].Values)
}

// TestAdminPruneIdentitiesUnknownTenant checks the no-engine case is a quiet zero, not an error —
// an operator sweeping every tenant must not have to know which ones this node holds.
func TestAdminPruneIdentitiesUnknownTenant(t *testing.T) {
	t.Parallel()

	s := pruneStorage(t, time.Hour)

	removed, err := s.Admin().PruneIdentities(context.Background(), "never-written")
	require.NoError(t, err)
	assert.Zero(t, removed)
}

// metricSamples fetches one metric's batches by name.
func metricSamples(ctx context.Context, t *testing.T, s *Storage, name string) []*fetch.Batch {
	t.Helper()

	eng, err := s.engineFor("default")
	require.NoError(t, err)

	it, err := eng.Fetch(ctx, fetch.Request{
		Start: 0, End: time.Now().UnixNano() + int64(time.Hour),
		Matchers: []fetch.Matcher{nameMatcher(name)},
	})
	require.NoError(t, err)

	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)

	return batches
}
