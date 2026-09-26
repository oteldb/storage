package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/faultbackend"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func anyOp(faultbackend.Op) bool { return true }

// TestMergeCandidatesFollowSizeRetention pins the candidate counts to the size cutoff the next
// maintenance cycle applies. The cycle's merges change the part set its cutoff was resolved for, so
// the gauges and Inspect after it must use the cutoff re-resolved for the new set; between cycles
// Inspect reads no backend, and says when the cutoff it has belongs to an earlier part set.
func TestMergeCandidatesFollowSizeRetention(t *testing.T) {
	t.Parallel()

	var budget atomic.Int64

	be := faultbackend.Wrap(backend.Memory())
	reader := sdkmetric.NewManualReader()
	s, err := Open(context.Background(), Options{},
		WithBackend(be),
		WithFlushInterval(-1),
		WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
		WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
			return tenant.Policy{
				Limits:    tenant.Limits{MaxPartSize: 160},
				Retention: tenant.Retention{MaxBytes: budget.Load()},
			}
		})))
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })

	ctx := context.Background()
	now := time.Now().UnixNano()

	write := func(start int64) {
		t.Helper()

		ts, vals := make([]int64, 200), make([]float64, 200)
		for i := range ts {
			ts[i] = start + int64(i)*int64(time.Second)
			vals[i] = float64(i)
		}

		_, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", ts, vals))
		require.NoError(t, err)
	}

	write(now - time.Hour.Nanoseconds())
	s.maintain(ctx)

	eff, err := s.EfficiencyStats(ctx)
	require.NoError(t, err)
	budget.Store(eff[0].Signals[0].StoredBytes)

	write(now - 200*int64(time.Second))
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))

	metricStats := func() SignalStats {
		t.Helper()

		ops := be.Count(anyOp)
		st := s.Inspect()
		require.Equal(t, ops, be.Count(anyOp), "Inspect must not touch the backend")

		for _, ts := range st.Tenants {
			for _, sig := range ts.Signals {
				if sig.Signal == signal.Metric {
					return sig
				}
			}
		}

		require.FailNow(t, "no metric stats")

		return SignalStats{}
	}

	assert.True(t, metricStats().MergeCandidatesStale, "a flush since the last cycle leaves the size cutoff stale")

	s.maintain(ctx)

	eng := mustEngine(s.engineFor("default"))
	exact := eng.MergeShapeWith(s.metricMergeOptions("default", s.sizeCutoffFor(ctx, "default").at(signal.Metric)))

	st := metricStats()
	assert.False(t, st.MergeCandidatesStale, "the cycle re-resolves the cutoff for the set its merges left")
	assert.Equal(t, exact.Candidates, st.MergeCandidates)
	assert.Equal(t, exact.ForceCandidates, st.MergeForceCandidates)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	metricSig := map[string]string{"signal": "metric"}
	assert.Equal(t, int64(exact.Candidates), partGauge(t, rm, "storage.parts.merge_candidates", metricSig))
	assert.Equal(t, int64(exact.ForceCandidates), partGauge(t, rm, "storage.parts.merge_force_candidates", metricSig))
	assert.Zero(t, partGauge(t, rm, "storage.parts.merge_candidates_stale", metricSig))

	// Part sizes that cannot be read when the cycle re-measures must not publish the fallback's
	// zero cutoff as the truth: the gauges keep what they last measured and say they are stale.
	write(now - 100*int64(time.Second))
	require.NoError(t, s.Admin().Flush(ctx, "default", signal.Metric))
	be.Add(faultbackend.Rule{Kind: faultbackend.List, Err: errSizesUnreadable})
	s.recordPartShape(ctx)

	require.NoError(t, reader.Collect(ctx, &rm))
	assert.Equal(t, int64(1), partGauge(t, rm, "storage.parts.merge_candidates_stale", metricSig))
	assert.Equal(t, int64(exact.Candidates), partGauge(t, rm, "storage.parts.merge_candidates", metricSig))
	assert.Equal(t, int64(exact.ForceCandidates), partGauge(t, rm, "storage.parts.merge_force_candidates", metricSig))
	assert.True(t, metricStats().MergeCandidatesStale)

	be.Reset()
	s.recordPartShape(ctx)

	require.NoError(t, reader.Collect(ctx, &rm))
	assert.Zero(t, partGauge(t, rm, "storage.parts.merge_candidates_stale", metricSig), "a readable measurement clears it")
	assert.False(t, metricStats().MergeCandidatesStale)
}

var errSizesUnreadable = errors.New("part sizes unreadable")
