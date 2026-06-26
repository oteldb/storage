package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func TestTokenBucket(t *testing.T) {
	t.Parallel()

	const t0 = int64(1_000_000_000)

	var b tokenBucket
	b.reconfigure(100, 100, t0) // 100 bytes/sec, burst 100

	assert.True(t, b.allow(100, t0, false), "full burst available")
	assert.False(t, b.allow(1, t0, false), "drained")

	// One second later, the bucket has refilled to burst.
	assert.True(t, b.allow(100, t0+1_000_000_000, false), "refilled after 1s")

	// Half a second ⇒ 50 tokens.
	assert.False(t, b.allow(100, t0+1_500_000_000, false), "only 50 refilled")
	assert.True(t, b.allow(50, t0+1_500_000_000, false))

	// Unlimited always admits regardless of tokens.
	assert.True(t, b.allow(1e9, t0, true))
}

func TestWriteMetricsRateLimit(t *testing.T) {
	t.Parallel()

	// 16 bytes/sec ⇒ one SampleBytes-sized point per second of budget.
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{IngestBytesPerSecond: 16}}
	})))
	require.NoError(t, err)

	s.now = func() int64 { return 1000 } // freeze the clock: no refill between writes

	ctx := context.Background()
	a, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1}, []float64{1}))
	require.NoError(t, err)
	assert.Equal(t, int64(1), a.Accepted)
	assert.Zero(t, a.Rejected)

	// Budget exhausted (no refill at the frozen clock): the next point is shed.
	b, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{2}, []float64{2}))
	require.NoError(t, err)
	assert.Zero(t, b.Accepted)
	assert.Equal(t, int64(1), b.Rejected)
	assert.Equal(t, "rate_limit", b.RejectedReason)

	st := s.AdmissionStats("default")
	assert.Equal(t, int64(1), st.Accepted)
	assert.Equal(t, int64(1), st.RejectedRate)
}

func TestWriteMetricsMaxSeries(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{MaxSeries: 1}}
	})))
	require.NoError(t, err)

	ctx := context.Background()
	a, err := s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1}, []float64{1}))
	require.NoError(t, err)
	assert.Equal(t, int64(1), a.Accepted)

	// A second, distinct series exceeds the cardinality cap and is shed.
	b, err := s.WriteMetrics(ctx, gaugeBatch("api", "m2", []int64{1}, []float64{1}))
	require.NoError(t, err)
	assert.Zero(t, b.Accepted)
	assert.Equal(t, int64(1), b.Rejected)
	assert.Equal(t, "max_series", b.RejectedReason)

	assert.Equal(t, int64(1), s.AdmissionStats("default").RejectedCardinality)
}

func TestWriteMetricsMaxInFlightBytes(t *testing.T) {
	t.Parallel()

	// Cap at two samples' worth of in-flight bytes.
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{MaxInFlightBytes: 32}}
	})))
	require.NoError(t, err)

	ctx := context.Background()
	a, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1, 2, 3}, []float64{1, 2, 3}))
	require.NoError(t, err)
	assert.Equal(t, int64(2), a.Accepted, "two samples fit the in-flight cap")
	assert.Equal(t, int64(1), a.Rejected)
	assert.Equal(t, "max_in_flight_bytes", a.RejectedReason)

	assert.Equal(t, int64(1), s.AdmissionStats("default").RejectedInFlight)
}

func TestSampler(t *testing.T) {
	t.Parallel()

	const sec = int64(1_000_000_000)

	var a tenantAdmission
	ids := make([]signal.SeriesID, 100)
	ts := make([]int64, 100)

	for i := range ids {
		ids[i] = signal.SeriesID{Lo: uint64(i)}
		ts[i] = int64(i)
	}

	// Window 1: no prior history ⇒ no sampling, every point kept at weight 1.
	w, active := a.sample(10, sec, ids, ts)
	assert.False(t, active)
	assert.Nil(t, w)

	// Window 2 (a second later): the prior window saw 100 rows against a budget of 10, so the
	// sampler keeps ~1/10 and weights them by 10.
	w, active = a.sample(10, 2*sec, ids, ts)
	require.True(t, active)
	require.Len(t, w, 100)

	kept, weighted := 0, 0.0

	for _, x := range w {
		if x != 0 {
			kept++
			weighted += x

			assert.InDelta(t, float64(10), x, 0, "kept samples carry the scale factor")
		}
	}

	assert.Positive(t, kept)
	assert.Less(t, kept, 100, "sampling dropped some points")
	// Σ weights is the unbiased estimate of the original count (100); deterministic hashing keeps
	// it close.
	assert.InDelta(t, 100, weighted, 60)

	// Determinism: the same (ids, ts) sample identically.
	w2, _ := a.sample(10, 2*sec+1, ids, ts)
	assert.Equal(t, w, w2)
}

func TestWriteMetricsBudgetedSampling(t *testing.T) {
	t.Parallel()

	const sec = int64(1_000_000_000)

	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Sampling: tenant.Sampling{MaxRowsPerSecond: 10}}
	})))
	require.NoError(t, err)

	now := 5 * sec
	s.now = func() int64 { return now }

	ctx := context.Background()

	// 100 timestamps on one series.
	ts := make([]int64, 100)
	vals := make([]float64, 100)

	for i := range ts {
		ts[i] = int64(i + 1)
		vals[i] = 1
	}

	// Window 1: no prior history ⇒ everything kept, no sampling.
	a1, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", ts, vals))
	require.NoError(t, err)
	assert.Equal(t, int64(100), a1.Accepted)
	assert.Zero(t, s.AdmissionStats("default").SampledDropped)

	// Window 2 (a second later): the prior 100 rows/s over a 10 budget triggers ~10x sampling.
	now += sec

	ts2 := make([]int64, 100)
	for i := range ts2 {
		ts2[i] = int64(i + 1000)
	}

	a2, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", ts2, vals))
	require.NoError(t, err)
	assert.Equal(t, int64(100), a2.Accepted, "sampled-out points still count as accepted (represented)")
	assert.Zero(t, a2.Rejected)

	st := s.AdmissionStats("default")
	assert.Positive(t, st.SampledDropped, "some points were sampled out")
	assert.Less(t, st.SampledDropped, int64(100))

	// The kept window-2 samples carry a scale factor of 10, and their count + dropped == 100.
	eng := mustEngine(s.engineFor("default"))
	it, err := eng.Fetch(ctx, fetch.Request{Start: 1000, End: 2000, Matchers: []fetch.Matcher{nameMatcher("m")}})
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	kept := int64(len(batches[0].Timestamps))
	assert.Equal(t, int64(100), kept+st.SampledDropped, "kept + sampled-dropped == observed")

	for i := range batches[0].Timestamps {
		assert.InDelta(t, float64(10), batches[0].ScaleFactor(i), 0)
	}
}

func TestAdmissionStatsUnknownTenant(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	assert.Equal(t, AdmissionStats{}, s.AdmissionStats("never-written"))
}

func TestWriteMetricsNoLimitsUnaffected(t *testing.T) {
	t.Parallel()

	s, err := InMemory() // default resolver ⇒ no limits
	require.NoError(t, err)

	ctx := context.Background()
	a, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1, 2, 3}, []float64{1, 2, 3}))
	require.NoError(t, err)
	assert.Equal(t, int64(3), a.Accepted)
	assert.Zero(t, a.Rejected)
	assert.Empty(t, a.RejectedReason)
	assert.Equal(t, int64(3), s.AdmissionStats("default").Accepted)
}

func sumCounter(t *testing.T, rm metricdata.ResourceMetrics, name string, want map[string]string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			var total int64
			for _, dp := range sum.DataPoints {
				match := true
				for k, v := range want {
					av, has := dp.Attributes.Value(attribute.Key(k))
					if !has || av.AsString() != v {
						match = false
						break
					}
				}
				if match {
					total += dp.Value
				}
			}
			return total
		}
	}
	return 0
}

func TestWriteMetricsEmitsAdmissionMetrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	s, err := InMemory(WithMeterProvider(mp), WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{MaxSeries: 1}}
	})))
	require.NoError(t, err)

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m1", []int64{1}, []float64{1}))
	require.NoError(t, err)
	// Second distinct series exceeds the cardinality cap ⇒ one rejected (max_series).
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m2", []int64{1}, []float64{1}))
	require.NoError(t, err)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Equal(t, int64(1), sumCounter(t, rm, "storage.ingest.accepted", map[string]string{"signal": "metric"}))
	assert.Equal(t, int64(1), sumCounter(t, rm, "storage.ingest.rejected", map[string]string{"signal": "metric", "reason": "max_series"}))
}

func TestEngineFlushAndFetchMetricsEmitted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	s, err := InMemory(WithMeterProvider(mp))
	require.NoError(t, err)

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1, 2}, []float64{1, 2}))
	require.NoError(t, err)
	s.maintain(ctx) // flushes the head to a part

	it, err := s.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 62, Matchers: []fetch.Matcher{nameMatcher("m")},
	})
	require.NoError(t, err)
	_, err = fetch.Drain(ctx, it)
	require.NoError(t, err)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Positive(t, sumCounter(t, rm, "storage.flush.total", map[string]string{"signal": "metric"}))
	assert.Positive(t, sumCounter(t, rm, "storage.fetch.total", map[string]string{"signal": "metric"}))
}

func TestRecordEngineMetricsEmitted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	s, err := InMemory(WithMeterProvider(mp))
	require.NoError(t, err)

	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{100, 1, "x"}))
	require.NoError(t, err)
	s.maintain(ctx)

	it, err := s.LogFetcher("default").Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62})
	require.NoError(t, err)
	_, err = fetch.Drain(ctx, it)
	require.NoError(t, err)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Positive(t, sumCounter(t, rm, "storage.flush.total", map[string]string{"signal": "log"}))
	assert.Positive(t, sumCounter(t, rm, "storage.fetch.total", map[string]string{"signal": "log"}))
}

func TestBackendMetricsEmitted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	s, err := InMemory(WithMeterProvider(mp))
	require.NoError(t, err)

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1}, []float64{1}))
	require.NoError(t, err)
	s.maintain(ctx) // flush writes a part (backend writes)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	assert.Positive(t, sumCounter(t, rm, "storage.backend.ops", map[string]string{"op": "write"}))
}
