package storage

import (
	"context"
	"math"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func retentionSignalFetcher(s *Storage, sig signal.Signal, tenants ...signal.TenantID) fetch.Fetcher {
	switch sig {
	case signal.Metric:
		return s.Fetcher(tenants...)
	case signal.Log:
		return s.LogFetcher(tenants...)
	case signal.Trace:
		return s.TraceFetcher(tenants...)
	case signal.Profile:
		return s.ProfileFetcher(tenants...)
	case signal.Exemplar:
		return s.ExemplarFetcher(tenants...)
	default:
		panic("invalid test signal")
	}
}

func writeRetentionSignals(t *testing.T, s *Storage, service string, ts []int64) {
	t.Helper()
	ctx := t.Context()
	_, err := s.WriteMetrics(ctx, gaugeBatch(service, "retention", ts, []float64{10, 20}))
	require.NoError(t, err)
	logs := logBatch(service)
	for _, timestamp := range ts {
		row := logs.Resources[0].Scopes[0].AddRecord()
		row.Timestamp, row.SeverityNumber, row.Body = timestamp, 9, []byte("record")
	}
	_, err = s.WriteLogs(ctx, logs)
	require.NoError(t, err)
	_, err = s.WriteTraces(ctx, traceBatch(service, spanSpec{traceID: "old", spanID: "old", name: "op", start: ts[0], end: ts[0] + 1}, spanSpec{traceID: "live", spanID: "live", name: "op", start: ts[1], end: ts[1] + 1}))
	require.NoError(t, err)
	for _, v := range ts {
		_, err = s.WriteProfiles(ctx, profileBatch(service, v, sampleSpec{typ: "cpu", unit: "ns", value: 1}))
		require.NoError(t, err)
		_, err = s.WriteMetrics(ctx, exemplarBatch(service, "retention-exemplar", v, map[string][]exemplarSpec{"/": {{ts: v, value: 1, traceID: "trace"}}}))
		require.NoError(t, err)
	}
}

func retentionTimestamps(t *testing.T, f fetch.Fetcher, sig signal.Signal, start, end int64) []int64 {
	t.Helper()
	request := fetch.Request{Start: start, End: end}
	if sig == signal.Metric {
		request.Matchers = []fetch.Matcher{nameMatcher("retention")}
	}
	it, err := f.Fetch(t.Context(), request)
	require.NoError(t, err)
	got, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	var times []int64
	for _, b := range got {
		times = append(times, b.Timestamps...)
	}
	slices.Sort(times)
	return times
}

func TestRetentionFetchSignalAndTenantIsolation(t *testing.T) {
	t.Parallel()
	const now = int64(20 * time.Hour)
	times := []int64{now - int64(10*time.Hour), now - int64(time.Hour)}
	for _, sig := range []signal.Signal{signal.Metric, signal.Log, signal.Trace, signal.Profile, signal.Exemplar} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()
			var policy atomic.Pointer[tenant.Retention]
			policy.Store(&tenant.Retention{})
			s, err := InMemory(WithTenant(tenantByService), WithTenancy(tenant.ResolverFunc(func(tid signal.TenantID) tenant.Policy {
				if tid == "a" {
					return tenant.Policy{Retention: *policy.Load()}
				}
				return tenant.Policy{}
			})))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			s.now = func() int64 { return now }
			writeRetentionSignals(t, s, "a", times)
			writeRetentionSignals(t, s, "b", times)
			oldFetcher := retentionSignalFetcher(s, sig, "a")
			require.Equal(t, times, retentionTimestamps(t, oldFetcher, sig, 0, now))
			policy.Store(&tenant.Retention{PerSignal: map[signal.Signal]tenant.SignalRetention{sig: {MaxAge: 2 * time.Hour}}})
			require.Equal(t, times[1:], retentionTimestamps(t, oldFetcher, sig, 0, now))
			require.Equal(t, times, retentionTimestamps(t, retentionSignalFetcher(s, sig, "b"), sig, 0, now))
			for _, other := range []signal.Signal{signal.Metric, signal.Log, signal.Trace, signal.Profile, signal.Exemplar} {
				if other != sig {
					require.Equal(t, times, retentionTimestamps(t, retentionSignalFetcher(s, other, "a"), other, 0, now))
				}
			}
			for _, tenants := range [][]signal.TenantID{{"a", "b"}, nil} {
				require.Equal(t, []int64{times[0], times[1], times[1]}, retentionTimestamps(t, retentionSignalFetcher(s, sig, tenants...), sig, 0, now))
			}
			require.Empty(t, retentionTimestamps(t, oldFetcher, sig, 0, times[0]))
			policy.Store(&tenant.Retention{})
			require.Equal(t, times, retentionTimestamps(t, oldFetcher, sig, 0, now), "physical data was never removed")
		})
	}
}

func TestRetentionFetchCacheTracksPolicyAndClock(t *testing.T) {
	t.Parallel()
	var age, clock atomic.Int64
	clock.Store(1000)
	s, err := InMemory(WithQueryCache(16), WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Retention: tenant.Retention{MaxAge: time.Duration(age.Load())}}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	s.now = clock.Load
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "retention", []int64{100, 800}, []float64{1, 2}))
	require.NoError(t, err)
	f := s.Fetcher("default")
	req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{cacheableNameMatcher("retention")}}
	read := func() []int64 {
		it, err := f.Fetch(t.Context(), req)
		require.NoError(t, err)
		bs, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		if len(bs) == 0 {
			return nil
		}
		return bs[0].Timestamps
	}
	require.Equal(t, []int64{100, 800}, read())
	age.Store(300)
	require.Equal(t, []int64{800}, read(), "a pre-policy cache result cannot expose expired samples")
	clock.Store(1200)
	require.Empty(t, read(), "a fixed policy's advancing cutoff cannot reuse an old boundary result")
	age.Store(0)
	require.Equal(t, []int64{100, 800}, read())
	age.Store(300)
	clock.Store(1000)
	req.Start = 700
	require.Equal(t, []int64{800}, read())
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "retention", []int64{900}, []float64{3}))
	require.NoError(t, err)
	require.Equal(t, []int64{800}, read(), "fully retained settled windows keep normal cache behavior")
}

func TestRetentionFetchCountsAndAggregates(t *testing.T) {
	t.Parallel()
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy { return tenant.Policy{Retention: tenant.Retention{MaxAge: 300}} })))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	s.now = func() int64 { return 1000 }
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "retention", []int64{100, 700, 800, 900}, []float64{1, 7, 8, 9}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "expired", []int64{100}, []float64{1}))
	require.NoError(t, err)
	f := s.Fetcher("default")
	r := fetch.Request{Start: 0, End: 1000}
	count := fetch.CounterOf(f)
	require.NotNil(t, count)
	n, err := count.Count(t.Context(), r)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	grouped := fetch.GroupCounterOf(f)
	require.NotNil(t, grouped)
	groups, err := grouped.CountBy(t.Context(), r, []byte("__name__"))
	require.NoError(t, err)
	require.Equal(t, map[string]int{"retention": 1}, groups)
	grid := fetch.GridCounterOf(f)
	require.NotNil(t, grid)
	got, err := grid.CountGrid(t.Context(), r, fetch.WindowGrid{Start: 100, Step: 200, Width: 200, NumSteps: 5}, nil)
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"": {0, 0, 0, 1, 1}}, got)
	expired, err := grid.CountGrid(t.Context(), fetch.Request{Start: 0, End: 100}, fetch.WindowGrid{Start: 100, Step: 200, Width: 200, NumSteps: 5}, nil)
	require.NoError(t, err)
	require.Empty(t, expired, "wholly expired count grids retain their coordinates and omit zero-only groups")
	_, err = grid.CountGrid(t.Context(), fetch.Request{Start: 0, End: 100}, fetch.WindowGrid{Start: 100, Step: 200, NumSteps: 5}, nil)
	require.Error(t, err, "a clipped-empty read must not bypass grid validation")
	require.NotNil(t, fetch.SeriesListerOf(f))
	series, err := fetch.SeriesListerOf(f).Series(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, series, 2, "metadata remains a superset")
	for _, test := range []struct {
		name string
		read func() ([]engine.NamedAgg, error)
	}{
		{"whole named", func() ([]engine.NamedAgg, error) { return s.AggregateMetricsStepNamed(t.Context(), "default", r, 0) }},
		{"step", func() ([]engine.NamedAgg, error) { return s.AggregateMetricsStepNamed(t.Context(), "default", r, 400) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result, err := test.read()
			require.NoError(t, err)
			require.Len(t, result, 1)
			var count, sum float64
			for _, b := range result[0].Buckets {
				count += b.Count
				sum += b.Sum
			}
			assert.InEpsilon(t, float64(3), count, 1e-12)
			assert.InEpsilon(t, float64(24), sum, 1e-12)
		})
	}
	whole, err := s.AggregateMetrics(t.Context(), "default", r)
	require.NoError(t, err)
	require.Len(t, whole, 1)
	for _, a := range whole {
		require.InEpsilon(t, float64(3), a.Count, 1e-12)
		require.InEpsilon(t, float64(24), a.Sum, 1e-12)
	}
	named, err := s.AggregateMetricsNamed(t.Context(), "default", r)
	require.NoError(t, err)
	require.Len(t, named, 1)
	require.InEpsilon(t, float64(24), named[0].Sum, 1e-12)
	windows, err := s.AggregateMetricsWindowNamed(t.Context(), "default", r, engine.WindowSpec{Step: 200, Window: 400})
	require.NoError(t, err)
	require.Len(t, windows, 1)
	require.Equal(t, []engine.WindowAgg{{End: 800, SeriesAgg: engine.SeriesAgg{Count: 2, Rows: 2, Sum: 15, Min: 7, Max: 8}}, {End: 1000, SeriesAgg: engine.SeriesAgg{Count: 3, Rows: 3, Sum: 24, Min: 7, Max: 9}}}, windows[0].Windows)
	offset, err := s.AggregateMetricsWindowNamed(t.Context(), "default", r, engine.WindowSpec{Step: 200, Window: 400, Anchor: 100})
	require.NoError(t, err)
	require.Len(t, offset, 1)
	require.Equal(t, []engine.WindowAgg{{End: 700, SeriesAgg: engine.SeriesAgg{Count: 1, Rows: 1, Sum: 7, Min: 7, Max: 7}}, {End: 900, SeriesAgg: engine.SeriesAgg{Count: 3, Rows: 3, Sum: 24, Min: 7, Max: 9}}}, offset[0].Windows)
	tried, err := f.(fetch.TryFetcher).TryFetch(t.Context(), fetch.Request{Start: 0, End: 100})
	require.NoError(t, err)
	empty, err := fetch.Drain(t.Context(), tried)
	require.NoError(t, err)
	require.Empty(t, empty)
}

func TestAgeReadFloorSaturates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		now  int64
		age  time.Duration
		want retentionFloor
	}{
		{"disabled", 0, 0, retentionFloor{}}, {"negative", 0, -1, retentionFloor{}}, {"zero cutoff", 1, 1, retentionFloor{enabled: true}},
		{"negative cutoff", -1, 1, retentionFloor{value: -2, enabled: true}}, {"saturation", math.MinInt64, 1, retentionFloor{value: math.MinInt64, enabled: true}},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); require.Equal(t, tc.want, ageReadFloor(tc.now, tc.age)) })
	}
}

func TestRetentionFetchEpochBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		now         int64
		age         time.Duration
		times, want []int64
	}{
		{"zero cutoff", 1, 1, []int64{-1, 0}, []int64{0}},
		{"negative cutoff", -1, 1, []int64{-3, -2}, []int64{-2}},
		{"disabled", 0, 0, []int64{-3, -2}, []int64{-3, -2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy { return tenant.Policy{Retention: tenant.Retention{MaxAge: tc.age}} })))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			s.now = func() int64 { return tc.now }
			eng, err := s.engineFor("default")
			require.NoError(t, err)
			series := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("retention"))})}
			for i, ts := range tc.times {
				_, err = eng.Append(series, ts, float64(i))
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, retentionTimestamps(t, s.Fetcher("default"), signal.Metric, math.MinInt64, math.MaxInt64))
		})
	}
}

func TestRetentionFetchCancellation(t *testing.T) {
	t.Parallel()
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy { return tenant.Policy{Retention: tenant.Retention{MaxAge: 300}} })))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	s.now = func() int64 { return 1000 }
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "retention", []int64{100, 800}, []float64{1, 2}))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	f := s.Fetcher("default")
	r := fetch.Request{Start: 0, End: 100}
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"fetch", func() error { _, err := f.Fetch(ctx, r); return err }},
		{"try", func() error { _, err := f.(fetch.TryFetcher).TryFetch(ctx, r); return err }},
		{"count", func() error { _, err := fetch.CounterOf(f).Count(ctx, r); return err }},
		{"count by", func() error { _, err := fetch.GroupCounterOf(f).CountBy(ctx, r, []byte("__name__")); return err }},
		{"grid", func() error {
			_, err := fetch.GridCounterOf(f).CountGrid(ctx, r, fetch.WindowGrid{Start: 100, Step: 10, Width: 10, NumSteps: 1}, nil)
			return err
		}},
		{"aggregate", func() error { _, err := s.AggregateMetrics(ctx, "default", r); return err }},
		{"named", func() error { _, err := s.AggregateMetricsNamed(ctx, "default", r); return err }},
		{"step", func() error { _, err := s.AggregateMetricsStepNamed(ctx, "default", r, 10); return err }},
		{"window", func() error {
			_, err := s.AggregateMetricsWindowNamed(ctx, "default", r, engine.WindowSpec{Step: 10, Window: 20})
			return err
		}},
		{"peer fetch", func() error { _, err := s.localFetch(ctx, "default", 0, 100, nil); return err }},
		{"peer records", func() error {
			_, err := s.localRecordFetch(ctx, fetch.Request{Tenant: "default", Signal: signal.Log, Start: 0, End: 100})
			return err
		}},
		{"peer aggregate", func() error { _, err := s.localAggregate(ctx, "default", 0, 100, 10, nil); return err }},
		{"peer window", func() error {
			_, err := s.localAggregateWindow(ctx, "default", 0, 100, engine.WindowSpec{Step: 10, Window: 20}, nil)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); require.ErrorIs(t, tc.call(), context.Canceled) })
	}
}

func TestRetentionFetchExpiredWindowValidation(t *testing.T) {
	t.Parallel()
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy { return tenant.Policy{Retention: tenant.Retention{MaxAge: 300}} })))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	s.now = func() int64 { return 1000 }
	_, err = s.WriteMetrics(t.Context(), gaugeBatch("api", "retention", []int64{100, 800}, []float64{1, 8}))
	require.NoError(t, err)
	eng, ok := s.lookupEngine("default")
	require.True(t, ok)
	expired := fetch.Request{Start: 0, End: 100}
	for _, tc := range []struct {
		name string
		step int64
	}{{"negative", -1}, {"zero", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := engine.WindowSpec{Step: tc.step, Window: 100}
			_, wantErr := eng.AggregateWindowNamed(t.Context(), expired, spec)
			require.Error(t, wantErr)
			_, err := s.AggregateMetricsWindowNamed(t.Context(), "default", expired, spec)
			require.ErrorIs(t, err, wantErr)
			_, err = s.localAggregateWindow(t.Context(), "default", expired.Start, expired.End, spec, nil)
			require.ErrorIs(t, err, wantErr)
			disjoint, err := s.AggregateMetricsStepNamed(t.Context(), "default", expired, tc.step)
			require.NoError(t, err)
			require.Empty(t, disjoint, "disjoint nonpositive step remains valid")
			peerDisjoint, err := s.localAggregate(t.Context(), "default", expired.Start, expired.End, tc.step, nil)
			require.NoError(t, err)
			require.Empty(t, peerDisjoint)
		})
	}
	windows, err := s.AggregateMetricsWindowNamed(t.Context(), "default", expired, engine.WindowSpec{Step: 100, Window: 100})
	require.NoError(t, err)
	require.Empty(t, windows)
}
