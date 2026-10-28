package storage

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/tenant"
	"github.com/oteldb/storage/wal"
)

func TestMetricAppendBoundaries(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		points []int
		writes int64
	}{
		{name: "small-metrics", points: []int{1, 1, 1}, writes: 1},
		{name: "full-buffer", points: []int{2048, 2048}, writes: 1},
		{name: "overflow-buffer", points: []int{2048, 2049}, writes: 2},
		{name: "large-metric-intact", points: []int{4097}, writes: 1},
		{name: "around-large-metric", points: []int{1, 4097, 1}, writes: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			dir := t.TempDir()
			be, err := file.New(filepath.Join(dir, "parts"))
			require.NoError(t, err)
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
			s, err := Open(ctx, Options{Backend: be, WALDir: filepath.Join(dir, "wal"),
				FlushInterval: int64(time.Hour), FlushThresholdBytes: -1}, WithMeterProvider(provider))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			var md metric.Metrics
			scope := md.AddResource().AddScope()
			var count int64
			for i, points := range tt.points {
				m := scope.AddMetric()
				m.Name = []byte("metric-" + strconv.Itoa(i))
				m.Kind = metric.KindGauge
				for point := range points {
					p := m.AddPoint()
					p.Ts, p.Value = int64(point+1), float64(point)
					count++
				}
			}
			accepted, err := s.WriteMetrics(ctx, md)
			require.NoError(t, err)
			assert.Equal(t, count, accepted.Accepted)
			assert.Zero(t, accepted.Rejected)
			var snapshot metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(ctx, &snapshot))
			var writes int64
			for _, scope := range snapshot.ScopeMetrics {
				for _, m := range scope.Metrics {
					if m.Name == "storage.wal.appends" {
						for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
							writes += point.Value
						}
					}
				}
			}
			assert.Equal(t, tt.writes, writes)
		})
	}
}

func TestMetricAppendWALFailureAndRetry(t *testing.T) {
	t.Parallel()
	fsys := faultfs.New()
	w, err := wal.CreateFS(fsys, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	eng := engine.New(engine.Config{WAL: w})
	var md metric.Metrics
	scope := md.AddResource().AddScope()
	for i := range 3 {
		m := scope.AddMetric()
		m.Name, m.Kind = []byte(strconv.Itoa(i)), metric.KindGauge
		p := m.AddPoint()
		p.Ts, p.Value = int64(i+1), float64(i+1)
	}
	var buffer metricAppendBuffer
	stage := func() { metric.Project(md, func(b *metric.Batch) { buffer.append(b) }) }
	stage()
	injected := errors.New("injected batch WAL failure")
	fsys.Add(faultfs.Rule{Op: faultfs.OpWrite, Times: 1, Err: injected,
		Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") }})
	_, err = buffer.flush(eng)
	require.ErrorIs(t, err, injected)
	assert.Zero(t, eng.Stats().HeadSamples)
	assert.Zero(t, eng.Stats().Series)
	stage()
	result, err := buffer.flush(eng)
	require.NoError(t, err)
	assert.Equal(t, 3, result.Accepted)
	assert.Empty(t, buffer.sources)
	entries, err := fsys.ReadDir(".")
	require.NoError(t, err)
	replayed := engine.New(engine.Config{})
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".wal") {
			data, err := fsys.ReadFile(entry.Name())
			require.NoError(t, err)
			require.NoError(t, replayed.ApplyReplicated(data))
		}
	}
	assert.EqualValues(t, 3, replayed.Stats().Series)
	assert.EqualValues(t, 3, replayed.Stats().HeadSamples)
}

func TestMetricAppendPolicyCompatibility(t *testing.T) {
	t.Parallel()
	for _, limits := range []tenant.Limits{
		{IngestBytesPerSecond: 1}, {MaxSeries: 1}, {MaxSeriesSoft: 1}, {MaxInFlightBytes: 1},
	} {
		assert.False(t, (metricRun{limits: limits}).canBatch())
	}
	assert.False(t, (metricRun{sampling: 1}).canBatch())
	assert.True(t, (metricRun{limits: tenant.Limits{MaxPartSize: 100}}).canBatch())
}

func TestMetricAppendPreservesRepeatedSeriesOrder(t *testing.T) {
	t.Parallel()
	for _, durable := range []bool{false, true} {
		t.Run(strconv.FormatBool(durable), func(t *testing.T) {
			t.Parallel()
			var s *Storage
			var err error
			if durable {
				dir := t.TempDir()
				be, openErr := file.New(filepath.Join(dir, "parts"))
				require.NoError(t, openErr)
				s, err = Open(t.Context(), Options{Backend: be, WALDir: filepath.Join(dir, "wal"),
					OOOWindow: 10, FlushInterval: int64(time.Hour), FlushThresholdBytes: -1})
			} else {
				s, err = InMemory(WithOOOWindow(10))
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			var md metric.Metrics
			scope := md.AddResource().AddScope()
			for i, ts := range []int64{100, 80, 95, 101} {
				m := scope.AddMetric()
				m.Name, m.Kind = []byte("same"), metric.KindGauge
				p := m.AddPoint()
				p.Ts, p.Value = ts, float64(i+10)
			}
			acc, err := s.WriteMetrics(t.Context(), md)
			require.NoError(t, err)
			assert.Equal(t, int64(3), acc.Accepted)
			assert.Equal(t, int64(1), acc.Rejected)
			eng := mustEngine(s.engineFor(signal.TenantID("default")))
			batches := queryEngine(t, eng, nameMatcher("same"))
			require.Len(t, batches, 1)
			assert.Equal(t, []int64{95, 100, 101}, batches[0].Timestamps)
			assert.Equal(t, []float64{12, 10, 13}, batches[0].Values)
		})
	}
}

func TestMetricAppendSamplingPolicyTransition(t *testing.T) {
	t.Parallel()
	const sec = int64(time.Second)
	for _, disableSameWindow := range []bool{false, true} {
		t.Run(strconv.FormatBool(disableSameWindow), func(t *testing.T) {
			t.Parallel()
			budget := int64(10)
			s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
				return tenant.Policy{Sampling: tenant.Sampling{MaxRowsPerSecond: budget}}
			})))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			now := sec
			s.now = func() int64 { return now }
			var reference tenantAdmission
			write := func(n int) {
				ts, values := make([]int64, n), make([]float64, n)
				for i := range ts {
					ts[i], values[i] = now+int64(i), 1
				}
				md := gaugeBatch("api", "m", ts, values)
				var dropped int64
				metric.Project(md, func(b *metric.Batch) {
					_, d := reference.sampleBatch(budget, now, b.IDs, b.Ts)
					dropped += d
				})
				before := s.AdmissionStats("default").SampledDropped
				acc, err := s.WriteMetrics(t.Context(), md)
				require.NoError(t, err)
				assert.Equal(t, int64(n), acc.Accepted)
				assert.Equal(t, dropped, s.AdmissionStats("default").SampledDropped-before)
			}
			write(100)
			now += sec
			if disableSameWindow {
				write(100)
				now++
			}
			budget = 0
			write(1)
			now += sec
			budget = 10
			write(1)
		})
	}
}

func TestMetricAppendTenantBoundaries(t *testing.T) {
	t.Parallel()
	s, err := InMemory(WithTenant(tenantByService))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	var md metric.Metrics
	for i, tid := range []string{"api", "web", "wal", "api"} {
		part := gaugeBatch(tid, "m", []int64{int64(i + 1)}, []float64{float64(i + 10)})
		md.Resources = append(md.Resources, part.Resources...)
	}
	acc, err := s.WriteMetrics(t.Context(), md)
	require.NoError(t, err)
	assert.Equal(t, int64(3), acc.Accepted)
	assert.Equal(t, int64(1), acc.Rejected)
	for _, tt := range []struct {
		tenant signal.TenantID
		values []float64
	}{
		{tenant: "api", values: []float64{10, 13}},
		{tenant: "web", values: []float64{11}},
	} {
		batches := queryEngine(t, mustEngine(s.engineFor(tt.tenant)), nameMatcher("m"))
		require.Len(t, batches, 1)
		assert.Equal(t, tt.values, batches[0].Values)
	}
	_, exists := s.lookupEngine("wal")
	assert.False(t, exists)
}
