package storage

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/vfs/faultfs"
	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
	"github.com/oteldb/storage/tenant"
	"github.com/oteldb/storage/wal"
)

func projectedFixture(series, points int, ts int64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	for i := range series {
		m := sm.Metrics().AppendEmpty()
		m.SetName("m")
		data := m.SetEmptyGauge().DataPoints()
		for j := range points {
			point := data.AppendEmpty()
			point.SetTimestamp(pcommon.Timestamp(ts + int64(j)))
			point.SetDoubleValue(float64(i + 1))
			point.Attributes().PutStr("series", strconv.Itoa(i))
		}
	}
	return md
}

func countingProjection(project func(func(*metric.Batch)) int, calls *int) func(func(*metric.Batch)) int {
	var sources []metric.Source
	materialize := metric.NewMaterializer(func(group, point int) metric.Identity {
		*calls++
		return sources[group].Identity(point)
	})
	return func(emit func(*metric.Batch)) int {
		return project(func(b *metric.Batch) {
			source := b.Source()
			index := len(sources)
			sources = append(sources, source)
			copyBatch := *b
			copyBatch.SetSource(b.Resource(), b.Scope(), metric.NewSource(materialize, index))
			emit(&copyBatch)
		})
	}
}

func TestWriteProjectedMetricsLifecycle(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	be, err := file.New(filepath.Join(dir, "parts"))
	require.NoError(t, err)
	options := Options{Backend: be, WALDir: filepath.Join(dir, "wal"), FlushInterval: int64(time.Hour), FlushThresholdBytes: -1}
	s, err := Open(ctx, options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	var p pdataconv.MetricsProjector
	write := func(ts int64) int {
		md := projectedFixture(3, 2, ts)
		require.True(t, p.Prepare(md))
		calls := 0
		acc, err := s.WriteProjectedMetrics(ctx, countingProjection(p.Project, &calls))
		require.NoError(t, err)
		require.EqualValues(t, 6, acc.Accepted)
		p.Reset()
		md.ResourceMetrics().RemoveIf(func(pmetric.ResourceMetrics) bool { return true })
		return calls
	}
	require.Equal(t, 3, write(1))
	require.Zero(t, write(3))
	eng := mustEngine(s.engineFor("default"))
	require.NoError(t, eng.Flush(ctx))
	require.Zero(t, write(5), "known identities relog from registry")
	batches := queryEngine(t, eng, nameMatcher("m"))
	require.Len(t, batches, 3)
	for _, b := range batches {
		require.Len(t, b.Timestamps, 6)
	}
	require.NoError(t, s.Close(ctx))
	s, err = Open(ctx, options)
	require.NoError(t, err)
	require.Zero(t, write(7), "recovered registry avoids materialization")
	require.Len(t, queryEngine(t, mustEngine(s.engineFor("default")), nameMatcher("m")), 3)
	require.NoError(t, mustEngine(s.engineFor("default")).Reset(ctx))
	require.Equal(t, 3, write(9), "reset registry resolves misses from current input")
}

func TestWriteProjectedMetricsPolicies(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		policy tenant.Policy
	}{
		{name: "unlimited"},
		{name: "rate", policy: tenant.Policy{Limits: tenant.Limits{IngestBytesPerSecond: 32}}},
		{name: "cardinality", policy: tenant.Policy{Limits: tenant.Limits{MaxSeries: 2}}},
		{name: "overflow", policy: tenant.Policy{Limits: tenant.Limits{MaxSeriesSoft: 2}}},
		{name: "bytes", policy: tenant.Policy{Limits: tenant.Limits{MaxInFlightBytes: 32}}},
		{name: "sampling", policy: tenant.Policy{Sampling: tenant.Sampling{MaxRowsPerSecond: 10}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var stores [2]*Storage
			now := int64(1e9)
			for i := range stores {
				s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy { return test.policy })))
				require.NoError(t, err)
				s.now = func() int64 { return now }
				stores[i] = s
				t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			}
			for pass := range 2 {
				md := projectedFixture(30, 4, int64(pass*1000+1))
				var native metric.Metrics
				require.Equal(t, pdataconv.Dropped{}, pdataconv.AppendMetrics(&native, md))
				var p pdataconv.MetricsProjector
				require.True(t, p.Prepare(md))
				expected, err := stores[0].WriteMetrics(t.Context(), native)
				require.NoError(t, err)
				actual, err := stores[1].WriteProjectedMetrics(t.Context(), p.Project)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
				require.Equal(t, stores[0].AdmissionStats("default"), stores[1].AdmissionStats("default"))
				p.Reset()
				now += 1e9
			}
			a := queryEngine(t, mustEngine(stores[0].engineFor("default")), nameMatcher("m"))
			b := queryEngine(t, mustEngine(stores[1].engineFor("default")), nameMatcher("m"))
			require.Equal(t, a, b, "sampled indices and overflow identities must match")
		})
	}
}

func TestWriteProjectedMetricsGuards(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		store *Storage
		err   error
	}{
		{name: "cluster", store: &Storage{cluster: &clusterNode{}}, err: ErrMetricProjectionUnsupported},
		{name: "read-only", store: &Storage{opts: Options{ReadOnly: true}}, err: ErrReadOnly},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			// Use an initialized handle for observability, replacing only the guard state.
			s, err := InMemory()
			require.NoError(t, err)
			t.Cleanup(func() { s.cluster = nil; s.opts.ReadOnly = false; require.NoError(t, s.Close(context.Background())) })
			s.cluster = test.store.cluster
			s.opts.ReadOnly = test.store.opts.ReadOnly
			_, err = s.WriteProjectedMetrics(t.Context(), func(func(*metric.Batch)) int { t.Fatal("guard invoked projector"); return 0 })
			require.ErrorIs(t, err, test.err)
			s.closed.Store(true)
			_, err = s.WriteProjectedMetrics(t.Context(), func(func(*metric.Batch)) int { t.Fatal("closed invoked projector"); return 0 })
			require.ErrorIs(t, err, ErrClosed)
			s.closed.Store(false)
		})
	}
}

func TestWriteProjectedMetricsTenants(t *testing.T) {
	t.Parallel()
	s, err := InMemory(WithTenant(func(r signal.Resource, sc signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("tenant"))
		return signal.TenantID(string(v.Str()) + string(sc.Name))
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	md := pmetric.NewMetrics()
	for _, tid := range []string{"one", "wal", "two", "one"} {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("tenant", tid)
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("m")
		point := m.SetEmptyGauge().DataPoints().AppendEmpty()
		point.SetIntValue(1)
		point.SetTimestamp(1)
	}
	var p pdataconv.MetricsProjector
	require.True(t, p.Prepare(md))
	result, err := s.WriteProjectedMetrics(t.Context(), p.Project)
	require.NoError(t, err)
	require.EqualValues(t, 3, result.Accepted)
	require.EqualValues(t, 1, result.Rejected)
	require.Equal(t, reasonReservedTenant, result.RejectedReason)
	for _, tid := range []signal.TenantID{"one", "two"} {
		require.EqualValues(t, 1, mustEngine(s.engineFor(tid)).Stats().Series)
	}
}

func TestProjectedMetricsWALFailure(t *testing.T) {
	t.Parallel()
	fsys := faultfs.New()
	w, err := wal.CreateFS(fsys, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	eng := engine.New(engine.Config{WAL: w})
	md := projectedFixture(3, 1, 1)
	var p pdataconv.MetricsProjector
	s, err := InMemory()
	require.NoError(t, err)
	s.tenants["default"] = eng
	t.Cleanup(func() { delete(s.tenants, "default"); require.NoError(t, s.Close(context.Background())) })
	stage := func() { require.True(t, p.Prepare(md)) }
	stage()
	injected := errors.New("projected WAL failure")
	fsys.Add(faultfs.Rule{Op: faultfs.OpWrite, Times: 1, Err: injected, Match: func(c faultfs.Call) bool { return strings.HasSuffix(c.Name, ".wal") }})
	_, err = s.WriteProjectedMetrics(t.Context(), p.Project)
	require.ErrorIs(t, err, injected)
	require.Zero(t, eng.Stats().Series)
	p.Reset()
	stage()
	result, err := s.WriteProjectedMetrics(t.Context(), p.Project)
	require.NoError(t, err)
	require.EqualValues(t, 3, result.Accepted)
	p.Reset()
	md.ResourceMetrics().RemoveIf(func(pmetric.ResourceMetrics) bool { return true })
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

func TestWriteProjectedMetricsConcurrent(t *testing.T) {
	t.Parallel()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	const workers, rounds = 4, 10
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Go(func() {
			md := projectedFixture(4, 4, 1)
			var p pdataconv.MetricsProjector
			for range rounds {
				if !p.Prepare(md) {
					errs <- errors.New("ineligible fixture")
					return
				}
				_, err := s.WriteProjectedMetrics(t.Context(), p.Project)
				p.Reset()
				if err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, workers*rounds*16, mustEngine(s.engineFor("default")).Stats().HeadSamples)
}

func TestWriteProjectedMetricsCanceledContextParity(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	md := projectedFixture(2, 2, 1)
	var native metric.Metrics
	pdataconv.AppendMetrics(&native, md)
	var p pdataconv.MetricsProjector
	require.True(t, p.Prepare(md))
	a, err := InMemory()
	require.NoError(t, err)
	b, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, a.Close(context.Background()))
		require.NoError(t, b.Close(context.Background()))
	})
	expected, expectedErr := a.WriteMetrics(ctx, native)
	actual, actualErr := b.WriteProjectedMetrics(ctx, p.Project)
	require.Equal(t, expected, actual)
	require.Equal(t, expectedErr, actualErr)
	p.Reset()
}
