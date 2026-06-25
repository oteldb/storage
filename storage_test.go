package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// gaugeBatch builds a one-gauge OTLP batch under resource service.name=service.
func gaugeBatch(service, name string, ts []int64, values []float64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", service)
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	g := m.SetEmptyGauge()

	for i := range ts {
		dp := g.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(ts[i]))
		dp.SetDoubleValue(values[i])
	}

	return md
}

func nameMatcher(name string) fetch.Matcher {
	want := []byte(name)

	return fetch.Matcher{Name: metric.LabelName, Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), want) }}
}

func queryEngine(t *testing.T, e *engine.Engine, m fetch.Matcher) []*fetch.Batch {
	t.Helper()
	it, err := e.Fetch(context.Background(), fetch.Request{Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{m}})
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	return got
}

func TestWriteMetricsAndFetch(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	acc, err := s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)
	assert.Equal(t, int64(2), acc.Accepted)
	assert.Equal(t, int64(0), acc.Rejected)

	batches := queryEngine(t, s.engineFor("default"), nameMatcher("http.requests"))
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{100, 200}, batches[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, batches[0].Values)
	// The folded __name__ label is in the reconstructed identity.
	nv, ok := batches[0].Series.Attributes.Get(metric.LabelName)
	require.True(t, ok)
	assert.Equal(t, []byte("http.requests"), nv.Str())
}

func TestWriteMetricsFlushThenFetch(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	_, err = s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	eng := s.engineFor("default")
	require.NoError(t, eng.Flush(context.Background()))
	assert.Equal(t, 1, eng.PartCount())

	// Ingest more after the flush; the query reads head ∪ part transparently.
	_, err = s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", []int64{300}, []float64{3}))
	require.NoError(t, err)

	batches := queryEngine(t, eng, nameMatcher("http.requests"))
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{100, 200, 300}, batches[0].Timestamps)
	assert.Equal(t, []float64{1, 2, 3}, batches[0].Values)
}

func TestMultiTenantRouting(t *testing.T) {
	t.Parallel()

	// Route each record to a tenant named after its service.
	s, err := InMemory(WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("service.name"))

		return signal.TenantID(v.Str())
	}))
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1}, []float64{1}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("web", "m", []int64{1}, []float64{2}))
	require.NoError(t, err)

	apiEng, webEng := s.engineFor("api"), s.engineFor("web")
	assert.Equal(t, 1, apiEng.SeriesCount())
	assert.Equal(t, 1, webEng.SeriesCount())

	apiBatches := queryEngine(t, apiEng, nameMatcher("m"))
	webBatches := queryEngine(t, webEng, nameMatcher("m"))
	require.Len(t, apiBatches, 1)
	require.Len(t, webBatches, 1)
	// Different services ⇒ different identities, isolated per tenant.
	assert.NotEqual(t, apiBatches[0].ID, webBatches[0].ID)
	assert.InDelta(t, 1.0, apiBatches[0].Values[0], 0)
	assert.InDelta(t, 2.0, webBatches[0].Values[0], 0)
}

func TestWriteMetricsRejectsUnsupported(t *testing.T) {
	t.Parallel()

	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	h := sm.Metrics().AppendEmpty()
	h.SetName("h")
	h.SetEmptyHistogram().DataPoints().AppendEmpty() // unsupported ⇒ rejected
	g := sm.Metrics().AppendEmpty()
	g.SetName("g")
	dp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(5)

	s, err := InMemory()
	require.NoError(t, err)
	acc, err := s.WriteMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Equal(t, int64(1), acc.Accepted)
	assert.Equal(t, int64(1), acc.Rejected)
}

func TestWriteAfterCloseRejected(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, s.Close(context.Background()), "Close is idempotent")

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1}, []float64{1}))
	require.ErrorIs(t, err, ErrClosed)
	_, err = s.WriteLogs(ctx, plog.NewLogs())
	require.ErrorIs(t, err, ErrClosed)
	_, err = s.Query(ctx, "t", Query{Lang: LangPromQL})
	require.ErrorIs(t, err, ErrClosed)
}

func TestUnimplementedSignals(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	ctx := context.Background()

	_, err = s.WriteLogs(ctx, plog.NewLogs())
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = s.WriteTraces(ctx, ptrace.NewTraces())
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = s.WriteProfiles(ctx, pprofile.NewProfiles())
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = s.Query(ctx, "", Query{Lang: LangPromQL})
	require.ErrorIs(t, err, ErrNotImplemented)
}

func TestOOOWindowViaStorage(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithOOOWindow(50))
	require.NoError(t, err)
	ctx := context.Background()

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100}, []float64{1}))
	require.NoError(t, err)

	acc, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{40}, []float64{2})) // older than 100-50
	require.NoError(t, err)
	assert.Equal(t, int64(0), acc.Accepted)
	assert.Equal(t, int64(1), acc.Rejected)
}

func TestLangString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "promql", LangPromQL.String())
	assert.Equal(t, "logql", LangLogQL.String())
	assert.Equal(t, "traceql", LangTraceQL.String())
	assert.Equal(t, "genericql", LangGenericQL.String())
	assert.Equal(t, "unknown", Lang(99).String())
}
