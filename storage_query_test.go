package storage_test

// These tests validate the end-to-end read path the library actually supports: ingest
// through the facade, then query through the language-agnostic fetch seam. The library owns
// no PromQL engine, so the test plays the embedder's role — it builds a Prometheus engine
// and drives it over the query/promql adapter wrapping Storage.Fetcher. This is exactly how
// go-faster/oteldb would consume the store.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	qpromql "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

const sec = int64(1e9) // one second in nanoseconds

type smpl struct {
	tSec int64
	v    float64
}

func promEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		MaxSamples:           10_000_000,
		Timeout:              time.Minute,
		LookbackDelta:        5 * time.Minute,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	})
}

func queryable(s *storage.Storage) *qpromql.Queryable {
	return qpromql.NewQueryable(s.Fetcher("default"), "default")
}

func instant(t *testing.T, s *storage.Storage, text string, atSec int64) *promql.Result {
	t.Helper()
	pq, err := promEngine().NewInstantQuery(context.Background(), queryable(s), nil, text, time.Unix(0, atSec*sec))
	require.NoError(t, err)
	t.Cleanup(pq.Close)
	res := pq.Exec(context.Background())
	require.NoError(t, res.Err)

	return res
}

// writeSeries ingests one metric (name, kind) whose data points span the given series,
// keyed by their `route` label value, each with second-resolution samples.
func writeSeries(t *testing.T, s *storage.Storage, name string, kind metric.PointKind, byRoute map[string][]smpl) {
	t.Helper()

	var md metric.Metrics
	rm := md.AddResource()
	rm.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("svc"))},
	)}
	mt := rm.AddScope().AddMetric()
	mt.Name = []byte(name)
	mt.Kind = kind
	if kind == metric.KindSum {
		mt.Temporality = metric.TemporalityCumulative
		mt.Monotonic = true
	}

	for route, samples := range byRoute {
		attrs := signal.NewAttributes(
			signal.KeyValue{Key: []byte("route"), Value: signal.StringValue([]byte(route))},
		)
		for _, sp := range samples {
			p := mt.AddPoint()
			p.Ts = sp.tSec * sec
			p.Value = sp.v
			p.Attributes = attrs
		}
	}

	_, err := s.WriteMetrics(context.Background(), md)
	require.NoError(t, err)
}

// vectorByRoute indexes an instant-vector result by each sample's route label.
func vectorByRoute(t *testing.T, res *promql.Result) map[string]float64 {
	t.Helper()
	v, err := res.Vector()
	require.NoError(t, err)
	out := make(map[string]float64, len(v))
	for _, s := range v {
		out[s.Metric.Get("route")] = s.F
	}

	return out
}

func gaugeStore(t *testing.T) *storage.Storage {
	t.Helper()
	s, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	writeSeries(t, s, "http_requests", metric.KindGauge, map[string][]smpl{
		"/a": {{1000, 1}, {1010, 2}, {1020, 3}},
		"/b": {{1000, 10}, {1010, 20}},
	})

	return s
}

func TestQueryInstantSelector(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)
	res := instant(t, s, "http_requests", 1020)

	assert.Equal(t, map[string]float64{"/a": 3, "/b": 20}, vectorByRoute(t, res),
		"instant vector takes the latest sample within lookback")

	v, _ := res.Vector()
	require.NotEmpty(t, v)
	assert.Equal(t, "http_requests", v[0].Metric.Get("__name__"))
	assert.Equal(t, "svc", v[0].Metric.Get("job"))
}

func TestQueryLabelMatchers(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)

	assert.Equal(t, map[string]float64{"/a": 3}, vectorByRoute(t, instant(t, s, `http_requests{route="/a"}`, 1020)))
	// Negative matcher must match the other series and not wrongly drop absent-label series.
	assert.Equal(t, map[string]float64{"/b": 20}, vectorByRoute(t, instant(t, s, `http_requests{route!="/a"}`, 1020)))
	assert.Equal(t, map[string]float64{"/a": 3}, vectorByRoute(t, instant(t, s, `http_requests{route=~"/a"}`, 1020)))
}

func TestQueryAggregation(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)

	total, err := instant(t, s, "sum(http_requests)", 1020).Vector()
	require.NoError(t, err)
	require.Len(t, total, 1)
	assert.Equal(t, 0, total[0].Metric.Len(), "sum() drops all labels")
	assert.InDelta(t, 23.0, total[0].F, 1e-9)

	assert.Equal(t, map[string]float64{"/a": 3, "/b": 20}, vectorByRoute(t, instant(t, s, "sum by (route) (http_requests)", 1020)))
}

func TestQueryScalarArithmetic(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)
	got := vectorByRoute(t, instant(t, s, `http_requests{route="/a"} * 2 + 1`, 1020))
	assert.InDelta(t, 7.0, got["/a"], 1e-9)

	sc, err := instant(t, s, "2 + 3 * 4", 1000).Scalar()
	require.NoError(t, err)
	assert.InDelta(t, 14.0, sc.V, 1e-9)
}

func TestQueryRate(t *testing.T) {
	t.Parallel()

	s, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// A cumulative counter increasing by 10 every 10s ⇒ a rate of 1/s.
	writeSeries(t, s, "reqs_total", metric.KindSum, map[string][]smpl{
		"/a": {{1000, 0}, {1010, 10}, {1020, 20}, {1030, 30}, {1040, 40}, {1050, 50}, {1060, 60}},
	})

	v, err := instant(t, s, "rate(reqs_total[1m])", 1060).Vector()
	require.NoError(t, err)
	require.Len(t, v, 1)
	assert.InDelta(t, 1.0, v[0].F, 0.05, "≈1 request/sec")
}

func TestQueryRange(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)
	pq, err := promEngine().NewRangeQuery(context.Background(), queryable(s), nil,
		`http_requests{route="/a"}`, time.Unix(0, 1000*sec), time.Unix(0, 1020*sec), 10*time.Second)
	require.NoError(t, err)
	t.Cleanup(pq.Close)

	res := pq.Exec(context.Background())
	require.NoError(t, res.Err)
	m, err := res.Matrix()
	require.NoError(t, err)
	require.Len(t, m, 1)

	vals := make([]float64, len(m[0].Floats))
	for i, p := range m[0].Floats {
		vals[i] = p.F
	}
	assert.Equal(t, []float64{1, 2, 3}, vals)
}

func TestQueryEmptyTenant(t *testing.T) {
	t.Parallel()

	s, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	v, err := instant(t, s, "http_requests", 1000).Vector()
	require.NoError(t, err)
	assert.Empty(t, v, "unknown tenant ⇒ empty fetcher ⇒ empty result")
}

func TestQueryParseError(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)
	_, err := promEngine().NewInstantQuery(context.Background(), queryable(s), nil, "http_requests{", time.Unix(0, 1000*sec))
	require.Error(t, err)
}
