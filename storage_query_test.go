package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

const sec = int64(1e9) // one second in nanoseconds

type smpl struct {
	tSec int64
	v    float64
}

// writeSeries ingests one metric (name, kind) whose data points span the given series,
// keyed by their `route` label value, each with second-resolution samples.
func writeSeries(t *testing.T, s *Storage, name string, kind metric.PointKind, byRoute map[string][]smpl) {
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

func promQL(t *testing.T, s *Storage, text string, atSec int64) query.Result {
	t.Helper()
	res, err := s.Query(context.Background(), "default", Query{Lang: LangPromQL, Text: text, Start: atSec * sec, End: atSec * sec})
	require.NoError(t, err)

	return res
}

// labelOf returns the value of label name in a result series.
func labelOf(s query.Series, name string) string {
	for _, l := range s.Metric {
		if l.Name == name {
			return l.Value
		}
	}

	return ""
}

// byRoute indexes a vector result's single-point series by their route label.
func byRouteValues(t *testing.T, res query.Result) map[string]float64 {
	t.Helper()
	require.Equal(t, query.ResultVector, res.Type)
	out := make(map[string]float64, len(res.Series))
	for _, s := range res.Series {
		require.Len(t, s.Points, 1)
		out[labelOf(s, "route")] = s.Points[0].V
	}

	return out
}

func gaugeStore(t *testing.T) *Storage {
	t.Helper()
	s, err := InMemory()
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
	res := promQL(t, s, "http_requests", 1020)

	got := byRouteValues(t, res)
	assert.Equal(t, map[string]float64{"/a": 3, "/b": 20}, got, "instant vector takes the latest sample within lookback")

	// The metric name is exposed as __name__; job/route are real labels.
	require.NotEmpty(t, res.Series)
	assert.Equal(t, "http_requests", labelOf(res.Series[0], "__name__"))
	assert.Equal(t, "svc", labelOf(res.Series[0], "job"))
}

func TestQueryLabelMatchers(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)

	eq := byRouteValues(t, promQL(t, s, `http_requests{route="/a"}`, 1020))
	assert.Equal(t, map[string]float64{"/a": 3}, eq)

	// Negative matcher must match the other series (and not wrongly drop absent-label series).
	neq := byRouteValues(t, promQL(t, s, `http_requests{route!="/a"}`, 1020))
	assert.Equal(t, map[string]float64{"/b": 20}, neq)

	re := byRouteValues(t, promQL(t, s, `http_requests{route=~"/a"}`, 1020))
	assert.Equal(t, map[string]float64{"/a": 3}, re)
}

func TestQueryAggregation(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)

	total := promQL(t, s, "sum(http_requests)", 1020)
	require.Equal(t, query.ResultVector, total.Type)
	require.Len(t, total.Series, 1)
	assert.Empty(t, total.Series[0].Metric, "sum() drops all labels")
	assert.InDelta(t, 23.0, total.Series[0].Points[0].V, 1e-9)

	byR := byRouteValues(t, promQL(t, s, "sum by (route) (http_requests)", 1020))
	assert.Equal(t, map[string]float64{"/a": 3, "/b": 20}, byR)
}

func TestQueryScalarArithmetic(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)
	got := byRouteValues(t, promQL(t, s, `http_requests{route="/a"} * 2 + 1`, 1020))
	assert.InDelta(t, 7.0, got["/a"], 1e-9)
}

func TestQueryRate(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// A cumulative counter increasing by 10 every 10s ⇒ a rate of 1/s.
	writeSeries(t, s, "reqs_total", metric.KindSum, map[string][]smpl{
		"/a": {{1000, 0}, {1010, 10}, {1020, 20}, {1030, 30}, {1040, 40}, {1050, 50}, {1060, 60}},
	})

	res := promQL(t, s, "rate(reqs_total[1m])", 1060)
	require.Equal(t, query.ResultVector, res.Type)
	require.Len(t, res.Series, 1)
	assert.InDelta(t, 1.0, res.Series[0].Points[0].V, 0.05, "≈1 request/sec")
}

func TestQueryRange(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)
	res, err := s.Query(context.Background(), "default", Query{
		Lang: LangPromQL, Text: `http_requests{route="/a"}`,
		Start: 1000 * sec, End: 1020 * sec, Step: 10 * sec,
	})
	require.NoError(t, err)
	require.Equal(t, query.ResultMatrix, res.Type)
	require.Len(t, res.Series, 1)

	vals := make([]float64, len(res.Series[0].Points))
	for i, p := range res.Series[0].Points {
		vals[i] = p.V
	}
	assert.Equal(t, []float64{1, 2, 3}, vals)
}

func TestQueryScalar(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)
	res := promQL(t, s, "2 + 3 * 4", 1000)
	require.Equal(t, query.ResultScalar, res.Type)
	assert.InDelta(t, 14.0, res.Scalar.V, 1e-9)
}

func TestQueryEmptyTenant(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	res := promQL(t, s, "http_requests", 1000)
	assert.Equal(t, query.ResultVector, res.Type)
	assert.Empty(t, res.Series)
}

func TestQueryUnsupportedLang(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	_, err = s.Query(context.Background(), "default", Query{Lang: LangLogQL, Text: `{}`})
	require.ErrorIs(t, err, ErrNotImplemented)
}

func TestQueryParseError(t *testing.T) {
	t.Parallel()

	s := gaugeStore(t)
	_, err := s.Query(context.Background(), "default", Query{Lang: LangPromQL, Text: "http_requests{"})
	require.Error(t, err)
}
