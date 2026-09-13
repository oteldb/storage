// Package obstest builds an [obs.Obs] backed by a manual metric reader, for tests that assert what
// a code path reported.
package obstest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/internal/obs"
)

// Metrics reads back the instruments of the handle [New] returned.
type Metrics struct {
	t      testing.TB
	reader *sdkmetric.ManualReader
}

// New returns a metered handle and its reader.
func New(tb testing.TB) (*obs.Obs, *Metrics) {
	tb.Helper()

	mp, m := Provider(tb)

	o, err := obs.New(obs.Config{MeterProvider: mp})
	require.NoError(tb, err)

	return o, m
}

// Provider returns a meter provider and its reader, for code that builds its own handle.
func Provider(tb testing.TB) (metric.MeterProvider, *Metrics) {
	tb.Helper()

	reader := sdkmetric.NewManualReader()

	return sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), &Metrics{t: tb, reader: reader}
}

// Counter returns the sum of int64 counter name over the data points carrying every attr (key,
// value pairs); zero when nothing was recorded.
func (m *Metrics) Counter(name string, attrs ...string) int64 {
	m.t.Helper()

	var total int64

	for _, md := range m.collect(name) {
		sum, ok := md.Data.(metricdata.Sum[int64])
		require.Truef(m.t, ok, "%s is not an int64 sum", name)

		for _, dp := range sum.DataPoints {
			if hasAttrs(dp.Attributes, attrs) {
				total += dp.Value
			}
		}
	}

	return total
}

// Gauge returns the int64 gauge name at the data point carrying every attr, and whether one was
// recorded.
func (m *Metrics) Gauge(name string, attrs ...string) (int64, bool) {
	m.t.Helper()

	for _, md := range m.collect(name) {
		g, ok := md.Data.(metricdata.Gauge[int64])
		require.Truef(m.t, ok, "%s is not an int64 gauge", name)

		for _, dp := range g.DataPoints {
			if hasAttrs(dp.Attributes, attrs) {
				return dp.Value, true
			}
		}
	}

	return 0, false
}

func (m *Metrics) collect(name string) []metricdata.Metrics {
	m.t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(m.t, m.reader.Collect(m.t.Context(), &rm))

	var out []metricdata.Metrics

	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name == name {
				out = append(out, md)
			}
		}
	}

	return out
}

func hasAttrs(set attribute.Set, attrs []string) bool {
	for i := 0; i+1 < len(attrs); i += 2 {
		v, ok := set.Value(attribute.Key(attrs[i]))
		if !ok || v.AsString() != attrs[i+1] {
			return false
		}
	}

	return true
}
