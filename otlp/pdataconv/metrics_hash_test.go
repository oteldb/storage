package pdataconv

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

func TestMetricsHashDuplicateKeys(t *testing.T) {
	t.Parallel()
	for _, count := range []int{2, 32, 33} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			var attributes strings.Builder
			for i := range count {
				if i != 0 {
					attributes.WriteByte(',')
				}
				attributes.WriteString(`{"key":"__name__","value":{"stringValue":"` + strconv.Itoa(i) + `"}}`)
			}
			data := `{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"metric","gauge":{"dataPoints":[{"asInt":"1","attributes":[` + attributes.String() + `]}]}}]}]}]}`
			md, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics([]byte(data))
			require.NoError(t, err)
			m := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
			attrs := numberPoints(m).At(0).Attributes()
			require.Equal(t, count, attrs.Len())
			var h metricsHash
			h.setGroup(signal.Resource{}, signal.Scope{})
			h.setMetric(m)
			got := h.id(attrs)
			want := (metric.Identity{Name: []byte(m.Name()), Series: signal.Series{Attributes: convertMap(attrs)}}).ToSeries()
			require.Equal(t, want.AppendHashInput(nil), h.buf)
			require.Equal(t, want.Hash(), got)
		})
	}
}

//nolint:paralleltest // AllocsPerRun changes GOMAXPROCS and forbids parallel tests.
func TestMetricsHashAllocations(t *testing.T) {
	for _, shape := range []struct {
		name          string
		count, nested int
	}{
		{"empty", 0, 0},
		{"small", 4, 0},
		{"small-threshold", 8, 0},
		{"medium", 9, 0},
		{"threshold", 32, 0},
		{"nested", 8, 8},
		{"nested-threshold", 2, 32},
		{"fallback", 33, 0},
		{"nested-fallback", 4, 33},
	} {
		t.Run(shape.name, func(t *testing.T) {
			attrs := hashAttributes(shape.count, shape.nested)
			m := pmetric.NewMetric()
			m.SetName("metric")
			m.SetEmptyGauge()
			var h metricsHash
			h.setGroup(signal.Resource{}, signal.Scope{})
			h.setMetric(m)
			h.id(attrs)
			require.Zero(t, testing.AllocsPerRun(100, func() { h.id(attrs) }))
			if shape.count <= metricsHashStackThreshold && shape.nested <= metricsHashStackThreshold {
				require.Empty(t, h.entries[:cap(h.entries)])
			}
			for _, entry := range h.entries[:cap(h.entries)] {
				require.Equal(t, mapEntry{}, entry)
			}
		})
	}
}

func hashAttributes(count, nested int) pcommon.Map {
	attrs := pcommon.NewMap()
	for i := count - 1; i >= 0; i-- {
		key := "attribute-" + strconv.Itoa(i)
		if nested == 0 {
			attrs.PutStr(key, "value")
			continue
		}
		hashAttributes(nested, 0).CopyTo(attrs.PutEmptyMap(key))
	}
	return attrs
}
