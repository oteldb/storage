package pdataconv

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/signal"
)

func BenchmarkMetricsHash(b *testing.B) {
	for _, shape := range []struct {
		name          string
		count, nested int
	}{
		{"empty", 0, 0},
		{"small", 4, 0},
		{"threshold", 32, 0},
		{"fallback", 33, 0},
		{"large", 128, 0},
		{"nested", 8, 8},
		{"nested-fallback", 4, 33},
	} {
		attrs := hashAttributes(shape.count, shape.nested)
		m := pmetric.NewMetric()
		m.SetName("node_cpu_seconds_total")
		m.SetUnit("seconds")
		m.SetEmptyGauge()
		for _, mode := range []string{"cold", "warm"} {
			b.Run(shape.name+"/"+mode, func(b *testing.B) {
				var h metricsHash
				if mode == "warm" {
					h.setGroup(signal.Resource{}, signal.Scope{})
					h.setMetric(m)
					h.id(attrs)
				}
				b.ReportAllocs()
				for b.Loop() {
					if mode == "cold" {
						h = metricsHash{}
						h.setGroup(signal.Resource{}, signal.Scope{})
						h.setMetric(m)
					}
					h.id(attrs)
				}
			})
		}
	}
}
