package storage

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/otlp/pdataconv"
	"github.com/oteldb/storage/signal/metric"
)

func BenchmarkProjectedMetrics(b *testing.B) {
	for _, shape := range []struct {
		name            string
		metrics, points int
	}{
		{"tiny", 1, 1}, {"grouped", 1, 1024}, {"fragmented", 1024, 1},
	} {
		for _, workload := range []string{"cold", "warm", "churn"} {
			for _, mode := range []string{"adapter", "projected"} {
				b.Run(shape.name+"/"+workload+"/"+mode, func(b *testing.B) {
					benchmarkProjectedMetrics(b, shape.metrics, shape.points, workload, mode, false)
				})
			}
		}
	}
}

func benchmarkProjectedMetrics(b *testing.B, metrics, points int, workload, mode string, durable bool) {
	b.Helper()
	ctx := context.Background()
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	for mi := range metrics {
		m := sm.Metrics().AppendEmpty()
		m.SetName("metric-" + strconv.Itoa(mi))
		ps := m.SetEmptyGauge().DataPoints()
		for pi := range points {
			point := ps.AppendEmpty()
			point.SetTimestamp(1)
			point.SetIntValue(1)
			attrs := point.Attributes()
			attrs.PutStr("instance", "host-"+strconv.Itoa(pi))
			attrs.PutStr("job", "node-exporter")
			attrs.PutStr("mode", "idle")
			attrs.PutStr("cpu", "0")
		}
	}
	var native metric.Metrics
	var projector pdataconv.MetricsProjector
	var s *Storage
	open := func() {
		var err error
		if durable {
			dir := b.TempDir()
			be, openErr := file.New(filepath.Join(dir, "parts"))
			if openErr != nil {
				b.Fatal(openErr)
			}
			s, err = Open(ctx, Options{Backend: be, WALDir: filepath.Join(dir, "wal"), FlushInterval: int64(time.Hour), FlushThresholdBytes: -1})
		} else {
			s, err = Open(ctx, Options{Backend: backend.Memory(), Durability: DurabilityEphemeral})
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	closeStore := func() {
		if err := s.Close(ctx); err != nil {
			b.Fatal(err)
		}
	}
	write := func() {
		var err error
		if mode == "adapter" {
			native.Reset()
			pdataconv.AppendMetrics(&native, md)
			_, err = s.WriteMetrics(ctx, native)
		} else {
			if !projector.Prepare(md) {
				b.Fatal("ineligible fixture")
			}
			_, err = s.WriteProjectedMetrics(ctx, projector.Project)
			projector.Reset()
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	open()
	if workload != "cold" {
		write()
	}
	b.ReportAllocs()
	iteration := 0
	for b.Loop() {
		if workload == "cold" || (iteration > 0 && iteration%max(1, (1<<20)/(metrics*points)) == 0) {
			b.StopTimer()
			closeStore()
			open()
			if workload == "warm" {
				write()
			}
			b.StartTimer()
		}
		if workload == "churn" {
			b.StopTimer()
			sm.Scope().SetVersion(strconv.Itoa(iteration + 1))
			b.StartTimer()
		}
		// Timestamps advance on every request; input construction and clock updates are excluded.
		b.StopTimer()
		for mi := range sm.Metrics().Len() {
			ps := sm.Metrics().At(mi).Gauge().DataPoints()
			for pi := range ps.Len() {
				ps.At(pi).SetTimestamp(pcommon.Timestamp(iteration + 2))
			}
		}
		b.StartTimer()
		write()
		iteration++
	}
	closeStore()
	reportIngestMetrics(b, metrics*points)
}

// BenchmarkProjectedMetricsWAL includes native file WAL writes with the default WALSyncNone policy.
func BenchmarkProjectedMetricsWAL(b *testing.B) {
	for _, workload := range []string{"cold", "warm", "churn"} {
		for _, mode := range []string{"adapter", "projected"} {
			b.Run(workload+"/"+mode, func(b *testing.B) { benchmarkProjectedMetrics(b, 10000, 1, workload, mode, true) })
		}
	}
}
