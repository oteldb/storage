package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/oteldb/storage/signal"
)

func catchupBacklog(b *testing.B, buckets int) (*Storage, int64) {
	b.Helper()
	ctx := context.Background()
	s, err := InMemory(WithMergeMemory(-1), WithMaintenanceConcurrency(1))
	if err != nil {
		b.Fatal(err)
	}
	const rows = 128
	for day := range buckets {
		for part := range 2 {
			ts, values := make([]int64, rows), make([]float64, rows)
			records := make([][3]any, rows)
			for row := range rows {
				ts[row] = int64(day)*int64(24*time.Hour) + int64(part*rows+row+1)
				values[row] = float64(row)
				records[row] = [3]any{int(ts[row]), 9, fmt.Sprintf("%s-%d", strings.Repeat("x", 512), row)}
			}
			if _, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", ts, values)); err != nil {
				b.Fatal(err)
			}
			if _, err := s.WriteLogs(ctx, logBatch("api", records...)); err != nil {
				b.Fatal(err)
			}
			if err := s.Admin().Flush(ctx, "default", signal.Metric); err != nil {
				b.Fatal(err)
			}
			if err := s.Admin().Flush(ctx, "default", signal.Log); err != nil {
				b.Fatal(err)
			}
		}
	}
	stats, err := s.EfficiencyStats(ctx)
	if err != nil {
		b.Fatal(err)
	}
	var logical int64
	for _, tenant := range stats {
		for _, sig := range tenant.Signals {
			logical += sig.LogicalBytes
		}
	}
	return s, logical
}

// BenchmarkMaintenanceDrain compares equal backlog work without charging the old timer's idle time.
func BenchmarkMaintenanceDrain(b *testing.B) {
	const buckets = 8
	for _, mode := range []string{"timed_cycles", "catchup"} {
		b.Run(mode, func(b *testing.B) {
			ctx := context.Background()
			var logical, cycles int64
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				s, bytes := catchupBacklog(b, buckets)
				logical = bytes
				b.StartTimer()
				s.maintain(ctx)
				if mode == "timed_cycles" {
					for range buckets - 1 {
						s.maintain(ctx)
					}
				} else {
					pending := s.mergeCatchupTasks(ctx)
					for len(pending) > 0 {
						pending = runMergeTasks(pending, s.maintenanceConcurrency())
					}
					s.recordPartShape(ctx)
				}
				b.StopTimer()
				stats := s.Inspect()
				cycles = stats.Maintenance.Cycles
				parts := 0
				for i := range stats.Tenants[0].Signals {
					parts += stats.Tenants[0].Signals[i].Parts
				}
				if parts != buckets*2 {
					b.Fatalf("backlog not drained: %d parts", parts)
				}
				if err := s.Close(ctx); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.SetBytes(logical)
			b.ReportMetric(float64(cycles), "cycles/op")
		})
	}
}
