package storage

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

func TestMetricFlushMemoryShare(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		budget int64
		want   int64
	}{
		{name: "bounded", budget: 64 << 20, want: 16 << 20},
		{name: "minimum", budget: 1, want: 1},
		{name: "unbounded", budget: -1, want: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s, err := InMemory(WithMergeMemory(tt.budget))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
			require.Equal(t, tt.want, s.metricFlushMemoryBytes())
		})
	}
}

func TestMetricFlushProgressWhileMergeOwnsItsShare(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		be, err := file.New(t.TempDir())
		require.NoError(t, err)
		s, err := Open(t.Context(), Options{
			Backend: be, MergeMemoryBytes: 64 << 20, FlushInterval: -1, FlushThresholdBytes: -1,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close(context.Background())) }()

		var md metric.Metrics
		mt := md.AddResource().AddScope().AddMetric()
		mt.Name = []byte("flush_progress")
		for i := range 1 << 19 {
			p := mt.AddPoint()
			p.Ts, p.Value = int64(i), float64(i)
		}
		_, err = s.WriteMetrics(t.Context(), md)
		require.NoError(t, err)

		release, ok, err := s.admitMerge(t.Context(), s.mergeMemoryBytes(), false)
		require.NoError(t, err)
		require.True(t, ok)
		defer release()

		done := make(chan error, 1)
		go func() { done <- s.Admin().Flush(t.Context(), "default", signal.Metric) }()
		synctest.Wait()
		select {
		case err := <-done:
			require.NoError(t, err)
		default:
			t.Fatal("metric flush cannot use its reserved workspace while a merge runs")
		}
	})
}
