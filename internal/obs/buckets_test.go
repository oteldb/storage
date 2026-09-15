package obs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/internal/obs"
)

func TestDurationHistogramSecondsBuckets(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	o, err := obs.New(obs.Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	require.NoError(t, err)

	ctx := t.Context()
	o.Flush.Record(ctx, "log", 100*time.Millisecond, 10, 1)
	o.Flush.Record(ctx, "log", 3*time.Millisecond, 10, 1)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "storage.flush.duration" {
				continue
			}

			h, ok := md.Data.(metricdata.Histogram[float64])
			require.True(t, ok)
			require.Len(t, h.DataPoints, 1)

			dp := h.DataPoints[0]
			filled := map[float64]uint64{}
			for i, c := range dp.BucketCounts {
				if c > 0 {
					require.Less(t, i, len(dp.Bounds), "observation overflowed every bound")
					filled[dp.Bounds[i]] = c
				}
			}

			require.Equal(t, map[float64]uint64{0.005: 1, 0.1: 1}, filled)

			return
		}
	}

	t.Fatal("storage.flush.duration not recorded")
}
