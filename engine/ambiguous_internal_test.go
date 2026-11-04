package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/query/fetch"
)

// TestReadTieAmbiguousIsReported: two parts hold representatives of disjoint samples at one bucket
// start, rolled with different Aggs, as nodes running different policies for one tenant write them.
// The read picks one cohort deterministically and says so: the batch carries the flag and the
// ambiguous-tie counter counts it.
func TestReadTieAmbiguousIsReported(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	step := 10 * int64(time.Second)
	count := MergeOptions{Downsample: countTiers(1<<62, time.Minute)}
	b := backend.Memory()

	e := reopenRollup(t, b, Config{})
	flushEvery(t, e, 0, 3*step, step, 1)
	require.NoError(t, e.MergeWith(ctx, count))

	first := e.parts[0].prefix
	// Restore a legacy snapshot with separately rolled representatives at one bucket start.
	_, err := e.Append(rollupSeries(), 0, 3)
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 2, e.PartCount())
	for _, p := range e.parts {
		if p.prefix != first {
			rewriteRollup(t, b, p.prefix, rollupMarker(count.Downsample))
		}
	}
	rewriteRollup(t, b, first, rollupMarker(sumTiers(1<<62, time.Minute)))

	reader := sdkmetric.NewManualReader()
	o, err := obs.New(obs.Config{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))})
	require.NoError(t, err)

	e = reopenRollup(t, b, Config{Obs: o})

	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62})
	require.NoError(t, err)
	got, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, got, 1)

	assert.Equal(t, []int64{0}, got[0].Timestamps)
	assert.True(t, got[0].AmbiguousRollup, "the batch says a cohort was left out")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	assert.Equal(t, int64(1), counterSum(rm, "storage.fetch.rollup_ambiguous_ties"))
}

func counterSum(rm metricdata.ResourceMetrics, name string) int64 {
	var n int64

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}

			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					n += dp.Value
				}
			}
		}
	}

	return n
}
