package obs_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/oteldb/storage/internal/obs"
)

// TestWriteBytesRecord pins the write-amplification pair: what flushes write and what merges read
// and rewrite, which a part count alone never shows.
func TestWriteBytesRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	o, err := obs.New(obs.Config{MeterProvider: mp})
	require.NoError(t, err)

	o.Flush.Record(ctx, "metric", time.Second, 1_000, 4<<20)
	o.Flush.Record(ctx, "metric", time.Second, 500, 2<<20)
	o.Merge.Record(ctx, "metric", 2*time.Second, 4, 6<<20, 5<<20)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	metricSig := map[string]string{"signal": "metric"}
	assert.Equal(t, int64(6<<20), counterSum(t, rm, "storage.flush.bytes", metricSig))
	assert.Equal(t, int64(6<<20), counterSum(t, rm, "storage.merge.bytes_in", metricSig))
	assert.Equal(t, int64(5<<20), counterSum(t, rm, "storage.merge.bytes_out", metricSig))
}

// TestRPCAttemptResult pins the result attribute: without it a retry counter reports reactions to
// failures the attempt counter never admits to, so no error rate exists.
func TestRPCAttemptResult(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	o, err := obs.New(obs.Config{MeterProvider: mp})
	require.NoError(t, err)

	o.RPC.Attempt(ctx, "read", "ok")
	o.RPC.Attempt(ctx, "read", "ok")
	o.RPC.Attempt(ctx, "read", "timeout")
	o.RPC.Attempt(ctx, "write", "error")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	assert.Equal(t, int64(2),
		counterSum(t, rm, "storage.rpc.attempts", map[string]string{"op": "read", "result": "ok"}))
	assert.Equal(t, int64(1),
		counterSum(t, rm, "storage.rpc.attempts", map[string]string{"op": "read", "result": "timeout"}))
	assert.Equal(t, int64(1),
		counterSum(t, rm, "storage.rpc.attempts", map[string]string{"op": "write", "result": "error"}))
}
