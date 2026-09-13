package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestFlushWritesScaledTimestamps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := New(Config{Backend: backend.Memory(), Prefix: "t/metrics"})

	for i := range 100 {
		_, err := e.Append(apiSeries(), 1_700_000_000_000_000_000+int64(i)*15_000_000_000, 1)
		require.NoError(t, err)
	}

	require.NoError(t, e.Flush(ctx))
	require.Equal(t, []string{"dodscaled"}, tsCodecs(ctx, t, e))
}

// TestLegacyDoDPartsMergeIntoScaled covers an upgrade: parts an earlier build wrote with
// [chunk.CodecDoD] stay readable, and a merge rewrites them with [chunk.CodecDoDScaled] without
// changing a sample.
func TestLegacyDoDPartsMergeIntoScaled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	be := backend.Memory()
	e := New(Config{Backend: be, Prefix: "t/metrics", MetricBlockRows: 128})
	e.tsCodec = chunk.CodecDoD

	series := apiSeries()

	const start = int64(1_700_000_000_000_000_000)

	want := make([]int64, 0, 1000)

	for batch := range 2 {
		for i := range 500 {
			ts := start + int64(batch*500+i)*1_000_000_000 + int64(i%3)*1_000_000 // ms-aligned, irregular
			if batch == 1 && i == 250 {
				ts += 7 // one ns-entropic sample, so the merged part mixes granule scales
			}

			_, err := e.Append(series, ts, float64(i))
			require.NoError(t, err)

			want = append(want, ts)
		}

		require.NoError(t, e.Flush(ctx))
	}

	require.Equal(t, []string{"dod", "dod"}, tsCodecs(ctx, t, e))
	require.Equal(t, want, fetchTimestamps(ctx, t, e))

	e.tsCodec = chunk.CodecDoDScaled
	require.NoError(t, e.Merge(ctx, 0))

	require.Equal(t, []string{"dodscaled"}, tsCodecs(ctx, t, e))
	require.Equal(t, want, fetchTimestamps(ctx, t, e))
}

func apiSeries() signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("api"))},
	)}
}

func tsCodecs(ctx context.Context, t *testing.T, e *Engine) []string {
	t.Helper()

	parts, err := e.PartsDetailed(ctx)
	require.NoError(t, err)

	var out []string

	for _, p := range parts {
		for _, c := range p.Columns {
			if c.Name == colTs {
				out = append(out, c.Codec)
			}
		}
	}

	return out
}

func fetchTimestamps(ctx context.Context, t *testing.T, e *Engine) []int64 {
	t.Helper()

	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 1 << 62})
	require.NoError(t, err)

	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	return batches[0].Timestamps
}
