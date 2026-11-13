package engine

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

func poolTestSeries(i int) signal.Series {
	return signal.Series{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("template_test"))},
		signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte(strconv.Itoa(i)))},
		signal.KeyValue{Key: []byte("cpu"), Value: signal.IntValue(0)},
		signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte("idle"))},
	)}
}

func TestEngineTemplatePoolLifecycle(t *testing.T) {
	t.Parallel()
	pool := series.NewAttributeTemplates(8<<20, []byte("instance"))
	engines := []*Engine{
		New(Config{Backend: backend.Memory(), Prefix: "flat"}),
		New(Config{Backend: backend.Memory(), Prefix: "templates", SeriesTemplates: pool}),
	}
	for _, e := range engines {
		t.Cleanup(func() { require.NoError(t, e.Close(context.WithoutCancel(t.Context()))) })
		for i := range 512 {
			_, err := e.Append(poolTestSeries(i), 1, 1)
			require.NoError(t, err)
		}
		require.NoError(t, e.Flush(t.Context()))
		for i := range 256 {
			_, err := e.Append(poolTestSeries(i), 3, 2)
			require.NoError(t, err)
		}
		require.NoError(t, e.Flush(t.Context()))
		require.NoError(t, e.Merge(t.Context(), 2))
		_, err := e.PruneIdentitiesWith(t.Context(), PruneOptions{Force: true})
		require.NoError(t, err)
	}
	require.Equal(t, catalogFetchTimes(t, engines[0]), catalogFetchTimes(t, engines[1]))
	require.Less(t, engines[1].head.series.SizeBytes(), engines[0].head.series.SizeBytes())
	for _, e := range engines {
		require.NoError(t, e.Reset(t.Context()))
		for i := range 512 {
			_, err := e.Append(poolTestSeries(i), 4, 1)
			require.NoError(t, err)
		}
	}
	require.Equal(t, catalogFetchTimes(t, engines[0]), catalogFetchTimes(t, engines[1]))
	require.Less(t, engines[1].head.series.SizeBytes(), engines[0].head.series.SizeBytes())
}
