package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/internal/memlimit"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestReturningIDHeadReadsAndReplays(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	e, fs := newFaultWALEngine(t, 10)
	e.cfg.Backend, e.cfg.Prefix = files, "returning"
	e.cfg.DiskSeries, e.cfg.AsyncPressureFlush, e.cfg.PressureGenerations = true, true, 2
	e.cfg.SeparateSeries, e.cfg.MetricSeriesChunks = true, true
	e.cfg.ResidentHeadBytes, e.cfg.HeadRotationBytes, e.cfg.FlushMemoryBytes = 1<<30, 256<<20, 64<<20
	pool := memlimit.NewPool(512 << 20)
	e.cfg.FlushAdmission, e.cfg.TryFlushAdmission = pool.Acquire, pool.TryAcquire
	e.head.disk = newDiskHeadState()
	point := func(value string) signal.KeyValue {
		return signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte(value))}
	}
	s := signal.Series{
		Resource:   signal.Resource{Attributes: signal.NewAttributes(point("resource"))},
		Attributes: signal.NewAttributes(point("point")),
	}
	_, err = e.Append(s, 100, 1)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))

	id := s.Hash()
	r, err := e.AppendBatch([]signal.SeriesID{id, id}, []int64{200, 201}, []float64{2, 3}, nil,
		func(int) signal.Series { return s }, AppendLimits{})
	require.NoError(t, err)
	require.Equal(t, 2, r.Accepted)
	require.Len(t, e.head.encodedRefs, 1)
	require.Equal(t, s.AppendHashInput(nil), e.head.series.View().EncodedDescriptor(int(e.head.encodedRefs[0])))
	require.Zero(t, e.head.sym.Len(), "returning attachments skip eager label interning")
	require.Equal(t, []int64{200, 201}, headTS(replayedHead(t, fs), s))

	for _, value := range []string{"resource", "point", "missing"} {
		matcher := fetch.EqualMatcher{Name: "host", Value: value}
		req := fetch.Request{Start: 200, End: 201, Matchers: []fetch.Matcher{
			{Name: []byte(matcher.Name), Match: matcher.Predicate(), Spec: &matcher},
		}}
		count, err := e.Count(t.Context(), req)
		require.NoError(t, err)
		it, err := e.Fetch(t.Context(), req)
		require.NoError(t, err)
		batches, err := fetch.Drain(t.Context(), it)
		require.NoError(t, err)
		if value == "missing" {
			require.Zero(t, count)
			require.Empty(t, batches)
			continue
		}
		require.Equal(t, 1, count)
		require.Len(t, batches, 1)
		require.Equal(t, []int64{200, 201}, batches[0].Timestamps)
	}
	require.Equal(t, 1, e.Cardinality(0).DistinctLabelNames)
	require.NoError(t, e.Flush(t.Context()))
	require.NoError(t, e.LoadParts(t.Context()))
	require.NoError(t, e.Close(t.Context()))
}
