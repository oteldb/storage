package engine

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/backendtest"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	storagepromql "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
)

type gridOnlyHead struct {
	fetch.Fetcher

	counter fetch.GridCounter
}

func (f gridOnlyHead) CountGrid(ctx context.Context, r fetch.Request, g fetch.WindowGrid, group []byte) (map[string][]int, error) {
	return f.counter.CountGrid(ctx, r, g, group)
}

func TestDisjointInstantCountTimestampOnly(t *testing.T) {
	t.Parallel()
	children := make([]fetch.Fetcher, 0, 2)
	reports := make([]backendtest.SizedByteCounter, 0, 2)
	for _, name := range []string{"a", "b"} {
		files, err := backendfile.New(t.TempDir())
		require.NoError(t, err)
		counter := backendtest.NewSizedByteCounter(files)
		e := New(Config{Backend: counter, Prefix: "count-lane", MetricBlockRows: 4, DecodeCacheBytes: 1 << 20, DecodeBudget: NewDecodeBudget(1 << 20)})
		s := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte(name))})}
		for i := range 32 {
			_, err := e.Append(s, int64(i)*1000000, float64(i))
			require.NoError(t, err)
		}
		require.NoError(t, e.Flush(t.Context()))
		require.Len(t, e.parts, 1)
		require.NoError(t, files.Delete(t.Context(), e.parts[0].prefix+"/c/2"))
		counter.Reset()
		reports = append(reports, counter)
		children = append(children, gridOnlyHead{Fetcher: e, counter: e})
	}
	merged := fetch.MergeDisjoint(func() bool { return true }, children...)
	q, err := storagepromql.NewQueryable(merged, "default").Querier(0, 32)
	require.NoError(t, err)
	defer q.Close()
	count, ok := q.(interface {
		CountSeries(ctx context.Context, start, end int64, matchers ...*labels.Matcher) (uint64, error)
	})
	require.True(t, ok)
	n, err := count.CountSeries(t.Context(), 5, 11)
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
	grouped, ok := q.(interface {
		CountSeriesBy(ctx context.Context, start, end int64, label string, matchers ...*labels.Matcher) (map[string]uint64, error)
	})
	require.True(t, ok)
	groups, err := grouped.CountSeriesBy(t.Context(), 5, 11, "job")
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"a": 1, "b": 1}, groups)
	for _, counter := range reports {
		require.Contains(t, counter.Report(), "/c/1")
		require.NotContains(t, counter.Report(), "/c/2")
	}
}
