package engine

import (
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/backendtest"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func BenchmarkCountGridFile(b *testing.B) {
	files, err := backendfile.New(b.TempDir())
	require.NoError(b, err)
	counter := backendtest.NewSizedByteCounter(files)
	e := New(Config{Backend: counter, Prefix: "count-grid-bench", MetricBlockRows: 1024, AggregateStats: true})
	const seriesCount = 16384
	series := make([]signal.Series, seriesCount)
	for i := range series {
		series[i] = signal.Series{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("node_cpu_seconds_total"))},
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte("host-" + strconv.Itoa(i/128)))},
			signal.KeyValue{Key: []byte("cpu"), Value: signal.StringValue([]byte(strconv.Itoa(i % 16)))},
			signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte(strconv.Itoa(i % 128 / 16)))},
		)}
	}
	for generation := range 16 {
		for i := range series {
			for _, offset := range []int{0, 15} {
				_, err := e.Append(series[i], int64(generation*30+offset)*int64(time.Second), float64(i+generation+offset))
				require.NoError(b, err)
			}
		}
		require.NoError(b, e.Flush(b.Context()))
	}
	spec := &fetch.EqualMatcher{Name: "instance", Value: "host-0"}
	req := fetch.Request{Start: 61 * int64(time.Second), End: 420 * int64(time.Second),
		Matchers: []fetch.Matcher{{Name: []byte("instance"), Spec: spec, Match: spec.Predicate()}}}
	grid := fetch.WindowGrid{Start: 360 * int64(time.Second), Step: 15 * int64(time.Second), Width: 300 * int64(time.Second), NumSteps: 5}
	fold := func() (map[string][]int, error) {
		named, err := e.AggregateWindowNamed(b.Context(), req, WindowSpec{Step: grid.Step, Window: grid.Width, Anchor: grid.Start})
		if err != nil {
			return nil, err
		}
		out := map[string][]int{}
		for i := range named {
			s := &named[i]
			value, _ := seriesLabelValue(s.Series, []byte("cpu"))
			key := string(value.AppendText(nil))
			for _, window := range s.Windows {
				if window.End < grid.Start || window.End > req.End || window.Rows == 0 {
					continue
				}
				if out[key] == nil {
					out[key] = make([]int, grid.NumSteps)
				}
				out[key][int((window.End-grid.Start)/grid.Step)]++
			}
		}
		return out, nil
	}
	count := func() (map[string][]int, error) { return e.CountGrid(b.Context(), req, grid, []byte("cpu")) }
	want, err := fold()
	require.NoError(b, err)
	got, err := count()
	require.NoError(b, err)
	require.True(b, reflect.DeepEqual(want, got))
	run := fold
	if os.Getenv("OTELDB_BENCH_COUNT_GRID") == "true" {
		run = count
	}
	counter.Reset()
	b.ReportAllocs()
	for b.Loop() {
		got, err := run()
		if err != nil || len(got) != 16 {
			b.Fatalf("count groups=%d: %v", len(got), err)
		}
		for _, values := range got {
			for _, n := range values {
				if n != 8 {
					b.Fatalf("count=%d want 8", n)
				}
			}
		}
	}
	b.ReportMetric(float64(counter.Bytes())/float64(b.N), "read-B/op")
}
