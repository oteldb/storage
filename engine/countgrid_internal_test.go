package engine

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestGridPresence(t *testing.T) {
	t.Parallel()
	for _, grid := range []fetch.WindowGrid{
		{Start: 0, Step: 5, Width: 7, NumSteps: 130},
		{Start: -30, Step: 7, Width: 3, NumSteps: 20},
		{Start: math.MinInt64 + 5, Step: 3, Width: 10, NumSteps: 4},
	} {
		p, err := newGridPresence(grid)
		require.NoError(t, err)
		want := make([]bool, grid.NumSteps)
		for _, ts := range []int64{math.MinInt64, -30, -23, 0, 1, 5, 315, 320, 321, 640, math.MaxInt64} {
			p.sample(ts)
			for i := range want {
				end := grid.Start + int64(i)*grid.Step
				want[i] = want[i] || ts <= end && (end < math.MinInt64+grid.Width || ts > end-grid.Width)
			}
		}
		got := make([]bool, len(want))
		prefix := []gridSpan{{lo: 999, hi: 1000}}
		for _, span := range p.spans(prefix)[1:] {
			for i := span.lo; i < span.hi; i++ {
				got[i] = true
			}
		}
		require.Equal(t, want, got)
		p.reset()
		require.Zero(t, p.count)
		require.Empty(t, p.spans(nil))
	}
}

func TestCountGridInvalid(t *testing.T) {
	t.Parallel()
	for _, grid := range []fetch.WindowGrid{
		{}, {Step: -1, Width: 1, NumSteps: 1}, {Step: 1, Width: 0, NumSteps: 1},
		{Start: math.MaxInt64, Step: 1, Width: 1, NumSteps: 2},
		{Step: math.MaxInt64, Width: 1, NumSteps: 3},
	} {
		_, err := newGridPresence(grid)
		require.Error(t, err)
	}
}

func TestCountGridMatchesWindows(t *testing.T) {
	t.Parallel()
	for _, cache := range []int64{0, 1 << 20} {
		e := New(Config{Backend: backend.Memory(), Prefix: "count-grid", MetricBlockRows: 4,
			DecodeCacheBytes: cache, DecodeBudget: NewDecodeBudget(1 << 20)})
		series := []signal.Series{apiSeries(), {Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("other"))})},
			{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("id"), Value: signal.IntValue(2)})}}
		for generation := range 3 {
			for i, s := range series {
				for _, ts := range []int64{40, 0, 10, 10, 60} {
					_, err := e.Append(s, ts+int64(generation*30+i), float64(ts))
					require.NoError(t, err)
				}
			}
			if generation < 2 {
				require.NoError(t, e.Flush(t.Context()))
			}
		}
		for _, grid := range []fetch.WindowGrid{
			{Start: 0, Step: 5, Width: 20, NumSteps: 30},
			{Start: 0, Step: 5, Width: 3, NumSteps: 130},
			{Start: 30, Step: 5, Width: 100, NumSteps: 5},
		} {
			for _, label := range [][]byte{nil, []byte("job")} {
				req := fetch.Request{Start: 9, End: 130}
				got, err := e.CountGrid(t.Context(), req, grid, label)
				require.NoError(t, err)
				want := make(map[string][]int)
				for step := range grid.NumSteps {
					end := grid.Start + int64(step)*grid.Step
					window := fetch.Request{Start: max(req.Start, end-grid.Width+1), End: min(req.End, end)}
					if window.Start > window.End {
						continue
					}
					counts, err := e.CountBy(t.Context(), window, label)
					require.NoError(t, err)
					if label == nil {
						n, err := e.Count(t.Context(), window)
						require.NoError(t, err)
						counts = map[string]int{"": n}
					}
					for group, count := range counts {
						if count == 0 {
							continue
						}
						if want[group] == nil {
							want[group] = make([]int, grid.NumSteps)
						}
						want[group][step] = count
					}
				}
				require.Equal(t, want, got)
				require.Zero(t, e.budget.inFlight())
			}
		}
	}
}

func TestCountGridTimestampOnly(t *testing.T) {
	t.Parallel()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	counter := backendtest.NewSizedByteCounter(files)
	e := New(Config{Backend: counter, Prefix: "timestamp-count", MetricBlockRows: 4,
		DecodeCacheBytes: 1 << 20, DecodeBudget: NewDecodeBudget(1 << 20)})
	for i := range 32 {
		_, err := e.Append(apiSeries(), int64(i), float64(i*i))
		require.NoError(t, err)
	}
	require.NoError(t, e.Flush(t.Context()))
	require.Len(t, e.parts, 1)
	part := e.parts[0]
	require.NoError(t, files.Delete(t.Context(), part.prefix+"/c/2"))
	counter.Reset()
	grid := fetch.WindowGrid{Start: 4, Step: 4, Width: 3, NumSteps: 5}
	got, err := e.CountGrid(t.Context(), fetch.Request{Start: 0, End: 40}, grid, nil)
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"": {1, 1, 1, 1, 1}}, got)
	require.Contains(t, counter.Report(), "/c/1", "edge windows inspect timestamps")
	require.NotContains(t, counter.Report(), "/c/2", "no value-column reads")
	require.Zero(t, e.budget.inFlight())
	e.blockCache.evictPrefix(part.prefix)
	require.NoError(t, files.Delete(t.Context(), part.prefix+"/c/1"))
	_, err = e.CountGrid(t.Context(), fetch.Request{Start: 0, End: 40}, grid, nil)
	require.Error(t, err, "unresolved windows still require their timestamp data")
	require.Zero(t, e.budget.inFlight())
	require.Zero(t, part.refs.Load())
}

func TestCountGridCommonWindowProof(t *testing.T) {
	t.Parallel()
	counter := backendtest.NewSizedByteCounter(backend.Memory())
	e := New(Config{Backend: counter, Prefix: "proof-count", MetricBlockRows: 4, DecodeBudget: NewDecodeBudget(1 << 20)})
	_, err := e.Append(apiSeries(), 50, 1)
	require.NoError(t, err)
	_, err = e.Append(apiSeries(), 51, 2)
	require.NoError(t, err)
	require.NoError(t, e.Flush(t.Context()))
	part := e.parts[0]
	require.NoError(t, counter.Delete(t.Context(), part.prefix+"/c/1"))
	require.NoError(t, counter.Delete(t.Context(), part.prefix+"/c/2"))
	counter.Reset()
	got, err := e.CountGrid(t.Context(), fetch.Request{Start: 0, End: 200},
		fetch.WindowGrid{Start: 60, Step: 10, Width: 100, NumSteps: 5}, []byte("job"))
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"api": {1, 1, 1, 1, 1}}, got)
	require.NotContains(t, counter.Report(), "/c/")
	require.Zero(t, e.budget.inFlight())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = e.CountGrid(ctx, fetch.Request{}, fetch.WindowGrid{Step: 1, Width: 1, NumSteps: 1}, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func FuzzGridPresence(f *testing.F) {
	f.Add(int64(-30), uint16(130), uint16(5), uint16(7), []byte{0, 64, 65, 128, 255})
	f.Add(int64(0), uint16(5), uint16(15), uint16(300), []byte{1, 1, 255})
	f.Fuzz(func(t *testing.T, start int64, steps, step, width uint16, points []byte) {
		grid := fetch.WindowGrid{Start: start % 10000, NumSteps: int(steps%512) + 1, Step: int64(step%100) + 1, Width: int64(width%1000) + 1}
		p, err := newGridPresence(grid)
		require.NoError(t, err)
		want := make([]bool, grid.NumSteps)
		for _, point := range points {
			ts := grid.Start + int64(int8(point))*int64(step%100)
			p.sample(ts)
			for i := range want {
				end := grid.Start + int64(i)*grid.Step
				want[i] = want[i] || ts > end-grid.Width && ts <= end
			}
		}
		got := make([]bool, grid.NumSteps)
		count := 0
		for _, span := range p.spans(nil) {
			for i := span.lo; i < span.hi; i++ {
				got[i] = true
				count++
			}
		}
		require.Equal(t, want, got)
		require.Equal(t, count, p.count)
	})
}

func FuzzCountGridAgainstRows(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 20, 30, 40, 50, 128, 255}, uint16(40), uint16(15))
	f.Add([]byte{10, 10, 10, 90}, uint16(130), uint16(3))
	f.Fuzz(func(t *testing.T, points []byte, steps, width uint16) {
		points = points[:min(len(points), 32)]
		e := New(Config{Backend: backend.Memory(), Prefix: "grid-fuzz", MetricBlockRows: 4,
			DecodeCacheBytes: 1 << 16, DecodeBudget: NewDecodeBudget(1 << 20)})
		series := make([]signal.Series, 5)
		groupNames := []string{"", "a", "a", "42", "42"}
		for i := range series {
			attrs := []signal.KeyValue{{Key: []byte("id"), Value: signal.IntValue(int64(i))}}
			if i != 0 {
				value := signal.StringValue([]byte(groupNames[i]))
				if i == 3 {
					value = signal.IntValue(42)
				}
				attrs = append(attrs, signal.KeyValue{Key: []byte("job"), Value: value})
			}
			series[i] = signal.Series{Attributes: signal.NewAttributes(attrs...)}
		}
		for i, point := range points {
			id := int(point % 5)
			_, err := e.Append(series[id], int64(int8(point)), float64(point))
			require.NoError(t, err)
			buf := e.head.samples[series[id].Hash()]
			if buf.sf == nil {
				buf.sf = make([]float64, len(buf.ts))
			}
			buf.sf[len(buf.ts)-1] = 3
			if i%8 == 7 {
				require.NoError(t, e.Flush(t.Context()))
			}
		}
		if len(points) > 3 {
			e.flushing, _, _ = e.head.detach()
		}
		grid := fetch.WindowGrid{Start: -50, Step: 7, Width: int64(width%80) + 1, NumSteps: int(steps%130) + 1}
		req := fetch.Request{Start: -100, End: 100, Matchers: []fetch.Matcher{{Name: []byte("id"),
			Match: func(v signal.Value) bool { return v.Kind() == signal.KindInt && v.Int() != 1 }}}}
		got, err := e.CountGrid(t.Context(), req, grid, []byte("job"))
		require.NoError(t, err)
		want := map[string][]int{}
		for step := range grid.NumSteps {
			end := grid.Start + int64(step)*grid.Step
			present := [5]bool{}
			for _, point := range points {
				id, ts := int(point%5), int64(int8(point))
				if id != 1 && ts >= req.Start && ts <= req.End && ts > end-grid.Width && ts <= end {
					present[id] = true
				}
			}
			for id, exists := range present {
				if !exists {
					continue
				}
				key := groupNames[id]
				if want[key] == nil {
					want[key] = make([]int, grid.NumSteps)
				}
				want[key][step]++
			}
		}
		require.Len(t, got, len(want))
		for key, counts := range want {
			require.Equal(t, counts, got[key])
		}
		require.Zero(t, e.budget.inFlight())
	})
}
