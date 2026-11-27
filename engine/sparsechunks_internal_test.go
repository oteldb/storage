package engine

import (
	"bytes"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func sparseChunkFixture(n int) []signal.Series {
	out := make([]signal.Series, n)
	for i := range out {
		out[i] = signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{
			Key: []byte("host"), Value: signal.StringValue([]byte(strconv.Itoa(i))),
		})}
	}
	slices.SortFunc(out, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	return out
}

func TestSparseMetricChunks(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	files, err := backendfile.New(t.TempDir())
	require.NoError(t, err)
	cfg := Config{
		Backend: files, Prefix: "sparse", DiskSeries: true, SeparateSeries: true,
		MetricSeriesChunks: true, MetricSparseChunks: true,
		DecodeCacheBytes: 1 << 20, DecodeBudget: NewDecodeBudget(8 << 20),
	}
	e := New(cfg)
	const target, dense = 4, 35
	series := sparseChunkFixture(67)
	for i, s := range series {
		ts := []int64{int64(100 + i%31)}
		values := []float64{float64(i + 1)}
		weights := []float64{float64(1 + i%3)}
		if i == dense {
			ts, values, weights = []int64{900, 901, 902, 903}, []float64{1, 2, 3, 4}, []float64{1, 2, 3, 4}
		}
		ids := make([]signal.SeriesID, len(ts))
		for k := range ids {
			ids[k] = s.Hash()
		}
		result, err := e.AppendBatch(ids, ts, values, weights, func(int) signal.Series { return s }, AppendLimits{})
		require.NoError(t, err)
		require.Equal(t, len(ts), result.Accepted)
	}
	require.NoError(t, e.Flush(ctx))
	require.Len(t, e.parts, 1)

	// Timestamp, value and weight columns agree on the shared and dedicated ranges.
	p := e.parts[0]
	require.True(t, p.sharedChunks)
	for _, name := range []string{colTs, colValue, colSF} {
		desc, ok := p.reader.ColumnDescByName(name)
		require.True(t, ok)
		require.True(t, desc.SharedRows)
		c, err := p.reader.Column(ctx, name)
		require.NoError(t, err)
		d, err := c.BlockDecoder()
		require.NoError(t, err)
		var lengths []int
		for i := range d.NumBlocks() {
			lo, hi := d.BlockSpan(i)
			lengths = append(lengths, hi-lo)
		}
		require.Equal(t, []int{32, 3, 4, 31}, lengths)
	}

	// Reopen with packing disabled. The part identifies the read layout.
	require.NoError(t, e.Close(ctx))
	cfg.MetricSparseChunks = false
	e = New(cfg)
	require.NoError(t, e.LoadParts(ctx))
	t.Cleanup(func() { require.NoError(t, e.Close(ctx)) })
	req := fetch.Request{Start: 100, End: 1150, Matchers: []fetch.Matcher{{
		Name: []byte("host"), Match: func(v signal.Value) bool {
			return bytes.Equal(v.Str(), series[target].Attributes[0].Value.Str())
		},
	}}}
	points := func() *fetch.Batch {
		it, err := e.Fetch(ctx, req)
		require.NoError(t, err)
		all, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		require.Len(t, all, 1)
		return all[0]
	}
	got := points()
	require.Equal(t, []int64{104}, got.Timestamps)
	require.Equal(t, []float64{5}, got.Values)
	require.InDelta(t, 2, got.ScaleFactor(0), 0)

	n, err := e.Count(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	neighborOnly := req
	neighborOnly.Start, neighborOnly.End = 105, 105
	n, err = e.Count(ctx, neighborOnly)
	require.NoError(t, err)
	require.Zero(t, n, "neighbor rows do not keep the selected series live")
	grid := fetch.WindowGrid{Start: 103, Step: 1, Width: 1, NumSteps: 3}
	counts, err := e.CountGrid(ctx, req, grid, nil)
	require.NoError(t, err)
	require.Equal(t, map[string][]int{"": {0, 1, 0}}, counts)
	require.Zero(t, e.budget.inFlight())

	// Force a retention merge across the packed part and a newer independent part.
	_, err = e.Append(series[target], 1100, 7)
	require.NoError(t, err)
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.MergeWith(ctx, MergeOptions{RetainFrom: 500, Force: true}))
	got = points()
	require.Equal(t, []int64{1100}, got.Timestamps)
	require.Equal(t, []float64{7}, got.Values)
}

func TestSparseChunkMergeSharedFrames(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	be := backend.Memory()
	e := New(Config{Backend: be, MetricSeriesChunks: true, MetricSparseChunks: true})
	t.Cleanup(func() { require.NoError(t, e.Close(ctx)) })
	series := sparseChunkFixture(2 * sparseMetricChunkRows)
	for pass := range 2 {
		for i, s := range series {
			_, err := e.Append(s, int64(pass*100+i), float64(pass*100+i))
			require.NoError(t, err)
		}
		require.NoError(t, e.Flush(ctx))
	}
	require.Len(t, e.parts, 2)
	for _, p := range e.parts {
		require.True(t, p.sharedChunks)
		_, _, eligible := encodedColumnDescs(p)
		require.True(t, eligible)
	}

	require.NoError(t, e.MergeWith(ctx, MergeOptions{Force: true}))
	require.Len(t, e.parts, 1)
	require.False(t, e.parts[0].sharedChunks)
	it, err := e.Fetch(ctx, fetch.Request{Start: 0, End: 200})
	require.NoError(t, err)
	all, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, all, len(series))
	for i, batch := range all {
		require.Equal(t, series[i].Hash(), batch.ID)
		require.Equal(t, []int64{int64(i), int64(100 + i)}, batch.Timestamps)
		require.Equal(t, []float64{float64(i), float64(100 + i)}, batch.Values)
	}
}

func BenchmarkSparseMetricChunkFlush(b *testing.B) {
	points := sparseChunkFixture(4 << 10)
	for _, tc := range []struct {
		name   string
		shared bool
	}{{"independent", false}, {"shared", true}} {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(points) * 16))
			b.ReportAllocs()
			var numericBytes int64
			for b.Loop() {
				b.StopTimer()
				e := New(Config{Backend: backend.Memory(), MetricSeriesChunks: true, MetricSparseChunks: tc.shared})
				for i := range points {
					_, err := e.Append(points[i], 1700000000000000000+int64(i%500)*1_000_000, float64(i%31))
					require.NoError(b, err)
				}
				b.StartTimer()
				require.NoError(b, e.Flush(b.Context()))
				b.StopTimer()
				for _, p := range e.parts {
					for _, name := range []string{colTs, colValue} {
						d, ok := p.reader.ColumnDescByName(name)
						require.True(b, ok)
						numericBytes += d.Bytes
					}
				}
				require.NoError(b, e.Close(b.Context()))
				b.StartTimer()
			}
			b.ReportMetric(float64(numericBytes)/float64(b.N*len(points)), "numeric-B/point")
		})
	}
}
