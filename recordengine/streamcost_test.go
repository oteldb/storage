package recordengine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

func costOf(t *testing.T, cs []recordengine.StreamCostStat, key string) recordengine.StreamCostStat {
	t.Helper()

	for _, c := range cs {
		if c.Key == key {
			return c
		}
	}

	t.Fatalf("group %q not found in %+v", key, cs)

	return recordengine.StreamCostStat{}
}

func columnOf(t *testing.T, c recordengine.StreamCostStat, name string) recordengine.ColumnCostStat {
	t.Helper()

	for _, cc := range c.Columns {
		if cc.Name == name {
			return cc
		}
	}

	t.Fatalf("column %q not found in %+v", name, c.Columns)

	return recordengine.ColumnCostStat{}
}

// templated is one klog-shaped line: the same template every time, with an embedded timestamp and
// counter. Distinct bodies, one template.
func templated(i int) string {
	return fmt.Sprintf("I0815 10:%02d:%02d.%06d       1 volume.go:%d] reconciling pvc-%d", i/60%60, i%60, i*37%1e6, 120+i%3, i)
}

// distinctBody is genuinely high-entropy text with no digits, so collapsing digits changes nothing.
func distinctBody(i int) string {
	const alpha = "abcdefghijklmnopqrstuvwxyz"

	return fmt.Sprintf("request %c%c%c%c completed for tenant %c%c",
		alpha[i%26], alpha[i/26%26], alpha[i/7%26], alpha[i/11%26], alpha[i/13%26], alpha[i/17%26])
}

// TestStreamCostSeparatesTemplatedFromEntropic is the diagnostic the whole surface exists for: two
// services with the same row count and near-identical distinct body counts, where only the digit-
// collapsed estimate tells the mis-parsed one from the genuinely high-entropy one.
func TestStreamCostSeparatesTemplatedFromEntropic(t *testing.T) {
	t.Parallel()

	const rows = 2000

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	tmpl := make([]rrec, 0, rows)
	rand := make([]rrec, 0, rows)

	for i := range rows {
		tmpl = append(tmpl, rrec{ts: int64(i), body: templated(i)})
		rand = append(rand, rrec{ts: int64(i), body: distinctBody(i)})
	}

	ingest(t, e, mkBatch("klog", tmpl...))
	ingest(t, e, mkBatch("api", rand...))
	require.NoError(t, e.Flush(ctx))

	cs, err := e.StreamCost(ctx, recordengine.StreamCostOptions{GroupBy: "service.name"})
	require.NoError(t, err)
	require.Len(t, cs, 2)

	klog := costOf(t, cs, "klog")
	require.True(t, klog.DistinctEstimated)
	assert.Equal(t, int64(rows), klog.Rows)
	assert.Equal(t, 1, klog.Streams)

	body := columnOf(t, klog, "body")
	assert.InEpsilon(t, float64(rows), float64(body.Distinct), 0.1, "every line differs")
	assert.LessOrEqual(t, body.DistinctNormalized, int64(4),
		"collapsing digits must leave a handful of templates, got %d", body.DistinctNormalized)

	api := columnOf(t, costOf(t, cs, "api"), "body")
	assert.Positive(t, api.Distinct)
	assert.InEpsilon(t, float64(api.Distinct), float64(api.DistinctNormalized), 0.1,
		"digit-free text is unaffected by collapsing")
}

// TestStreamCostBytesAndDisk: the byte attribution covers the accounted columns, sums to no more
// than the part holds, and puts the fat stream on top.
func TestStreamCostBytesAndDisk(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	fat := make([]rrec, 0, 200)
	thin := make([]rrec, 0, 200)

	for i := range 200 {
		fat = append(fat, rrec{ts: int64(i), body: fmt.Sprintf("%s-%d", string(make([]byte, 300)), i)})
		thin = append(thin, rrec{ts: int64(i), body: "ok"})
	}

	ingest(t, e, mkBatch("fat", fat...))
	ingest(t, e, mkBatch("thin", thin...))
	require.NoError(t, e.Flush(ctx))

	// A second part, so the pass folds groups across parts.
	ingest(t, e, mkBatch("fat", rrec{ts: 1000, body: "tail"}))
	require.NoError(t, e.Flush(ctx))

	cs, err := e.StreamCost(ctx, recordengine.StreamCostOptions{GroupBy: "service.name"})
	require.NoError(t, err)
	require.Len(t, cs, 2)
	assert.Equal(t, "fat", cs[0].Key, "sorted by RawBytes descending")
	assert.Equal(t, int64(201), cs[0].Rows, "rows fold across parts")

	var (
		rawTotal, diskTotal int64
		partTotal           int64
	)

	for _, c := range cs {
		assert.Positive(t, c.RawBytes)
		assert.Positive(t, c.DiskBytes)

		var colRaw, colDisk int64
		for _, cc := range c.Columns {
			colRaw += cc.RawBytes
			colDisk += cc.DiskBytes
		}

		assert.Equal(t, c.RawBytes, colRaw, "group totals are the column sum")
		assert.Equal(t, c.DiskBytes, colDisk)

		rawTotal += c.RawBytes
		diskTotal += c.DiskBytes
	}

	ds, err := e.PartsDetailed(ctx)
	require.NoError(t, err)

	for _, d := range ds {
		partTotal += d.Bytes
	}

	assert.Positive(t, rawTotal)
	assert.LessOrEqual(t, diskTotal, partTotal,
		"apportioned disk bytes never exceed what the parts occupy (%d vs %d)", diskTotal, partTotal)
	assert.Greater(t, cs[0].DiskBytes, cs[1].DiskBytes, "the fat stream carries the bytes")
}

func TestStreamCostGroupByStreamID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", rrec{ts: 1, body: "a"}))
	ingest(t, e, mkBatch("web", rrec{ts: 2, body: "b"}))
	require.NoError(t, e.Flush(ctx))

	cs, err := e.StreamCost(ctx, recordengine.StreamCostOptions{})
	require.NoError(t, err)
	require.Len(t, cs, 2)

	for _, c := range cs {
		assert.Len(t, c.Key, 32, "stream id renders as 32 hex digits")
		assert.Equal(t, 1, c.Streams)
	}
}

// TestStreamCostAbsentLabel: streams missing the grouping label collapse into the empty key rather
// than disappearing.
func TestStreamCostAbsentLabel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", rrec{ts: 1, body: "a"}))
	ingest(t, e, mkBatch("web", rrec{ts: 2, body: "b"}))
	require.NoError(t, e.Flush(ctx))

	cs, err := e.StreamCost(ctx, recordengine.StreamCostOptions{GroupBy: "no.such.label"})
	require.NoError(t, err)
	require.Len(t, cs, 1)
	assert.Empty(t, cs[0].Key)
	assert.Equal(t, 2, cs[0].Streams)
	assert.Equal(t, int64(2), cs[0].Rows)
}

// TestStreamCostGroupByScopeAttribute: the grouping label is resolved against scope attributes too,
// not only resource ones.
func TestStreamCostGroupByScopeAttribute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	series := signal.Series{Scope: signal.Scope{
		Name: []byte("lib"),
		Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("deployment.env"), Value: signal.StringValue([]byte("prod"))},
		),
	}}

	b := &recordengine.Batch{
		Stream:   series.Hash(),
		Identity: func() signal.Series { return series },
		Ts:       []int64{1},
		Ints:     [][]int64{{0}},
		Bytes:    [][][]byte{{[]byte("hello")}, {nil}, {nil}},
	}

	ingest(t, e, b)
	require.NoError(t, e.Flush(ctx))

	cs, err := e.StreamCost(ctx, recordengine.StreamCostOptions{GroupBy: "deployment.env"})
	require.NoError(t, err)
	require.Len(t, cs, 1)
	assert.Equal(t, "prod", cs[0].Key)
	assert.Equal(t, int64(5), columnOf(t, cs[0], "body").RawBytes)
}

func TestStreamCostColumnsAndTopN(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", rrec{ts: 1, body: "a", id: "x"}))
	ingest(t, e, mkBatch("web", rrec{ts: 2, body: "bbbb", id: "y"}))
	require.NoError(t, e.Flush(ctx))

	cs, err := e.StreamCost(ctx, recordengine.StreamCostOptions{
		GroupBy: "service.name", Columns: []string{"body"}, TopN: 1,
	})
	require.NoError(t, err)
	require.Len(t, cs, 1, "TopN truncates")
	assert.Equal(t, "web", cs[0].Key)

	names := make([]string, 0, len(cs[0].Columns))
	for _, cc := range cs[0].Columns {
		names = append(names, cc.Name)
	}

	assert.Equal(t, []string{"stream", "ts", "sev", "body"}, names,
		"int columns are free, so they stay; the unselected byte columns are dropped")
}

// TestStreamCostSketchBudget: past the budget a group keeps its byte attribution and loses only the
// distinct estimates, which it reports rather than passing off zeros as measured.
func TestStreamCostSketchBudget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("busy", rrec{ts: 1, body: "a"}, rrec{ts: 2, body: "b"}, rrec{ts: 3, body: "c"}))
	ingest(t, e, mkBatch("quiet", rrec{ts: 1, body: "z"}))
	require.NoError(t, e.Flush(ctx))

	cs, err := e.StreamCost(ctx, recordengine.StreamCostOptions{GroupBy: "service.name", MaxSketchGroups: 1})
	require.NoError(t, err)
	require.Len(t, cs, 2)

	busy := costOf(t, cs, "busy")
	assert.True(t, busy.DistinctEstimated, "the budget goes to the group with the most rows")
	assert.Positive(t, columnOf(t, busy, "body").Distinct)

	quiet := costOf(t, cs, "quiet")
	assert.False(t, quiet.DistinctEstimated)
	assert.Zero(t, columnOf(t, quiet, "body").Distinct)
	assert.Positive(t, quiet.RawBytes, "bytes are still attributed")
}

func TestStreamCostNoParts(t *testing.T) {
	t.Parallel()

	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", rrec{ts: 1, body: "a"}))

	cs, err := e.StreamCost(context.Background(), recordengine.StreamCostOptions{GroupBy: "service.name"})
	require.NoError(t, err)
	assert.Empty(t, cs, "the head holds no compressed bytes to attribute")
}

// BenchmarkStreamCost measures the drill-down itself — the whole pass over a flushed part — sized by
// the logical body bytes it walks, so the figure is a per-byte attribution rate an operator can
// multiply by a store's size. Nothing here runs on the ingest or merge path.
func BenchmarkStreamCost(b *testing.B) {
	const (
		streams = 16
		rows    = 2000
	)

	ctx := context.Background()
	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "b/recs"})

	var logical int64

	for s := range streams {
		recs := make([]rrec, 0, rows)

		for i := range rows {
			body := templated(i * (s + 1))
			logical += int64(len(body))
			recs = append(recs, rrec{ts: int64(i), body: body})
		}

		if _, err := e.AppendBatch(mkBatch(fmt.Sprintf("svc-%d", s), recs...), recordengine.AppendLimits{}); err != nil {
			b.Fatal(err)
		}
	}

	if err := e.Flush(ctx); err != nil {
		b.Fatal(err)
	}

	opts := recordengine.StreamCostOptions{GroupBy: "service.name"}

	b.ReportAllocs()
	b.SetBytes(logical)
	b.ResetTimer()

	for range b.N {
		if _, err := e.StreamCost(ctx, opts); err != nil {
			b.Fatal(err)
		}
	}
}

// TestStreamCostAfterMerge: a merged part is attributed like any other.
func TestStreamCostAfterMerge(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api", rrec{ts: 1, body: "one"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 2, body: "two"}))
	require.NoError(t, e.Flush(ctx))
	require.NoError(t, e.Merge(ctx, 0))

	cs, err := e.StreamCost(ctx, recordengine.StreamCostOptions{GroupBy: "service.name"})
	require.NoError(t, err)
	require.Len(t, cs, 1)
	assert.Equal(t, int64(2), cs[0].Rows)
	assert.Equal(t, 1, cs[0].Streams)
	assert.Equal(t, int64(6), columnOf(t, cs[0], "body").RawBytes)
}
