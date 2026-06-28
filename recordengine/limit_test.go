package recordengine_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// allTs drains a fetch and returns every returned record timestamp, sorted ascending.
func allTs(t *testing.T, e *recordengine.Engine, r fetch.Request) []int64 {
	t.Helper()

	var out []int64
	for _, b := range fetchAll(t, e, r) {
		out = append(out, b.Timestamps...)
	}

	slices.Sort(out)

	return out
}

// limitReq matches every stream in the window with the given limit/direction.
func limitReq(limit int, reverse bool) fetch.Request {
	return fetch.Request{Signal: signal.Log, Start: 0, End: 1 << 60, Limit: limit, Reverse: reverse}
}

// threeStreams ingests three streams with interleaved timestamps 10..90 (nine rows total).
func threeStreams(t *testing.T) *recordengine.Engine {
	t.Helper()

	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("a", rrec{ts: 10, body: "a1"}, rrec{ts: 40, body: "a2"}, rrec{ts: 70, body: "a3"}))
	ingest(t, e, mkBatch("b", rrec{ts: 20, body: "b1"}, rrec{ts: 50, body: "b2"}, rrec{ts: 80, body: "b3"}))
	ingest(t, e, mkBatch("c", rrec{ts: 30, body: "c1"}, rrec{ts: 60, body: "c2"}, rrec{ts: 90, body: "c3"}))

	return e
}

func TestFetchLimitReverse(t *testing.T) {
	t.Parallel()

	e := threeStreams(t)
	require.NoError(t, e.Flush(context.Background()))

	assert.Equal(t, []int64{70, 80, 90}, allTs(t, e, limitReq(3, true)), "newest three across streams")
	assert.Equal(t, []int64{60, 70, 80, 90}, allTs(t, e, limitReq(4, true)), "newest four")
}

func TestFetchLimitForward(t *testing.T) {
	t.Parallel()

	e := threeStreams(t)
	require.NoError(t, e.Flush(context.Background()))

	assert.Equal(t, []int64{10, 20, 30}, allTs(t, e, limitReq(3, false)), "oldest three across streams")
}

// TestFetchLimitHeadOnly exercises the limit trim over the unflushed head (no parts).
func TestFetchLimitHeadOnly(t *testing.T) {
	t.Parallel()

	e := threeStreams(t) // no flush — all rows live in the head
	assert.Equal(t, []int64{70, 80, 90}, allTs(t, e, limitReq(3, true)))
}

// TestFetchLimitBoundaryTies verifies the superset guarantee: all rows tying at the boundary
// timestamp are kept, even past the requested limit.
func TestFetchLimitBoundaryTies(t *testing.T) {
	t.Parallel()

	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("a", rrec{ts: 70, body: "a"}))
	ingest(t, e, mkBatch("b", rrec{ts: 70, body: "b"}))
	ingest(t, e, mkBatch("c", rrec{ts: 70, body: "c"}))
	ingest(t, e, mkBatch("d", rrec{ts: 10, body: "d"}))
	require.NoError(t, e.Flush(context.Background()))

	// limit=1 but three rows tie at ts=70: all three returned (superset), the ts=10 row dropped.
	got := allTs(t, e, limitReq(1, true))
	assert.Equal(t, []int64{70, 70, 70}, got)
}

func TestFetchLimitUnlimitedAndOversize(t *testing.T) {
	t.Parallel()

	e := threeStreams(t)
	require.NoError(t, e.Flush(context.Background()))

	assert.Len(t, allTs(t, e, limitReq(0, true)), 9, "limit 0 ⇒ unlimited")
	assert.Len(t, allTs(t, e, limitReq(100, true)), 9, "limit ≥ total ⇒ all rows")
}

// TestFetchLimitComposesWithCondition is the Proposal-C check: a json-field filter lowered to a
// per-row Condition over the body column drops rows BEFORE the limit selection, so json-filter +
// limit compose with no new storage primitive.
func TestFetchLimitComposesWithCondition(t *testing.T) {
	t.Parallel()

	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("svc",
		rrec{ts: 10, body: `{"status":200}`},
		rrec{ts: 20, body: `{"status":500}`},
		rrec{ts: 30, body: `{"status":503}`},
		rrec{ts: 40, body: `{"status":200}`},
	))
	require.NoError(t, e.Flush(context.Background()))

	// `| json | status>=400` as a Condition over the body column.
	statusGTE := fetch.Condition{
		Column: "body",
		Match: func(v signal.Value) bool {
			var m struct {
				Status int `json:"status"`
			}
			if err := json.Unmarshal(v.Str(), &m); err != nil {
				return false
			}

			return m.Status >= 400
		},
	}

	r := fetch.Request{
		Signal: signal.Log, Start: 0, End: 1 << 60,
		Conditions: []fetch.Condition{statusGTE}, AllConditions: true,
		Limit: 1, Reverse: true,
	}

	// Matching rows are ts=20 (500) and ts=30 (503); newest one is ts=30 — the limit counts only
	// rows that survived the condition.
	assert.Equal(t, []int64{30}, allTs(t, e, r))
}

func BenchmarkRecordFetchLimit(b *testing.B) {
	ctx := context.Background()
	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: backend.Memory(), Prefix: "t/recs"})

	const (
		streams = 50
		perStrm = 200
	)

	for s := range streams {
		recs := make([]rrec, perStrm)
		for i := range recs {
			recs[i] = rrec{ts: int64(i*streams + s), body: "log line body text"}
		}

		if _, err := e.AppendBatch(mkBatch(svcName(s), recs...), recordengine.AppendLimits{}); err != nil {
			b.Fatal(err)
		}
	}

	if err := e.Flush(ctx); err != nil {
		b.Fatal(err)
	}

	// The limit pushdown's payoff is downstream: the embedder materializes a LabelSet+pcommon.Map
	// per RETURNED row, so fewer returned rows is the win. A fetch-only benchmark cannot show that
	// (the limit adds a per-query selection on top of the same columnar scan), so report rows/op as
	// the downstream-cost proxy alongside ns/op.
	bench := func(b *testing.B, r fetch.Request) {
		b.Helper()
		b.ReportAllocs()
		b.ResetTimer()

		var rows int64

		for range b.N {
			it, err := e.Fetch(ctx, r)
			if err != nil {
				b.Fatal(err)
			}

			batches, err := fetch.Drain(ctx, it)
			if err != nil {
				b.Fatal(err)
			}

			for _, bt := range batches {
				rows += int64(len(bt.Timestamps))
			}
		}

		b.ReportMetric(float64(rows)/float64(b.N), "rows/op")
	}

	b.Run("unlimited", func(b *testing.B) {
		bench(b, fetch.Request{Signal: signal.Log, Start: 0, End: 1 << 60})
	})
	b.Run("limit1000", func(b *testing.B) {
		bench(b, fetch.Request{Signal: signal.Log, Start: 0, End: 1 << 60, Limit: 1000, Reverse: true})
	})
}

func svcName(i int) string { return "svc-" + string(rune('a'+i%26)) + string(rune('0'+i/26)) }
