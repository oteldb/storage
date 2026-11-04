package recordengine_test

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/backendtest"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

func rangeCondition(column string, r *fetch.Int64Range) fetch.Condition {
	return fetch.Condition{Column: column, Int64Range: r, Match: func(v signal.Value) bool {
		if v.Kind() != signal.KindInt {
			return false
		}
		n := v.Int()
		return (r.Lower == nil || n > r.Lower.Value || (r.Lower.Inclusive && n == r.Lower.Value)) &&
			(r.Upper == nil || n < r.Upper.Value || (r.Upper.Inclusive && n == r.Upper.Value)) && n%2 == 0
	}}
}

func TestInt64RangeMatchesBruteForce(t *testing.T) {
	t.Parallel()
	be := backend.Memory()
	e := newEngine(t, be)
	values := append(make([]int64, 0, 11), math.MinInt64, -100, -1, 0, 1, 2, 100, 1<<53, 1<<53+1, math.MaxInt64)
	for i, v := range values {
		ingest(t, e, mkBatch("api", rrec{ts: int64(i), sev: v, body: strconv.Itoa(i)}))
	}
	check := func(stage string) {
		t.Helper()
		for _, lower := range []*fetch.Int64Bound{nil, {Value: math.MinInt64}, {Value: 0, Inclusive: true}, {Value: 1<<53 + 1}, {Value: math.MaxInt64}} {
			for _, upper := range []*fetch.Int64Bound{nil, {Value: math.MaxInt64, Inclusive: true}, {Value: 100}, {Value: 0, Inclusive: true}, {Value: math.MinInt64}} {
				r := &fetch.Int64Range{Lower: lower, Upper: upper}
				cond := rangeCondition("sev", r)
				var want []int64
				for i, v := range values {
					if cond.Match(signal.IntValue(v)) {
						want = append(want, int64(i))
					}
				}
				for _, hinted := range []bool{false, true} {
					c := cond
					if !hinted {
						c.Int64Range = nil
					}
					var got []int64
					for _, batch := range fetchAll(t, e, req("api", c)) {
						got = append(got, batch.Timestamps...)
					}
					slices.Sort(got)
					require.Equal(t, want, got, "%s hinted=%t lower=%v upper=%v", stage, hinted, lower, upper)
				}
			}
		}
		// An attribute and an absent key are never mistaken for physical int64 extrema.
		for _, column := range []string{"absent", "user"} {
			c := rangeCondition(column, &fetch.Int64Range{Lower: &fetch.Int64Bound{Value: 1, Inclusive: true}})
			c.Match = func(v signal.Value) bool { return v.Kind() == signal.KindEmpty }
			// Absence-accepting callbacks carry no hint by contract.
			c.Int64Range = nil
			var got int
			for _, b := range fetchAll(t, e, req("api", c)) {
				got += len(b.Timestamps)
			}
			require.Equal(t, len(values), got)
		}
	}
	check("head")
	require.NoError(t, e.Flush(t.Context()))
	check("flushed")
	e = newEngine(t, be)
	require.NoError(t, e.LoadParts(t.Context()))
	check("reopened")
	ingest(t, e, mkBatch("api", rrec{ts: int64(len(values)), sev: 42, body: "late"}))
	values = append(values, 42)
	check("parts_and_head")
	require.NoError(t, e.Flush(t.Context()))
	require.NoError(t, e.MergeWith(t.Context(), recordengine.MergeOptions{Force: true}))
	check("merged")
}

func TestInt64RangeAvoidsColumnReads(t *testing.T) {
	t.Parallel()
	be := backendtest.NewCounting(backend.Memory())
	e := newEngine(t, be)
	for part := range 3 {
		ingest(t, e, mkBatch("api",
			rrec{ts: int64(part * 2), sev: int64(part * 100), body: strconv.Itoa(part)},
			rrec{ts: int64(part*2 + 1), sev: int64(part*100 + 1), body: strconv.Itoa(part)},
		))
		require.NoError(t, e.Flush(t.Context()))
	}
	cond := rangeCondition("sev", &fetch.Int64Range{Lower: &fetch.Int64Bound{Value: 1000, Inclusive: true}})
	before := be.Reads()
	require.Empty(t, fetchAll(t, e, req("api", cond)))
	require.Equal(t, before, be.Reads(), "metadata pruning opens no condition or projected column")
	cond.Int64Range = nil
	require.Empty(t, fetchAll(t, e, req("api", cond)))
	conditionReads := 0
	for key, reads := range be.Reads() {
		// Severity is the first schema column, after the implicit timestamp column.
		if strings.HasSuffix(key, "/c/1") {
			conditionReads += reads - before[key]
		}
	}
	require.Equal(t, 3, conditionReads, "the equivalent callback alone reads every severity column")
}

func BenchmarkInt64RangeFetch(b *testing.B) {
	be := backendtest.NewCounting(backend.Memory())
	e := recordengine.New(recordengine.Config{Schema: testSchema, Backend: be, Prefix: "range/bench"})
	const parts, rows = 8, 256
	for part := range parts {
		recs := make([]rrec, rows)
		for i := range recs {
			recs[i] = rrec{ts: int64(part*rows + i), sev: int64(part*rows + i), body: "payload"}
		}
		if _, err := e.AppendBatch(mkBatch("api", recs...), recordengine.AppendLimits{}); err != nil {
			b.Fatal(err)
		}
		if err := e.Flush(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
	for _, selective := range []bool{true, false} {
		for _, hinted := range []bool{false, true} {
			b.Run(fmt.Sprintf("selective=%t/hint=%t", selective, hinted), func(b *testing.B) {
				minimum := int64(0)
				if selective {
					minimum = (parts - 1) * rows
				}
				cond := rangeCondition("sev", &fetch.Int64Range{Lower: &fetch.Int64Bound{Value: minimum, Inclusive: true}})
				if !hinted {
					cond.Int64Range = nil
				}
				before := be.Reads()
				b.ReportAllocs()
				b.SetBytes(parts * rows * 8)
				for b.Loop() {
					it, err := e.Fetch(b.Context(), req("api", cond))
					if err != nil {
						b.Fatal(err)
					}
					got, err := fetch.Drain(b.Context(), it)
					if err != nil {
						b.Fatal(err)
					}
					n := 0
					for _, batch := range got {
						n += len(batch.Timestamps)
					}
					want := (parts*rows - int(minimum)) / 2
					if n != want {
						b.Fatalf("rows=%d want=%d", n, want)
					}
				}
				reads := 0
				for key, n := range be.Reads() {
					reads += n - before[key]
				}
				b.ReportMetric(float64(reads)/float64(b.N), "reads/op")
			})
		}
	}
}

func TestInt64RangeAdvisoryAndAbsentColumns(t *testing.T) {
	t.Parallel()
	be := backendtest.NewCounting(backend.Memory())
	e := newEngine(t, be)
	ingest(t, e, mkBatch("api", rrec{ts: 1, sev: 2, body: "row"}))
	require.NoError(t, e.Flush(t.Context()))
	cond := rangeCondition("sev", &fetch.Int64Range{Lower: &fetch.Int64Bound{Value: 100, Inclusive: true}})
	r := req("api", cond)
	r.AllConditions = false
	got := fetchAll(t, e, r)
	require.Len(t, got, 1)
	require.Equal(t, []int64{1}, got[0].Timestamps, "advisory conditions never prune parts")
	for _, name := range []string{"absent", "body"} {
		cond.Column = name
		ctx, collector := profile.WithCollector(t.Context())
		it, err := e.Fetch(ctx, req("api", cond))
		require.NoError(t, err)
		batches, err := fetch.Drain(ctx, it)
		require.NoError(t, err)
		require.Empty(t, batches)
		node := findCounter(collector.Root(), "recordengine.fetch")
		require.NotNil(t, node)
		require.EqualValues(t, 1, node.Counters["parts_live"], "missing or non-int physical columns retain parts")
		require.Zero(t, node.Counters["parts_pruned_range"])
	}
}

func TestInt64RangesRemainConjunctive(t *testing.T) {
	t.Parallel()
	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", rrec{ts: 1, sev: 2}, rrec{ts: 2, sev: 4}, rrec{ts: 3, sev: 6}))
	require.NoError(t, e.Flush(t.Context()))
	lower := rangeCondition("sev", &fetch.Int64Range{Lower: &fetch.Int64Bound{Value: 2}})
	upper := rangeCondition("sev", &fetch.Int64Range{Upper: &fetch.Int64Bound{Value: 6}})
	got := fetchAll(t, e, req("api", lower, upper))
	require.Len(t, got, 1)
	require.Equal(t, []int64{2}, got[0].Timestamps)
}
