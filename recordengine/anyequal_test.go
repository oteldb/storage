package recordengine_test

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// setCondition is the set-membership predicate as [fetch.Condition.AnyEqual] expresses it: the
// closure and the hint say exactly the same thing, which is the contract (the hint is a superset of
// what Match accepts).
func setCondition(column string, members [][]byte, calls *atomic.Int64) fetch.Condition {
	set := fetch.AnyEqualSet(members)

	return fetch.Condition{
		Column:   column,
		Match:    countingSetMatch(set, calls),
		AnyEqual: set,
	}
}

// closureCondition is the same predicate as the status quo forces it: a bare Match closure with no
// hint at all, so no part is prunable and every row pays the callback.
func closureCondition(column string, members [][]byte, calls *atomic.Int64) fetch.Condition {
	return fetch.Condition{Column: column, Match: countingSetMatch(fetch.AnyEqualSet(members), calls)}
}

func countingSetMatch(set [][]byte, calls *atomic.Int64) func(signal.Value) bool {
	pred := fetch.AnyEqualPredicate(set)

	return func(v signal.Value) bool {
		if calls != nil {
			calls.Add(1)
		}

		return pred(v)
	}
}

func strs(vals ...string) [][]byte {
	out := make([][]byte, len(vals))
	for i, v := range vals {
		out[i] = []byte(v)
	}

	return out
}

// TestAnyEqualReturnsTheSameRowsAsTheClosure is the differential correctness check: for every set
// shape, the hinted condition must return exactly the rows the equivalent bare Match closure does.
// The hint is result-neutral by contract, so this guards a regression rather than proving the new
// path fired — TestAnyEqualSkipsPerRowMatch does that.
func TestAnyEqualReturnsTheSameRowsAsTheClosure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// A dictionary equality column ("id") and a per-record attribute ("user"), so both the
	// dictionary-entry test and the attrs-blob test are covered.
	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api",
		rrec{ts: 100, body: "a", id: "t1", attr: [2]string{"user", "alice"}},
		rrec{ts: 200, body: "b", id: "t2", attr: [2]string{"user", "bob"}},
		rrec{ts: 300, body: "c", id: "t3", attr: [2]string{"user", "carol"}},
	))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api",
		rrec{ts: 400, body: "d", id: "t2", attr: [2]string{"user", "bob"}},
		rrec{ts: 500, body: "e", id: "", attr: [2]string{"user", "alice"}}, // no id at all
	))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 600, body: "f", id: "t1"})) // head, no attribute

	for _, tt := range []struct {
		name    string
		column  string
		members [][]byte
		want    []string
	}{
		{"nil set", "id", nil, nil},
		{"empty set", "id", [][]byte{}, nil},
		{"single member", "id", strs("t1"), []string{"a", "f"}},
		{"several members", "id", strs("t1", "t2"), []string{"a", "b", "d", "f"}},
		{"unsorted members", "id", strs("t3", "t1", "t2"), []string{"a", "b", "c", "d", "f"}},
		{"duplicate members", "id", strs("t1", "t1", "t1"), []string{"a", "f"}},
		{"members absent from the column", "id", strs("t9", "t8"), nil},
		{"one present, one absent", "id", strs("t3", "t9"), []string{"c"}},
		{"attribute set", "user", strs("alice", "carol"), []string{"a", "c", "e"}},
		{"attribute set, absent value", "user", strs("dave"), nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hinted := allBodies(t, e, req("api", setCondition(tt.column, tt.members, nil)))
			closure := allBodies(t, e, req("api", closureCondition(tt.column, tt.members, nil)))

			assert.ElementsMatch(t, tt.want, hinted, "the hinted condition selects the expected rows")
			assert.ElementsMatch(t, closure, hinted, "the hint changes no result the closure alone gives")
		})
	}
}

// TestAnyEqualEmptySetMatchesNothing pins the N=0 semantics: nil and an empty set are the same
// thing — no hint — and the predicate alone still selects nothing, because a membership closure over
// an empty set rejects every row.
func TestAnyEqualEmptySetMatchesNothing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "a", id: "t1"}))
	require.NoError(t, e.Flush(ctx))

	assert.Nil(t, fetch.AnyEqualSet(nil), "nil normalizes to nil")
	assert.Nil(t, fetch.AnyEqualSet([][]byte{}), "an empty set normalizes to the same nil")

	assert.Empty(t, allBodies(t, e, req("api", setCondition("id", nil, nil))))
	assert.Empty(t, allBodies(t, e, req("api", setCondition("id", [][]byte{}, nil))))
}

// TestAnyEqualSkipsPerRowMatch is the behavioral control for the row-evaluation half: with the set
// carried as a hint the predicate runs for the matching rows only, where the bare closure runs for
// every row of every part the query touches.
func TestAnyEqualSkipsPerRowMatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newRawIDEngine(t, backend.Memory())

	const rows = 512

	recs := make([]rawIDRec, 0, rows)
	for i := range rows {
		recs = append(recs, rawIDRec{ts: int64(i), body: strconv.Itoa(i), id: id16(byte(i % 251))})
	}

	ingest(t, e, mkRawIDBatch("api", recs...))
	require.NoError(t, e.Flush(ctx))

	members := [][]byte{id16(3), id16(7)}

	all := func(c fetch.Condition) fetch.Request {
		return fetch.Request{Start: 0, End: 1 << 60, AllConditions: true, Conditions: []fetch.Condition{c}}
	}

	var hinted, closure atomic.Int64

	gotHinted := rawIDBodies(t, e, all(setCondition("trace_id", members, &hinted)))
	gotClosure := rawIDBodies(t, e, all(closureCondition("trace_id", members, &closure)))

	assert.ElementsMatch(t, gotClosure, gotHinted)
	assert.NotEmpty(t, gotHinted)

	t.Logf("match calls: hinted=%d closure=%d rows=%d", hinted.Load(), closure.Load(), rows)
	assert.EqualValues(t, rows, closure.Load(), "the bare closure is called once per row")
	assert.Lessf(t, hinted.Load(), int64(rows/4),
		"the set rejects non-members without the callback (hinted=%d of %d rows)", hinted.Load(), rows)
}

// TestAnyEqualPrunesParts is the behavioral control for the pruning half: a part whose equality
// bloom proves *every* member absent must never be read. The disjunction lives inside the one
// condition — passing the members as N conditions would AND them and prune everything — so this is
// what the field exists for.
func TestAnyEqualPrunesParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newRawIDEngine(t, backend.Memory())

	const parts = 8

	for p := range parts {
		ingest(t, e, mkRawIDBatch("api",
			rawIDRec{ts: int64(p*100 + 1), body: fmt.Sprintf("p%d-a", p), id: id16(byte(2 * p))},
			rawIDRec{ts: int64(p*100 + 2), body: fmt.Sprintf("p%d-b", p), id: id16(byte(2*p + 1))},
		))
		require.NoError(t, e.Flush(ctx))
	}

	require.Equal(t, parts, e.PartCount())

	// Members live in parts 0 and 5 only.
	members := [][]byte{id16(0), id16(10), id16(11)}

	run := func(t *testing.T, c fetch.Condition) (bodies []string, pruned, live int64) {
		t.Helper()

		pctx, coll := profile.WithCollector(ctx)

		it, err := e.Fetch(pctx, fetch.Request{
			Start: 0, End: 1 << 60, AllConditions: true, Conditions: []fetch.Condition{c},
		})
		require.NoError(t, err)

		batches, err := fetch.Drain(pctx, it)
		require.NoError(t, err)

		for _, b := range batches {
			bodies = append(bodies, bodyValues(b)...)
		}

		fn := findCounter(coll.Root(), "recordengine.fetch")
		require.NotNil(t, fn)

		return bodies, fn.Counters["parts_pruned_bloom"], fn.Counters["parts_live"]
	}

	hinted, pruned, live := run(t, setCondition("trace_id", members, nil))
	assert.ElementsMatch(t, []string{"p0-a", "p5-a", "p5-b"}, hinted)
	assert.EqualValues(t, parts-2, pruned, "every part holding no member is skipped")
	assert.EqualValues(t, 2, live, "only the parts that may hold a member are read")

	closure, prunedClosure, liveClosure := run(t, closureCondition("trace_id", members, nil))
	assert.ElementsMatch(t, hinted, closure, "pruning drops no matching row")
	assert.Zero(t, prunedClosure, "a bare closure prunes nothing")
	assert.EqualValues(t, parts, liveClosure, "a bare closure reads every part")
}

// bodyValues reads the "body" column of a batch, for the schemas that carry one.
func bodyValues(b *fetch.Batch) []string {
	col, _ := b.Column("body")

	out := make([]string, len(col.Bytes))
	for i, v := range col.Bytes {
		out[i] = string(v)
	}

	return out
}

// allBodies is [bodies] over every returned batch.
func allBodies(t *testing.T, e *recordengine.Engine, r fetch.Request) []string {
	t.Helper()

	batches := fetchAll(t, e, r)

	out := make([]string, 0, len(batches))
	for _, b := range batches {
		out = append(out, bodies(b)...)
	}

	return out
}
