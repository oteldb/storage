package recordengine_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func sevAtLeast(threshold int64) fetch.Condition {
	return fetch.Condition{Column: "sev", Match: func(v signal.Value) bool { return v.Int() >= threshold }}
}

func bodyContains(sub string) fetch.Condition {
	want := []byte(sub)

	return fetch.Condition{
		Column: "body",
		Match:  func(v signal.Value) bool { return bytes.Contains(v.Str(), want) },
		Tokens: bloom.Tokenize(nil, want),
	}
}

func idEquals(id string) fetch.Condition {
	want := []byte(id)

	return fetch.Condition{
		Column: "id",
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		Equal:  &fetch.EqualMatcher{Name: "id", Value: id},
	}
}

func attrEquals(key, val string) fetch.Condition {
	want := []byte(val)

	return fetch.Condition{
		Column: key,
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		Equal:  &fetch.EqualMatcher{Name: key, Value: val},
	}
}

func attrNotEquals(key, val string) fetch.Condition {
	want := []byte(val)

	// No Equal/Tokens hint: a negation asserts nothing is present, so nothing may be bloom-pruned.
	return fetch.Condition{
		Column: key,
		Match:  func(v signal.Value) bool { return !bytes.Equal(v.Str(), want) },
	}
}

func attrUnset(key string) fetch.Condition {
	return fetch.Condition{
		Column: key,
		Match:  func(v signal.Value) bool { return v.Kind() == signal.KindEmpty },
	}
}

// A row that lacks the condition's column must reach the predicate as [signal.EmptyValue] rather
// than being dropped before it — otherwise every negation and is-unset condition silently loses the
// rows it is supposed to select. Covers both scan paths: the flushed part (lazy scan) and the head.
func TestConditionMatchesRowsMissingTheColumn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	ingest(t, e, mkBatch("api",
		rrec{ts: 100, body: "part-alice", attr: [2]string{"user", "alice"}},
		rrec{ts: 200, body: "part-bob", attr: [2]string{"user", "bob"}},
		rrec{ts: 300, body: "part-none"}, // no user attribute
	))
	require.NoError(t, e.Flush(ctx))

	ingest(t, e, mkBatch("api",
		rrec{ts: 400, body: "head-alice", attr: [2]string{"user", "alice"}},
		rrec{ts: 500, body: "head-none"}, // no user attribute
	))

	// Negation: the rows carrying another value AND the rows carrying no "user" at all.
	got := fetchAll(t, e, req("api", attrNotEquals("user", "alice")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"part-bob", "part-none", "head-none"}, bodies(got[0]))

	// Is-unset: only the rows with no "user" attribute.
	got = fetchAll(t, e, req("api", attrUnset("user")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"part-none", "head-none"}, bodies(got[0]))

	// A key no row carries (and that is not a schema column) is unset everywhere.
	got = fetchAll(t, e, req("api", attrUnset("nosuchkey")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"part-alice", "part-bob", "part-none", "head-alice", "head-none"}, bodies(got[0]))

	// A positive equality still rejects the absent rows — absence is a value the predicate judges.
	got = fetchAll(t, e, req("api", attrEquals("user", "alice")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"part-alice", "head-alice"}, bodies(got[0]))
}

func TestConditionsFilterColumnsAndAttributes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api",
		rrec{ts: 100, sev: 9, body: "info ok", attr: [2]string{"user", "alice"}},
		rrec{ts: 200, sev: 17, body: "error boom", attr: [2]string{"user", "bob"}},
		rrec{ts: 300, sev: 17, body: "error again", attr: [2]string{"user", "alice"}},
	))
	require.NoError(t, e.Flush(ctx))

	// Int-column condition.
	got := fetchAll(t, e, req("api", sevAtLeast(17)))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"error boom", "error again"}, bodies(got[0]))

	// Full-text body condition (with bloom token).
	got = fetchAll(t, e, req("api", bodyContains("info")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"info ok"}, bodies(got[0]))

	// Attribute condition (decoded from the attrs blob), ANDed with severity.
	got = fetchAll(t, e, req("api", sevAtLeast(17), attrEquals("user", "alice")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"error again"}, bodies(got[0]))
}

func TestEqualityBloomPrunesTraceByID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	// Two parts with disjoint ids, plus a head record.
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "a", id: "trace-1"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 200, body: "b", id: "trace-2"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("api", rrec{ts: 300, body: "c", id: "trace-1"}))

	// id == trace-1: part 2 (only trace-2) is equality-bloom-pruned; part 1 + head match.
	got := fetchAll(t, e, fetch.Request{
		Start: 0, End: 1 << 60, AllConditions: true, Conditions: []fetch.Condition{idEquals("trace-1")},
	})
	require.Len(t, got, 1)
	assert.Equal(t, []string{"a", "c"}, bodies(got[0]))

	// An id present nowhere ⇒ all parts pruned, head scanned, empty.
	assert.Empty(t, fetchAll(t, e, fetch.Request{
		Start: 0, End: 1 << 60, AllConditions: true, Conditions: []fetch.Condition{idEquals("trace-none")},
	}))
}

func TestLazyProjectionDecodesReferencedColumnsOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", rrec{ts: 100, sev: 17, body: "x", id: "t", attr: [2]string{"k", "v"}}))
	require.NoError(t, e.Flush(ctx))

	// Filter on sev, project only body: sev is decoded for the filter, only body is output.
	r := req("api", sevAtLeast(17))
	r.Projection = []string{"body"}
	got := fetchAll(t, e, r)
	require.Len(t, got, 1)

	require.Len(t, got[0].Columns, 1)
	assert.Equal(t, "body", got[0].Columns[0].Name)
	_, ok := got[0].Column("sev")
	assert.False(t, ok, "the filter-only column is not materialized in the output")
	assert.Equal(t, []int64{100}, got[0].Timestamps)
}

func TestLazyFilteredMultiStreamWindowAbsentAttr(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())

	// Two streams flushed to separate parts, so each part lacks the other stream (exercises the
	// per-stream range lookup miss in the lazy scan). One api record has no "user" attribute.
	ingest(t, e, mkBatch("api",
		rrec{ts: 100, sev: 17, body: "a1", attr: [2]string{"user", "alice"}},
		rrec{ts: 200, sev: 17, body: "a2"}, // no user attr
	))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, mkBatch("web",
		rrec{ts: 150, sev: 17, body: "w1", attr: [2]string{"user", "alice"}},
	))
	require.NoError(t, e.Flush(ctx))

	// No matcher ⇒ all streams resolve; both parts are scanned though each holds only one stream.
	all := func(start, end int64, conds ...fetch.Condition) fetch.Request {
		return fetch.Request{Start: start, End: end, AllConditions: true, Conditions: conds}
	}
	collect := func(bs []*fetch.Batch) []string {
		out := make([]string, 0, len(bs))
		for _, b := range bs {
			out = append(out, bodies(b)...)
		}

		return out
	}

	// Full window: every stream's matching rows across both parts.
	assert.ElementsMatch(t, []string{"a1", "a2", "w1"}, collect(fetchAll(t, e, all(0, 1<<60, sevAtLeast(17)))))

	// Narrow window drops a1 (ts 100) and a2 (ts 200), keeps only w1 (ts 150).
	assert.ElementsMatch(t, []string{"w1"}, collect(fetchAll(t, e, all(120, 180, sevAtLeast(17)))))

	// Attribute condition: a2 has no "user" attribute, so the lazy lookup misses and it is excluded.
	assert.ElementsMatch(t, []string{"a1", "w1"}, collect(fetchAll(t, e, all(0, 1<<60, attrEquals("user", "alice")))))
}

func TestSecondPassDropsBatch(t *testing.T) {
	t.Parallel()

	e := newEngine(t, nil)
	ingest(t, e, mkBatch("api", rrec{ts: 100, body: "a"}))
	ingest(t, e, mkBatch("web", rrec{ts: 100, body: "b"}))

	got := fetchAll(t, e, fetch.Request{
		Start: 0, End: 1 << 60,
		SecondPass: func(b *fetch.Batch) bool {
			col, _ := b.Column("body")

			return len(col.Bytes) > 0 && bytes.Equal(col.Bytes[0], []byte("b"))
		},
	})
	require.Len(t, got, 1)
	assert.Equal(t, []string{"b"}, bodies(got[0]))
}
