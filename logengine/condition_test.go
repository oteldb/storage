package logengine_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/logengine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
)

// richStream builds a one-stream batch where each record carries a body, severity, and a "user"
// attribute, so column and attribute conditions can be exercised.
func richStream(svc string, recs ...richRec) log.Logs {
	var ld log.Logs
	rl := ld.AddResource()
	rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}
	sl := rl.AddScope()
	sl.Scope = signal.Scope{Name: []byte("lib")}

	for _, r := range recs {
		rec := sl.AddRecord()
		rec.Timestamp = r.ts
		rec.SeverityNumber = r.sev
		rec.Body = []byte(r.body)
		rec.Attributes = signal.NewAttributes(
			signal.KeyValue{Key: []byte("user"), Value: signal.StringValue([]byte(r.user))},
		)
	}

	return ld
}

type richRec struct {
	ts   int64
	sev  int32
	body string
	user string
}

// severityAtLeast is a column condition over the severity column.
func severityAtLeast(threshold int64) fetch.Condition {
	return fetch.Condition{Column: "severity", Match: func(v signal.Value) bool { return v.Int() >= threshold }}
}

// bodyContains is a column condition over the body column.
func bodyContains(sub string) fetch.Condition {
	want := []byte(sub)

	return fetch.Condition{Column: "body", Match: func(v signal.Value) bool { return bytes.Contains(v.Str(), want) }}
}

// attrEquals is an equality condition over a per-record attribute key. It carries the serializable
// Equal spec so the engine can prune parts via the attribute bloom.
func attrEquals(key, val string) fetch.Condition {
	want := []byte(val)

	return fetch.Condition{
		Column: key,
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		Equal:  &fetch.EqualMatcher{Name: key, Value: val},
	}
}

// attrContains is a non-equality (substring) condition over a per-record attribute key. It carries
// the search Tokens so the engine can prune parts via the attribute bloom's key-scoped value tokens.
func attrContains(key, sub string) fetch.Condition {
	want := []byte(sub)

	return fetch.Condition{
		Column: key,
		Match:  func(v signal.Value) bool { return bytes.Contains(v.Str(), want) },
		Tokens: bloom.Tokenize(nil, []byte(sub)),
	}
}

func condReq(svc string, conds ...fetch.Condition) fetch.Request {
	return fetch.Request{
		Signal: signal.Log, Start: 0, End: 1 << 60,
		Matchers:      []fetch.Matcher{eqMatcher("service.name", svc)},
		Conditions:    conds,
		AllConditions: true,
	}
}

func TestConditionFiltersBySeverity(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	ingest(t, e, richStream("api",
		richRec{100, 9, "info msg", "alice"},
		richRec{200, 17, "error msg", "bob"},
		richRec{300, 5, "debug msg", "alice"},
	))

	got := fetchAll(t, e, condReq("api", severityAtLeast(17)))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"error msg"}, bodies(got[0]), "only the severity≥17 record")
}

func TestConditionBodyContains(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	ingest(t, e, richStream("api",
		richRec{100, 9, "GET /health", "x"},
		richRec{200, 9, "POST /login", "y"},
		richRec{300, 9, "GET /metrics", "z"},
	))

	got := fetchAll(t, e, condReq("api", bodyContains("GET")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"GET /health", "GET /metrics"}, bodies(got[0]))
}

func TestConditionAttributeFilter(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	ingest(t, e, richStream("api",
		richRec{100, 9, "a", "alice"},
		richRec{200, 9, "b", "bob"},
		richRec{300, 9, "c", "alice"},
	))

	got := fetchAll(t, e, condReq("api", attrEquals("user", "alice")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"a", "c"}, bodies(got[0]), "filtered by the per-record user attribute")
}

func TestConditionsAndedAcrossColumns(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	ingest(t, e, richStream("api",
		richRec{100, 17, "error from alice", "alice"},
		richRec{200, 17, "error from bob", "bob"},
		richRec{300, 9, "info from alice", "alice"},
	))

	// severity≥17 AND user=alice ⇒ only the first record.
	got := fetchAll(t, e, condReq("api", severityAtLeast(17), attrEquals("user", "alice")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"error from alice"}, bodies(got[0]))
}

func TestConditionsAcrossHeadAndPart(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	ingest(t, e, richStream("api", richRec{100, 17, "old error", "alice"}, richRec{150, 9, "old info", "bob"}))
	require.NoError(t, e.Flush(ctx)) // → part
	ingest(t, e, richStream("api", richRec{200, 17, "new error", "carol"}))

	got := fetchAll(t, e, condReq("api", severityAtLeast(17)))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"old error", "new error"}, bodies(got[0]), "condition applies to head ∪ part")
}

func TestAllConditionsFalseReturnsSuperset(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	ingest(t, e, richStream("api", richRec{100, 9, "a", "x"}, richRec{200, 17, "b", "y"}))

	// AllConditions=false ⇒ the engine does not filter; the language layer would.
	r := condReq("api", severityAtLeast(17))
	r.AllConditions = false
	got := fetchAll(t, e, r)
	require.Len(t, got, 1)
	assert.Equal(t, []string{"a", "b"}, bodies(got[0]), "unfiltered superset")
}

func TestProjectionNarrowsColumns(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	ingest(t, e, richStream("api", richRec{100, 9, "msg", "x"}))

	r := condReq("api")
	r.Projection = []string{"body"} // only the body column
	got := fetchAll(t, e, r)
	require.Len(t, got, 1)

	require.Len(t, got[0].Columns, 1)
	assert.Equal(t, "body", got[0].Columns[0].Name)
	_, ok := got[0].Column("severity")
	assert.False(t, ok, "unprojected columns are not materialized")
	assert.Equal(t, []int64{100}, got[0].Timestamps, "timestamps are always present")
}

func TestAttributeBloomPrunesParts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	// Two parts with disjoint attribute values, plus a head record.
	ingest(t, e, richStream("api", richRec{100, 9, "p1", "alice"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, richStream("api", richRec{200, 9, "p2", "bob"}))
	require.NoError(t, e.Flush(ctx))
	ingest(t, e, richStream("api", richRec{300, 9, "head", "alice"}))
	require.Equal(t, 2, e.PartCount())

	// user=alice: part 2 (only bob) is bloom-pruned; part 1 and the head match. The per-row Match
	// re-checks, so the result is exact — no false negatives from pruning.
	got := fetchAll(t, e, condReq("api", attrEquals("user", "alice")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"p1", "head"}, bodies(got[0]))

	// A value present in no part nor head ⇒ every part pruned, head scanned, nothing matches.
	none := fetchAll(t, e, condReq("api", attrEquals("user", "nobody")))
	assert.Empty(t, none)
}

func TestNonEqualityAttributeSearchAndPrune(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := logengine.New(logengine.Config{Backend: backend.Memory(), Prefix: "t/logs"})

	// "msg" attribute carries free text; two parts with disjoint vocabularies, plus a head record.
	ingestMsg := func(ts int64, body, m string) {
		var ld log.Logs
		rl := ld.AddResource()
		rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
		)}
		sl := rl.AddScope()
		r := sl.AddRecord()
		r.Timestamp, r.Body = ts, []byte(body)
		r.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("msg"), Value: signal.StringValue([]byte(m))})
		ingest(t, e, ld)
	}

	ingestMsg(100, "p1", "connection refused upstream")
	require.NoError(t, e.Flush(ctx))
	ingestMsg(200, "p2", "request timeout")
	require.NoError(t, e.Flush(ctx))
	ingestMsg(300, "head", "connection reset")
	require.Equal(t, 2, e.PartCount())

	// `msg contains "connection"`: part 2 (timeout) is bloom-pruned; part 1 + head match.
	got := fetchAll(t, e, condReq("api", attrContains("msg", "connection")))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"p1", "head"}, bodies(got[0]), "non-equality attribute search across head ∪ parts")

	// A word in no part nor head ⇒ every part pruned, head scanned, nothing matches.
	assert.Empty(t, fetchAll(t, e, condReq("api", attrContains("msg", "nonexistent"))))

	// A range predicate over a numeric attribute still evaluates correctly (it just can't be
	// bloom-pruned): only the equality/contains hints prune.
	ingestMsg(400, "lvl", "ok")
	got = fetchAll(t, e, condReq("api", fetch.Condition{
		Column: "msg", Match: func(v signal.Value) bool { return len(v.Str()) <= 2 },
	}))
	require.Len(t, got, 1)
	assert.Equal(t, []string{"lvl"}, bodies(got[0]), "non-token predicate scans and matches")
}

func TestProjectionMultipleColumnsAndUnknown(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	ingest(t, e, richStream("api", richRec{100, 9, "msg", "x"}))

	r := condReq("api")
	r.Projection = []string{"severity", "observed", "flags", "dropped", "severity_text", "trace_id", "span_id", "attrs", "nope"}
	got := fetchAll(t, e, r)
	require.Len(t, got, 1)

	names := make([]string, len(got[0].Columns))
	for i, c := range got[0].Columns {
		names[i] = c.Name
	}

	assert.Equal(t, []string{"severity", "observed", "flags", "dropped", "severity_text", "trace_id", "span_id", "attrs"}, names,
		"every known projected column is materialized; the unknown name is ignored")
}

func TestSecondPassDropsStreamBatch(t *testing.T) {
	t.Parallel()

	e := logengine.New(logengine.Config{})
	ingest(t, e, richStream("api", richRec{100, 9, "a", "x"}))
	ingest(t, e, richStream("web", richRec{100, 9, "b", "y"}))

	r := fetch.Request{
		Signal: signal.Log, Start: 0, End: 1 << 60,
		// No matchers ⇒ both streams; SecondPass keeps only batches whose first body is "b".
		SecondPass: func(b *fetch.Batch) bool {
			col, _ := b.Column("body")

			return len(col.Bytes) > 0 && bytes.Equal(col.Bytes[0], []byte("b"))
		},
	}

	got := fetchAll(t, e, r)
	require.Len(t, got, 1, "only the matching stream batch survives")
	assert.Equal(t, []string{"b"}, bodies(got[0]))
}
