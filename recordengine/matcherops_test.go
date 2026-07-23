package recordengine_test

import (
	"bytes"
	"context"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// The engine's two predicate seams — [fetch.Matcher] over stream identity and [fetch.Condition]
// over columns — are operator-free: a language lowers every operator it has to a callback over a
// [signal.Value]. These tests pin what each seam must do for the whole operator set a language
// actually lowers (equality, inequality, regexp match/non-match, exists, unset), against every kind
// of lookup the engine supports, so a predicate that has to see an absent value does.
//
// The distinction the cases turn on: a fixed schema column is always present (a record without a
// trace id has an *empty* value, [signal.KindStr]), whereas a per-record attribute key or an unknown
// column is *absent* ([signal.KindEmpty]). Only the second reaches a predicate as empty.

func strEquals(column, val string) fetch.Condition {
	want := []byte(val)

	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		Equal:  &fetch.EqualMatcher{Name: column, Value: val},
	}
}

func strNotEquals(column, val string) fetch.Condition {
	want := []byte(val)

	// No Equal/Tokens hint: an inequality asserts no value is present, so nothing may be pruned.
	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return !bytes.Equal(v.Str(), want) },
	}
}

func strMatches(column, expr string) fetch.Condition {
	re := regexp.MustCompile(expr)

	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return re.Match(v.Str()) },
	}
}

func strNotMatches(column, expr string) fetch.Condition {
	re := regexp.MustCompile(expr)

	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return !re.Match(v.Str()) },
	}
}

func colExists(column string) fetch.Condition {
	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return v.Kind() != signal.KindEmpty },
	}
}

func colUnset(column string) fetch.Condition {
	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return v.Kind() == signal.KindEmpty },
	}
}

func intEquals(column string, want int64) fetch.Condition {
	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return v.Kind() == signal.KindInt && v.Int() == want },
	}
}

func intNotEquals(column string, want int64) fetch.Condition {
	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return v.Kind() != signal.KindInt || v.Int() != want },
	}
}

func bodyHasToken(sub string) fetch.Condition {
	want := []byte(sub)

	return fetch.Condition{
		Column: "body",
		Match:  func(v signal.Value) bool { return bytes.Contains(v.Str(), want) },
		Tokens: bloom.Tokenize(nil, want),
	}
}

// condFixture ingests a part (flushed) and a head buffer over one stream, so every case below runs
// through both scan paths: the lazy part scan and the eagerly buffered head.
//
//	body          ts   sev  id    user
//	part-alice    100  9    t-1   alice
//	part-bob      200  17   t-2   bob
//	part-bare     300  17   ""    (unset)
//	head-alice    400  9    t-1   alice
//	head-bare     500  17   ""    (unset)
func condFixture(t *testing.T) *recordengine.Engine {
	t.Helper()

	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api",
		rrec{ts: 100, sev: 9, body: "part-alice", id: "t-1", attr: [2]string{"user", "alice"}},
		rrec{ts: 200, sev: 17, body: "part-bob", id: "t-2", attr: [2]string{"user", "bob"}},
		rrec{ts: 300, sev: 17, body: "part-bare"},
	))
	require.NoError(t, e.Flush(context.Background()))
	ingest(t, e, mkBatch("api",
		rrec{ts: 400, sev: 9, body: "head-alice", id: "t-1", attr: [2]string{"user", "alice"}},
		rrec{ts: 500, sev: 17, body: "head-bare"},
	))

	return e
}

func TestConditionOperatorsOverEveryLookup(t *testing.T) {
	t.Parallel()

	const (
		alice = "part-alice"
		bob   = "part-bob"
		bare  = "part-bare"
		halic = "head-alice"
		hbare = "head-bare"
	)

	all := []string{alice, bob, bare, halic, hbare}

	tests := []struct {
		name string
		cond fetch.Condition
		want []string
	}{
		// Int column: always present, so exists/unset are decided by the column, not the row.
		{"int equals", intEquals("sev", 17), []string{bob, bare, hbare}},
		{"int not equals", intNotEquals("sev", 17), []string{alice, halic}},
		{"int exists", colExists("sev"), all},
		{"int unset", colUnset("sev"), nil},

		// Full-text byte column (bloom-pruned by token).
		{"body token equals", strEquals("body", bob), []string{bob}},
		{"body token contains", bodyHasToken("head"), []string{halic, hbare}},
		{"body not equals", strNotEquals("body", bob), []string{alice, bare, halic, hbare}},
		{"body matches", strMatches("body", `^part-`), []string{alice, bob, bare}},
		{"body not matches", strNotMatches("body", `^part-`), []string{halic, hbare}},

		// Equality byte column (the trace-by-id shape): rows without an id hold an EMPTY value,
		// not an absent one — a fixed column is always present.
		{"id equals", strEquals("id", "t-1"), []string{alice, halic}},
		{"id equals empty", strEquals("id", ""), []string{bare, hbare}},
		{"id not equals", strNotEquals("id", "t-1"), []string{bob, bare, hbare}},
		{"id matches", strMatches("id", `^t-`), []string{alice, bob, halic}},
		{"id not matches", strNotMatches("id", `^t-`), []string{bare, hbare}},
		{"id exists", colExists("id"), all},
		{"id unset", colUnset("id"), nil},

		// Per-record attribute: a row without the key is ABSENT, and every predicate that accepts
		// an empty value must select it.
		{"attr equals", strEquals("user", "alice"), []string{alice, halic}},
		{"attr not equals", strNotEquals("user", "alice"), []string{bob, bare, hbare}},
		{"attr matches", strMatches("user", `^a`), []string{alice, halic}},
		{"attr not matches", strNotMatches("user", `^a`), []string{bob, bare, hbare}},
		{"attr exists", colExists("user"), []string{alice, bob, halic}},
		{"attr unset", colUnset("user"), []string{bare, hbare}},

		// A column that is neither in the schema nor in any attrs blob: absent everywhere.
		{"unknown equals", strEquals("nosuch", "x"), nil},
		{"unknown not equals", strNotEquals("nosuch", "x"), all},
		{"unknown exists", colExists("nosuch"), nil},
		{"unknown unset", colUnset("nosuch"), all},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := condFixture(t)

			got := fetchAll(t, e, req("api", tt.cond))
			if len(tt.want) == 0 {
				assert.Empty(t, got)

				return
			}

			require.Len(t, got, 1)
			assert.Equal(t, tt.want, bodies(got[0]))
		})
	}
}

// mkBatchLabeled is [mkBatch] with an arbitrary resource identity, so a stream can carry a label
// another stream lacks.
func mkBatchLabeled(labels [][2]string, recs ...rrec) *recordengine.Batch {
	kvs := make([]signal.KeyValue, 0, len(labels))
	for _, l := range labels {
		kvs = append(kvs, signal.KeyValue{Key: []byte(l[0]), Value: signal.StringValue([]byte(l[1]))})
	}

	series := signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(kvs...)}}

	b := mkBatch("", recs...)
	b.Stream = series.Hash()
	b.Identity = func() signal.Series { return series }

	return b
}

func labelMatcher(name string, match func(signal.Value) bool) fetch.Matcher {
	return fetch.Matcher{Name: []byte(name), Match: match}
}

func TestMatcherOperatorsOverStreamLabels(t *testing.T) {
	t.Parallel()

	streamed := func(t *testing.T, ms ...fetch.Matcher) []string {
		t.Helper()

		// Two streams: only "api" carries env; "web" lacks the label entirely.
		e := newEngine(t, backend.Memory())
		ingest(t, e, mkBatchLabeled([][2]string{{"service.name", "api"}, {"env", "prod"}},
			rrec{ts: 100, body: "api"}))
		ingest(t, e, mkBatchLabeled([][2]string{{"service.name", "web"}},
			rrec{ts: 200, body: "web"}))
		require.NoError(t, e.Flush(context.Background()))

		out := make([]string, 0, 2)
		for _, b := range fetchAll(t, e, fetch.Request{Start: 0, End: 1 << 60, Matchers: ms}) {
			out = append(out, bodies(b)...)
		}

		return out
	}

	eq := func(want string) func(signal.Value) bool {
		return func(v signal.Value) bool { return string(v.Str()) == want }
	}
	ne := func(want string) func(signal.Value) bool {
		return func(v signal.Value) bool { return string(v.Str()) != want }
	}

	tests := []struct {
		name string
		ms   []fetch.Matcher
		want []string
	}{
		{"no matchers", nil, []string{"api", "web"}},
		{"equals", []fetch.Matcher{labelMatcher("env", eq("prod"))}, []string{"api"}},
		{"equals miss", []fetch.Matcher{labelMatcher("env", eq("dev"))}, nil},
		// A stream lacking the label satisfies an inequality — the predicate is offered the empty
		// value and accepts it.
		{"not equals", []fetch.Matcher{labelMatcher("env", ne("prod"))}, []string{"web"}},
		{"not equals other", []fetch.Matcher{labelMatcher("env", ne("dev"))}, []string{"api", "web"}},
		{
			"matches",
			[]fetch.Matcher{labelMatcher("env", func(v signal.Value) bool {
				return regexp.MustCompile(`^pro`).Match(v.Str())
			})},
			[]string{"api"},
		},
		{
			"not matches",
			[]fetch.Matcher{labelMatcher("env", func(v signal.Value) bool {
				return !regexp.MustCompile(`^pro`).Match(v.Str())
			})},
			[]string{"web"},
		},
		{
			"exists",
			[]fetch.Matcher{labelMatcher("env", func(v signal.Value) bool { return v.Kind() != signal.KindEmpty })},
			[]string{"api"},
		},
		{
			"unset",
			[]fetch.Matcher{labelMatcher("env", func(v signal.Value) bool { return v.Kind() == signal.KindEmpty })},
			[]string{"web"},
		},
		// A label no stream carries: every stream is an absent one.
		{
			"unknown label unset",
			[]fetch.Matcher{labelMatcher("nosuch", func(v signal.Value) bool { return v.Kind() == signal.KindEmpty })},
			[]string{"api", "web"},
		},
		{"unknown label equals", []fetch.Matcher{labelMatcher("nosuch", eq("x"))}, nil},
		// Intersection: an absent-accepting matcher ANDed with a present one.
		{
			"intersect present and absent",
			[]fetch.Matcher{labelMatcher("service.name", eq("web")), labelMatcher("env", ne("prod"))},
			[]string{"web"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.ElementsMatch(t, tt.want, streamed(t, tt.ms...))
		})
	}
}
