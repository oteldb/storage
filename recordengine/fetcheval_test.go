package recordengine_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// countingCond counts how often the engine actually invokes a condition's predicate.
func countingCond(column string, calls *int, match func(signal.Value) bool) fetch.Condition {
	return fetch.Condition{
		Column: column,
		Match: func(v signal.Value) bool {
			*calls++

			return match(v)
		},
	}
}

func eqPredicate(want string) func(signal.Value) bool {
	w := []byte(want)

	return func(v signal.Value) bool { return bytes.Equal(v.Str(), w) }
}

// dictRows returns rows cycling through distinct bodies/attribute values, so a flushed part's byte
// columns dictionary-encode to exactly `distinct` entries.
func dictRows(rows, distinct int, withAttr bool) []rrec {
	out := make([]rrec, rows)
	for i := range out {
		v := fmt.Sprintf("v%d", i%distinct)
		out[i] = rrec{ts: int64(i + 1), body: v}

		if withAttr {
			out[i].attr = [2]string{"user", v}
		}
	}

	return out
}

// A dictionary-encoded column's predicate must run once per distinct entry, not once per row: the
// columns hold at most 65536 entries, so this is what keeps an expensive predicate (a regex, or an
// attribute lookup that re-parses the attrs blob) off the per-row path.
func TestPartConditionRunsPerDictionaryEntry(t *testing.T) {
	t.Parallel()

	const (
		rows     = 500
		distinct = 5
	)

	for _, tt := range []struct {
		name   string
		column string
		attr   bool
	}{
		{name: "fixed column", column: "body"},
		{name: "attribute", column: "user", attr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEngine(t, backend.Memory())
			ingest(t, e, mkBatch("api", dictRows(rows, distinct, tt.attr)...))
			require.NoError(t, e.Flush(context.Background()))

			calls := 0
			got := fetchAll(t, e, req("api", countingCond(tt.column, &calls, eqPredicate("v2"))))

			require.Len(t, got, 1)
			assert.Len(t, got[0].Timestamps, rows/distinct)
			assert.Equal(t, distinct, calls, "one call per distinct dictionary entry, not per row")
		})
	}
}

// The memo is filled lazily, so a high-selectivity scan that touches a handful of rows must not pay
// a predicate call for every entry in the dictionary.
func TestPartConditionEvaluatesOnlyTouchedEntries(t *testing.T) {
	t.Parallel()

	const (
		rows     = 500
		distinct = 50
	)

	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", dictRows(rows, distinct, false)...))
	require.NoError(t, e.Flush(context.Background()))

	// The window selects the first 3 rows, which reference 3 of the 50 entries.
	calls := 0
	r := req("api", countingCond("body", &calls, eqPredicate("v0")))
	r.Start, r.End = 1, 3

	got := fetchAll(t, e, r)
	require.Len(t, got, 1)
	assert.Equal(t, []int64{1}, got[0].Timestamps)
	assert.Equal(t, 3, calls, "only the entries the scanned rows reference are evaluated")
}

// Part rows are matched during the scan, so the post-scan filter must re-check only the head-seeded
// prefix — and must still drop the head rows that do not match.
func TestHeadPrefixIsFilteredWithoutRecheckingPartRows(t *testing.T) {
	t.Parallel()

	const (
		partRows = 200
		distinct = 4
	)

	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api", dictRows(partRows, distinct, false)...))
	require.NoError(t, e.Flush(context.Background()))

	// Two head rows on top of the flushed part: one matching, one not.
	ingest(t, e, mkBatch("api",
		rrec{ts: partRows + 1, body: "v0"},
		rrec{ts: partRows + 2, body: "other"},
	))

	calls := 0
	got := fetchAll(t, e, req("api", countingCond("body", &calls, eqPredicate("v0"))))

	require.Len(t, got, 1)
	assert.Len(t, got[0].Timestamps, partRows/distinct+1)
	assert.Equal(t, int64(partRows+1), got[0].Timestamps[len(got[0].Timestamps)-1])

	// distinct dictionary entries for the part + one call per head row. A part row re-checked by the
	// post-scan filter would push this to partRows + 2.
	assert.Equal(t, distinct+2, calls)
}

// Conditions are reordered cheap-first per part; the result must be identical whichever order the
// caller supplies them in, and every predicate must still see the value its own column holds.
func TestMultipleConditionsAreOrderIndependent(t *testing.T) {
	t.Parallel()

	e := newEngine(t, backend.Memory())
	ingest(t, e, mkBatch("api",
		rrec{ts: 1, sev: 9, body: "keep", id: "a", attr: [2]string{"user", "alice"}},
		rrec{ts: 2, sev: 1, body: "keep", id: "a", attr: [2]string{"user", "alice"}},
		rrec{ts: 3, sev: 9, body: "drop", id: "a", attr: [2]string{"user", "alice"}},
		rrec{ts: 4, sev: 9, body: "keep", id: "b", attr: [2]string{"user", "alice"}},
		rrec{ts: 5, sev: 9, body: "keep", id: "a", attr: [2]string{"user", "bob"}},
	))
	require.NoError(t, e.Flush(context.Background()))

	conds := []fetch.Condition{
		sevAtLeast(5),
		bodyContains("keep"),
		idEquals("a"),
		attrEquals("user", "alice"),
	}

	for i := range conds {
		rotated := append(append([]fetch.Condition(nil), conds[i:]...), conds[:i]...)

		got := fetchAll(t, e, req("api", rotated...))
		require.Len(t, got, 1)
		assert.Equal(t, []int64{1}, got[0].Timestamps)
	}
}
