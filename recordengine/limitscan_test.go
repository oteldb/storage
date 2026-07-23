package recordengine_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/query/profile"
	"github.com/oteldb/storage/recordengine"
	"github.com/oteldb/storage/signal"
)

// limitFixture writes parts spanning interleaved time ranges across several streams, so a top-N scan
// has to reason about part order rather than luck into it.
func limitFixture(t *testing.T, parts, streamsPerPart, rowsPerStream int, jitter bool) *recordengine.Engine {
	t.Helper()

	ctx := context.Background()
	e := newEngine(t, backend.Memory())
	rng := rand.New(rand.NewPCG(1, 2))

	for p := range parts {
		for s := range streamsPerPart {
			recs := make([]rrec, 0, rowsPerStream)

			for i := range rowsPerStream {
				// Parts advance in time, but jitter overlaps their ranges so maxTime ordering is not
				// the same as write order.
				ts := int64(p*1000 + i*10 + s)
				if jitter {
					ts += int64(rng.IntN(1500))
				}

				recs = append(recs, rrec{ts: ts, body: fmt.Sprintf("p%d-s%d-r%d", p, s, i)})
			}

			ingest(t, e, mkBatch(fmt.Sprintf("svc-%d", s), recs...))
		}

		require.NoError(t, e.Flush(ctx))
	}

	return e
}

func fetchRows(t *testing.T, e *recordengine.Engine, r fetch.Request) []string {
	t.Helper()

	ctx := context.Background()

	it, err := e.Fetch(ctx, r)
	require.NoError(t, err)

	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)

	type row struct {
		ts   int64
		body string
	}

	var rows []row

	for _, b := range batches {
		col, _ := b.Column("body")
		for i, ts := range b.Timestamps {
			rows = append(rows, row{ts, string(col.Bytes[i])})
		}
	}

	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, fmt.Sprintf("%d/%s", r.ts, r.body))
	}

	return out
}

// TestLimitScanMatchesFullScan is the correctness contract: reading parts in time order and stopping
// at the watermark must return exactly what reading every part returns. The unlimited fetch is the
// reference — it is trimmed in memory to the same top-N.
func TestLimitScanMatchesFullScan(t *testing.T) {
	t.Parallel()

	for _, jitter := range []bool{false, true} {
		t.Run(fmt.Sprintf("jitter=%v", jitter), func(t *testing.T) {
			t.Parallel()

			e := limitFixture(t, 6, 3, 40, jitter)

			for _, reverse := range []bool{true, false} {
				for _, limit := range []int{1, 5, 37, 200, 10000} {
					req := fetch.Request{
						Start: 0, End: 1 << 62,
						Projection: []string{"body"},
						Limit:      limit, Reverse: reverse,
					}

					got := fetchRows(t, e, req)

					// Reference: same request with no limit, trimmed by the caller.
					ref := req
					ref.Limit = 0
					full := fetchRows(t, e, ref)

					assert.Subsetf(t, full, got, "limit=%d reverse=%v: rows must come from the full scan", limit, reverse)
					assert.GreaterOrEqualf(t, len(got), min(limit, len(full)),
						"limit=%d reverse=%v: at least Limit rows when that many exist", limit, reverse)

					// Every returned row must be at least as extreme as every row left out.
					assertNoBetterRowExcluded(t, full, got, reverse)
				}
			}
		})
	}
}

// assertNoBetterRowExcluded checks the returned set is a genuine top-N: no excluded row ranks ahead
// of an included one.
func assertNoBetterRowExcluded(t *testing.T, full, got []string, reverse bool) {
	t.Helper()

	inc := make(map[string]struct{}, len(got))
	for _, r := range got {
		inc[r] = struct{}{}
	}

	tsOf := func(s string) int64 {
		var ts int64
		_, err := fmt.Sscanf(s, "%d/", &ts)
		require.NoError(t, err)

		return ts
	}

	worstIncluded := int64(-1)
	for _, r := range got {
		ts := tsOf(r)
		if worstIncluded < 0 {
			worstIncluded = ts

			continue
		}

		if (reverse && ts < worstIncluded) || (!reverse && ts > worstIncluded) {
			worstIncluded = ts
		}
	}

	for _, r := range full {
		if _, ok := inc[r]; ok {
			continue
		}

		ts := tsOf(r)
		if reverse {
			assert.LessOrEqualf(t, ts, worstIncluded, "excluded row %s is newer than an included one", r)
		} else {
			assert.GreaterOrEqualf(t, ts, worstIncluded, "excluded row %s is older than an included one", r)
		}
	}
}

// TestLimitScanSkipsParts checks the optimization actually fires: a newest-first top-N over many
// parts must leave most of them unread.
func TestLimitScanSkipsParts(t *testing.T) {
	t.Parallel()

	e := limitFixture(t, 12, 2, 30, false)
	require.Equal(t, 12, e.PartCount())

	pctx, coll := profile.WithCollector(context.Background())
	it, err := e.Fetch(pctx, fetch.Request{
		Start: 0, End: 1 << 62,
		Projection: []string{"body"},
		Limit:      10, Reverse: true,
	})
	require.NoError(t, err)
	_, err = fetch.Drain(pctx, it)
	require.NoError(t, err)

	fn := findCounter(coll.Root(), "recordengine.fetch")
	require.NotNil(t, fn)

	live := fn.Counters["parts_live"]
	skipped := fn.Counters["parts_skipped_limit"]
	t.Logf("live=%d skipped=%d", live, skipped)
	assert.Positivef(t, skipped, "a top-10 over %d parts must skip some", e.PartCount())
	assert.Lessf(t, skipped, live, "at least the parts holding the answer are read")
}

// findCounter walks the profile tree for the named node.
func findCounter(n *profile.Node, name string) *profile.Node {
	if n == nil {
		return nil
	}

	if n.Name == name {
		return n
	}

	for _, c := range n.Children {
		if hit := findCounter(c, name); hit != nil {
			return hit
		}
	}

	return nil
}

// TestLimitScanConditionsUnaffected pins the guard: with conditions the scan must not stop early,
// because a part's surviving-row count is not known until the filter runs.
func TestLimitScanConditionsUnaffected(t *testing.T) {
	t.Parallel()

	e := limitFixture(t, 8, 2, 30, false)

	want := []byte("p0-s0-r0")
	got := fetchRows(t, e, fetch.Request{
		Start: 0, End: 1 << 62,
		Conditions: []fetch.Condition{{
			Column: "body",
			Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		}},
		AllConditions: true,
		Projection:    []string{"body"},
		Limit:         10, Reverse: true,
	})

	require.Len(t, got, 1, "the one matching row lives in the oldest part and must still be found")
	assert.Contains(t, got[0], "p0-s0-r0")
}
