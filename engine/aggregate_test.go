package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// aggEngine is a flush-capable engine with the aggregate-stats sidecar enabled, so AggregateRange
// exercises the pushdown fast path.
func aggEngine() *engine.Engine {
	return engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics", AggregateStats: true})
}

// aggFromBatches folds raw fetch batches into the per-series aggregate they imply — the ground
// truth the pushdown path must match.
func aggFromBatches(batches []*fetch.Batch) map[signal.SeriesID]engine.SeriesAgg {
	out := make(map[signal.SeriesID]engine.SeriesAgg, len(batches))
	for _, b := range batches {
		var a engine.SeriesAgg
		for _, v := range b.Values {
			if a.Count == 0 {
				a.Min, a.Max = v, v
			} else {
				a.Min = min(a.Min, v)
				a.Max = max(a.Max, v)
			}
			a.Sum += v
			a.Count++
		}
		out[b.ID] = a
	}

	return out
}

// assertAggMatchesFetch checks AggregateRange returns exactly the aggregate of a raw fetch over the
// same request.
func assertAggMatchesFetch(t *testing.T, e *engine.Engine, r fetch.Request) map[signal.SeriesID]engine.SeriesAgg {
	t.Helper()

	got, err := e.AggregateRange(context.Background(), r)
	require.NoError(t, err)

	want := aggFromBatches(fetchAll(t, e, r))
	assert.Equal(t, want, got, "aggregate must equal the fold of a raw fetch")

	return got
}

func TestAggregateMatchesFetchSinglePart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := aggEngine()
	api := mkSeries("job", "api")
	web := mkSeries("job", "web")
	for ts := int64(1); ts <= 40; ts++ {
		mustAppend(t, e, api, ts, float64(ts))
		mustAppend(t, e, web, ts, float64(100-ts))
	}
	require.NoError(t, e.Flush(ctx))

	got := assertAggMatchesFetch(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, engine.SeriesAgg{Count: 40, Sum: 820, Min: 1, Max: 40}, got[api.Hash()])
}

func TestAggregateMatchesFetchAcrossScenarios(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := aggEngine()
	s := mkSeries("job", "api")

	// Three time-disjoint parts (the pushdown fast path).
	mustAppend(t, e, s, 10, 1)
	mustAppend(t, e, s, 20, 3)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 110, 5)
	mustAppend(t, e, s, 120, 7)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 210, 9)
	require.NoError(t, e.Flush(ctx))
	// Plus unflushed head data, newer than every part.
	mustAppend(t, e, s, 310, 11)

	all := []fetch.Matcher{eqMatcher("job", "api")}

	// Whole range (full coverage, disjoint ⇒ pushdown) and a partial range (⇒ decode fallback).
	assertAggMatchesFetch(t, e, fetch.Request{Start: 0, End: 1000, Matchers: all})
	assertAggMatchesFetch(t, e, fetch.Request{Start: 100, End: 215, Matchers: all})
	assertAggMatchesFetch(t, e, fetch.Request{Start: 0, End: 250, Matchers: all}) // excludes the head sample
}

func TestAggregateDedupsOverlappingParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := aggEngine()
	s := mkSeries("job", "api")

	// Two parts that overlap in time at ts=20 (a re-flush of the same timestamp): the aggregate must
	// dedup (freshest wins), so the pushdown is unsafe and it falls back to decode+merge.
	mustAppend(t, e, s, 10, 1)
	mustAppend(t, e, s, 20, 2)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 20, 9) // same ts, newer value
	mustAppend(t, e, s, 30, 4)
	require.NoError(t, e.Flush(ctx))

	got := assertAggMatchesFetch(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	// ts 10→1, 20→9 (freshest), 30→4: count 3, sum 14, not 4 samples / sum 16.
	assert.Equal(t, engine.SeriesAgg{Count: 3, Sum: 14, Min: 1, Max: 9}, got[s.Hash()])
}

// TestAggregatePushdownAvoidsDecode proves the fast path: with a decode cache attached, a
// fully-covered aggregate never decodes a value column (the sidecar answers it), whereas a partial
// range falls back to decoding.
func TestAggregatePushdownAvoidsDecode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics", DecodeCacheBytes: 1 << 20, AggregateStats: true})
	s := mkSeries("job", "api")
	for ts := int64(1); ts <= 50; ts++ {
		mustAppend(t, e, s, ts, float64(ts))
	}
	require.NoError(t, e.Flush(ctx))

	all := []fetch.Matcher{eqMatcher("job", "api")}

	_, err := e.AggregateRange(ctx, fetch.Request{Start: 0, End: 1000, Matchers: all})
	require.NoError(t, err)
	st, _ := e.DecodeCacheStats()
	assert.Equal(t, int64(0), st.Misses, "full-coverage aggregate answers from the sidecar, no decode")

	_, err = e.AggregateRange(ctx, fetch.Request{Start: 10, End: 40, Matchers: all})
	require.NoError(t, err)
	st, _ = e.DecodeCacheStats()
	assert.Positive(t, st.Misses, "a partial range falls back to decoding")
}
