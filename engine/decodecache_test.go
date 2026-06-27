package engine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
)

func cacheEngine() *engine.Engine {
	return engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics", DecodeCacheBytes: 1 << 20})
}

// TestDecodeCacheHitsAcrossFetches confirms a part decodes once and later fetches hit the cache.
func TestDecodeCacheHitsAcrossFetches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := cacheEngine()
	s := mkSeries("job", "api")
	for ts := int64(1); ts <= 50; ts++ {
		mustAppend(t, e, s, ts, float64(ts))
	}
	require.NoError(t, e.Flush(ctx))

	req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}
	first := fetchAll(t, e, req)
	require.Len(t, first, 1)
	assert.Len(t, first[0].Timestamps, 50)

	// Re-fetch several times; the part is decoded once and served from the cache thereafter.
	for range 5 {
		got := fetchAll(t, e, req)
		require.Len(t, got, 1)
		assert.Equal(t, first[0].Values, got[0].Values, "cached decode returns identical data")
	}

	st, ok := e.DecodeCacheStats()
	require.True(t, ok)
	assert.Equal(t, 1, st.Items, "exactly one part cached")
	assert.Positive(t, st.Hits, "later fetches hit the cache")
	assert.Equal(t, int64(1), st.Misses, "the part is decoded exactly once")
}

// TestDecodeCacheEvictsRetiredParts confirms that compaction drops the source parts' decoded
// entries (they will never be read again) and the merged part caches afresh.
func TestDecodeCacheEvictsRetiredParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	e := cacheEngine()
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 10, 1)
	require.NoError(t, e.Flush(ctx))
	mustAppend(t, e, s, 20, 2)
	require.NoError(t, e.Flush(ctx))
	require.Equal(t, 2, e.PartCount())

	req := fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}
	fetchAll(t, e, req) // caches both parts (prefetch warms ≥2 touched parts)

	st, _ := e.DecodeCacheStats()
	assert.Equal(t, 2, st.Items, "both flushed parts cached")

	require.NoError(t, e.Merge(ctx, 0)) // compacts to one part, retiring + reclaiming the two sources
	require.Equal(t, 1, e.PartCount())

	st, _ = e.DecodeCacheStats()
	assert.Equal(t, 0, st.Items, "retired source parts evicted from the decode cache")

	got := fetchAll(t, e, req)
	require.Len(t, got, 1)
	assert.Equal(t, []int64{10, 20}, got[0].Timestamps, "merged data intact")

	st, _ = e.DecodeCacheStats()
	assert.Equal(t, 1, st.Items, "the merged part is cached on the next fetch")
}

// TestDecodeCacheMatchesUncached is a differential check: a cache-enabled engine returns exactly the
// same data as a plain one across several parts and series.
func TestDecodeCacheMatchesUncached(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cached := cacheEngine()
	plain := engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})

	for _, e := range []*engine.Engine{cached, plain} {
		for f := range 3 { // three parts
			for i := range 20 {
				ts := int64(f*100 + i)
				mustAppend(t, e, mkSeries("job", "api"), ts, float64(ts))
				mustAppend(t, e, mkSeries("job", "web"), ts, float64(ts*2))
			}
			require.NoError(t, e.Flush(ctx))
		}
	}

	req := fetch.Request{Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{eqMatcher("job", "api")}}
	gotCached := fetchAll(t, cached, req)
	gotPlain := fetchAll(t, plain, req)

	require.Len(t, gotCached, 1)
	require.Len(t, gotPlain, 1)
	assert.Equal(t, gotPlain[0].Timestamps, gotCached[0].Timestamps)
	assert.Equal(t, gotPlain[0].Values, gotCached[0].Values)
}
