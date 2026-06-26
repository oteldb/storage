package engine_test

import (
	"bytes"
	"context"
	"regexp"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/wal"
)

func mkSeries(pairs ...string) signal.Series {
	kvs := make([]signal.KeyValue, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		kvs = append(kvs, signal.KeyValue{Key: []byte(pairs[i]), Value: signal.StringValue([]byte(pairs[i+1]))})
	}

	return signal.Series{Attributes: signal.NewAttributes(kvs...)}
}

func eqMatcher(name, value string) fetch.Matcher {
	want := []byte(value)

	return fetch.Matcher{Name: []byte(name), Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), want) }}
}

func fetchAll(t *testing.T, e *engine.Engine, r fetch.Request) []*fetch.Batch {
	t.Helper()
	it, err := e.Fetch(context.Background(), r)
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	return got
}

func TestAppendAndFetch(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{})
	api := mkSeries("job", "api", "inst", "1")
	web := mkSeries("job", "web")

	mustAppend(t, e, api, 100, 1.0)
	mustAppend(t, e, api, 200, 2.0)
	mustAppend(t, e, web, 150, 9.0)

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, api.Hash(), got[0].ID)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, got[0].Values)
	assert.True(t, api.Equal(got[0].Series), "batch carries the identity")

	// No matchers ⇒ every series.
	assert.Len(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1000}), 2)
	// Unknown label ⇒ nothing.
	assert.Empty(t, fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("missing", "x")}}))
	assert.Equal(t, 2, e.SeriesCount())
}

func TestFetchWindowAndSort(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{})
	s := mkSeries("job", "api")
	// Out-of-arrival-order timestamps; fetch sorts them.
	mustAppend(t, e, s, 300, 3.0)
	mustAppend(t, e, s, 100, 1.0)
	mustAppend(t, e, s, 200, 2.0)

	got := fetchAll(t, e, fetch.Request{Start: 150, End: 300, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{200, 300}, got[0].Timestamps, "sorted, windowed")
	assert.Equal(t, []float64{2, 3}, got[0].Values)
}

func TestRegexpMatcherCallback(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{})
	mustAppend(t, e, mkSeries("job", "api"), 1, 1)
	mustAppend(t, e, mkSeries("job", "auth"), 1, 1)
	mustAppend(t, e, mkSeries("job", "web"), 1, 1)

	re := regexp.MustCompile("^(?:a.*)$")
	m := fetch.Matcher{Name: []byte("job"), Match: func(v signal.Value) bool { return re.Match(v.Str()) }}

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 10, Matchers: []fetch.Matcher{m}})
	assert.Len(t, got, 2, "api and auth match a.*")
}

func TestOutOfOrderRejected(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{OOOWindow: 50})
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 100, 1.0)
	mustAppend(t, e, s, 80, 2.0) // within window (100-50=50)

	ok, err := e.Append(s, 40, 3.0) // older than 50 ⇒ rejected
	require.NoError(t, err)
	assert.False(t, ok)

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{80, 100}, got[0].Timestamps, "the OOO sample was dropped")
}

func TestWALReplayReconstructs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sw, err := wal.Create(dir, 64) // tiny ⇒ rotation
	require.NoError(t, err)

	src := engine.New(engine.Config{WAL: sw})
	series := []signal.Series{mkSeries("job", "api"), mkSeries("job", "web"), mkSeries("job", "db")}
	for i, s := range series {
		mustAppend(t, src, s, int64(100+i), float64(i))
		mustAppend(t, src, s, int64(200+i), float64(i*10))
	}
	require.NoError(t, sw.Close())

	// A fresh engine replays the WAL and answers the same query.
	restored := engine.New(engine.Config{})
	require.NoError(t, restored.Replay(dir))
	assert.Equal(t, 3, restored.SeriesCount())

	got := fetchAll(t, restored, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "web")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{101, 201}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 10}, got[0].Values)
}

func mustAppend(t *testing.T, e *engine.Engine, s signal.Series, ts int64, v float64) {
	t.Helper()
	ok, err := e.Append(s, ts, v)
	require.NoError(t, err)
	require.True(t, ok)
}

// TestConcurrentFetchAfterWrite guards the engine's concurrent-read contract: the label index
// sorts lazily on the first read after a write, so several fetches issued at once (as
// split-by-interval does) must not race on that in-place sort. Run under `go test -race`.
func TestConcurrentFetchAfterWrite(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{})
	for i := range 50 {
		mustAppend(t, e, mkSeries("job", "api", "shard", string(rune('a'+i%5))), int64(100+i), float64(i))
	}

	const readers = 8

	var (
		wg     sync.WaitGroup
		counts [readers]int
		errs   [readers]error
	)

	wg.Add(readers)

	for i := range readers {
		go func(i int) {
			defer wg.Done()

			it, err := e.Fetch(context.Background(), fetch.Request{
				Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")},
			})
			if err != nil {
				errs[i] = err

				return
			}

			got, err := fetch.Drain(context.Background(), it)
			errs[i], counts[i] = err, len(got)
		}(i)
	}

	wg.Wait()

	for i := range readers {
		require.NoError(t, errs[i])
		assert.Equal(t, 5, counts[i], "every concurrent reader resolves all five job=api series")
	}
}
