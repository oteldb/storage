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

func flushEngine() *engine.Engine {
	return engine.New(engine.Config{Backend: backend.Memory(), Prefix: "default/metrics"})
}

func TestFlushThenFetch(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	api := mkSeries("job", "api")
	mustAppend(t, e, api, 100, 1.0)
	mustAppend(t, e, api, 200, 2.0)

	require.NoError(t, e.Flush(context.Background()))
	assert.Equal(t, 1, e.PartCount())
	assert.Equal(t, 1, e.SeriesCount(), "identity survives the flush")

	// The samples now live only in the part, but the query reads them identically.
	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, api.Hash(), got[0].ID)
	assert.Equal(t, []int64{100, 200}, got[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, got[0].Values)
	assert.True(t, api.Equal(got[0].Series))
}

func TestFetchMergesHeadAndParts(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	s := mkSeries("job", "api")
	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(context.Background()))

	// New samples land in the head after the flush.
	mustAppend(t, e, s, 200, 2.0)
	mustAppend(t, e, s, 300, 3.0)

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100, 200, 300}, got[0].Timestamps, "head ∪ part, ts-ordered")
	assert.Equal(t, []float64{1, 2, 3}, got[0].Values)
}

func TestFetchAcrossMultipleParts(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	s := mkSeries("job", "api")

	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(context.Background()))
	mustAppend(t, e, s, 200, 2.0)
	require.NoError(t, e.Flush(context.Background()))
	mustAppend(t, e, s, 300, 3.0)

	assert.Equal(t, 2, e.PartCount())

	got := fetchAll(t, e, fetch.Request{Start: 150, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{200, 300}, got[0].Timestamps, "window prunes the first part's sample")
	assert.Equal(t, []float64{2, 3}, got[0].Values)
}

func TestFlushDuplicateTimestampHeadWins(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	s := mkSeries("job", "api")
	mustAppend(t, e, s, 100, 1.0)
	require.NoError(t, e.Flush(context.Background()))

	// Re-write the same timestamp; the fresher head value supersedes the part's.
	mustAppend(t, e, s, 100, 9.0)

	got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, got, 1)
	assert.Equal(t, []int64{100}, got[0].Timestamps)
	assert.Equal(t, []float64{9}, got[0].Values)
}

func TestFlushEmptyNoop(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	require.NoError(t, e.Flush(context.Background()), "flushing an empty head is a no-op")
	assert.Equal(t, 0, e.PartCount())
}

func TestFetchPartAbsentSeries(t *testing.T) {
	t.Parallel()

	// Each series lives in a different part, so every fetch exercises the part-absent
	// branch for the part that does not hold it.
	e := flushEngine()
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	require.NoError(t, e.Flush(context.Background()))
	mustAppend(t, e, mkSeries("job", "web"), 100, 2.0)
	require.NoError(t, e.Flush(context.Background()))

	api := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "api")}})
	require.Len(t, api, 1)
	assert.Equal(t, []float64{1}, api[0].Values)

	web := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", "web")}})
	require.Len(t, web, 1)
	assert.Equal(t, []float64{2}, web[0].Values)
}

func TestFlushScopeAndResourceLabels(t *testing.T) {
	t.Parallel()

	s := signal.Series{
		Resource: signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
		)},
		Scope: signal.Scope{
			Name:    []byte("otel.lib"),
			Version: []byte("1.2.3"),
			Attributes: signal.NewAttributes(
				signal.KeyValue{Key: []byte("scope.attr"), Value: signal.StringValue([]byte("v"))},
			),
		},
		Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("le"), Value: signal.StringValue([]byte("0.5"))},
		),
	}

	e := flushEngine()
	mustAppend(t, e, s, 100, 7.0)
	require.NoError(t, e.Flush(context.Background()))

	// All identity facets are queryable after a flush, from resource down to scope.
	for _, m := range []fetch.Matcher{
		eqMatcher("service.name", "api"),
		eqMatcher("otel.scope.name", "otel.lib"),
		eqMatcher("otel.scope.version", "1.2.3"),
		eqMatcher("scope.attr", "v"),
		eqMatcher("le", "0.5"),
	} {
		got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{m}})
		require.Len(t, got, 1)
		assert.Equal(t, []float64{7}, got[0].Values)
	}
}

// failWriteBackend wraps a backend but fails every Write, to exercise the flush error path.
type failWriteBackend struct{ backend.Backend }

func (failWriteBackend) Write(context.Context, string, []byte) error {
	return errWriteFailed
}

var errWriteFailed = writeError{}

type writeError struct{}

func (writeError) Error() string { return "write failed" }

func TestFlushWriteError(t *testing.T) {
	t.Parallel()

	e := engine.New(engine.Config{Backend: failWriteBackend{backend.Memory()}, Prefix: "default/metrics"})
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)

	err := e.Flush(context.Background())
	require.Error(t, err, "a backend write failure surfaces from Flush")
	assert.Equal(t, 0, e.PartCount())
}

func TestFlushMultipleSeries(t *testing.T) {
	t.Parallel()

	e := flushEngine()
	mustAppend(t, e, mkSeries("job", "api"), 100, 1.0)
	mustAppend(t, e, mkSeries("job", "web"), 100, 2.0)
	mustAppend(t, e, mkSeries("job", "db"), 100, 3.0)
	require.NoError(t, e.Flush(context.Background()))

	for name, want := range map[string]float64{"api": 1, "web": 2, "db": 3} {
		got := fetchAll(t, e, fetch.Request{Start: 0, End: 1000, Matchers: []fetch.Matcher{eqMatcher("job", name)}})
		require.Len(t, got, 1, name)
		assert.Equal(t, []float64{want}, got[0].Values, name)
	}
}
