package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/signal/metric"
)

// durableBackend wraps the memory backend but reports itself non-ephemeral, to exercise
// the [Storage.Reset] ephemeral gate without touching disk.
type durableBackend struct{ backend.Backend }

func (durableBackend) IsEphemeral() bool { return false }

// gaugeBatch builds a one-gauge internal batch under resource service.name=service.
func gaugeBatch(service, name string, ts []int64, values []float64) metric.Metrics {
	var md metric.Metrics
	rm := md.AddResource()
	rm.Resource = signal.Resource{
		Attributes: signal.NewAttributes(signal.KeyValue{
			Key: []byte("service.name"), Value: signal.StringValue([]byte(service)),
		}),
	}
	mt := rm.AddScope().AddMetric()
	mt.Name = []byte(name)
	mt.Kind = metric.KindGauge

	for i := range ts {
		p := mt.AddPoint()
		p.Ts = ts[i]
		p.Value = values[i]
	}

	return md
}

func nameMatcher(name string) fetch.Matcher {
	want := []byte(name)

	return fetch.Matcher{Name: metric.LabelName, Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), want) }}
}

func queryEngine(t *testing.T, e *engine.Engine, m fetch.Matcher) []*fetch.Batch {
	t.Helper()
	it, err := e.Fetch(context.Background(), fetch.Request{Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{m}})
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	return got
}

func TestWriteMetricsAndFetch(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	acc, err := s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)
	assert.Equal(t, int64(2), acc.Accepted)
	assert.Equal(t, int64(0), acc.Rejected)

	batches := queryEngine(t, mustEngine(s.engineFor("default")), nameMatcher("http.requests"))
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{100, 200}, batches[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, batches[0].Values)
	// The folded __name__ label is in the reconstructed identity.
	nv, ok := batches[0].Series.Attributes.Get(metric.LabelName)
	require.True(t, ok)
	assert.Equal(t, []byte("http.requests"), nv.Str())
}

func TestWriteMetricsFlushThenFetch(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)

	_, err = s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)

	eng := mustEngine(s.engineFor("default"))
	require.NoError(t, eng.Flush(context.Background()))
	assert.Equal(t, 1, eng.PartCount())

	// Ingest more after the flush; the query reads head ∪ part transparently.
	_, err = s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", []int64{300}, []float64{3}))
	require.NoError(t, err)

	batches := queryEngine(t, eng, nameMatcher("http.requests"))
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{100, 200, 300}, batches[0].Timestamps)
	assert.Equal(t, []float64{1, 2, 3}, batches[0].Values)
}

func TestMultiTenantRouting(t *testing.T) {
	t.Parallel()

	// Route each record to a tenant named after its service.
	s, err := InMemory(WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("service.name"))

		return signal.TenantID(v.Str())
	}))
	require.NoError(t, err)

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1}, []float64{1}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("web", "m", []int64{1}, []float64{2}))
	require.NoError(t, err)

	apiEng, webEng := mustEngine(s.engineFor("api")), mustEngine(s.engineFor("web"))
	assert.Equal(t, 1, apiEng.SeriesCount())
	assert.Equal(t, 1, webEng.SeriesCount())

	apiBatches := queryEngine(t, apiEng, nameMatcher("m"))
	webBatches := queryEngine(t, webEng, nameMatcher("m"))
	require.Len(t, apiBatches, 1)
	require.Len(t, webBatches, 1)
	// Different services ⇒ different identities, isolated per tenant.
	assert.NotEqual(t, apiBatches[0].ID, webBatches[0].ID)
	assert.InDelta(t, 1.0, apiBatches[0].Values[0], 0)
	assert.InDelta(t, 2.0, webBatches[0].Values[0], 0)
}

func TestRecoverFlushedDataOnReopen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ctx := context.Background()

	// Process 1: ingest over a durable (file) backend, then Close (which flushes to disk).
	be1, err := file.New(dir)
	require.NoError(t, err)
	s1, err := Open(ctx, Options{}, WithBackend(be1))
	require.NoError(t, err)
	_, err = s1.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)
	require.NoError(t, s1.Close(ctx))

	// Process 2: a fresh Storage over the same directory recovers the flushed data at Open.
	be2, err := file.New(dir)
	require.NoError(t, err)
	s2, err := Open(ctx, Options{}, WithBackend(be2))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close(ctx) })

	it, err := s2.Fetcher("default").Fetch(ctx, fetch.Request{
		Start: 0, End: 1 << 60, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.NoError(t, err)
	batches, err := fetch.Drain(ctx, it)
	require.NoError(t, err)
	require.Len(t, batches, 1, "the previous process's flushed series is served after recovery")
	assert.Equal(t, []int64{100, 200}, batches[0].Timestamps)
	assert.Equal(t, []float64{1, 2}, batches[0].Values)

	// Labels were reconstructed from the identity index, not just ids.
	nv, ok := batches[0].Series.Attributes.Get(metric.LabelName)
	require.True(t, ok)
	assert.Equal(t, []byte("http.requests"), nv.Str())
}

func TestResetClearsData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100, 200}, []float64{1, 2}))
	require.NoError(t, err)
	eng := mustEngine(s.engineFor("default"))
	require.NoError(t, eng.Flush(ctx))
	require.Equal(t, 1, eng.PartCount())
	require.Equal(t, 1, eng.SeriesCount())

	require.NoError(t, s.Reset(ctx))
	assert.Equal(t, 0, eng.PartCount(), "flushed parts dropped")
	assert.Equal(t, 0, eng.SeriesCount(), "head index cleared")
	assert.Empty(t, queryEngine(t, eng, nameMatcher("http.requests")))

	// The store is reusable: writing after reset works and only the new data is visible.
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{300}, []float64{3}))
	require.NoError(t, err)
	batches := queryEngine(t, eng, nameMatcher("http.requests"))
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{300}, batches[0].Timestamps)
	assert.Equal(t, []float64{3}, batches[0].Values)
}

func TestResetRequiresEphemeral(t *testing.T) {
	t.Parallel()

	s, err := Open(context.Background(), Options{}, WithBackend(durableBackend{backend.Memory()}))
	require.NoError(t, err)

	require.ErrorIs(t, s.Reset(context.Background()), ErrNotEphemeral)
}

func TestResetAfterCloseRejected(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	require.NoError(t, s.Close(context.Background()))

	require.ErrorIs(t, s.Reset(context.Background()), ErrClosed)
}

func TestWriteAfterCloseRejected(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	require.NoError(t, s.Close(context.Background()))
	require.NoError(t, s.Close(context.Background()), "Close is idempotent")

	ctx := context.Background()
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{1}, []float64{1}))
	require.ErrorIs(t, err, ErrClosed)
	_, err = s.WriteLogs(ctx, log.Logs{})
	require.ErrorIs(t, err, ErrClosed)
	// After Close, the read seam yields an empty fetcher (not an error).
	got, err := fetch.Drain(ctx, mustFetch(t, s.Fetcher("t")))
	require.NoError(t, err)
	assert.Empty(t, got)
}

// mustFetch runs a match-all fetch over f and returns the iterator.
func mustFetch(t *testing.T, f fetch.Fetcher) fetch.Iterator {
	t.Helper()
	it, err := f.Fetch(context.Background(), fetch.Request{Start: 0, End: 1 << 62})
	require.NoError(t, err)

	return it
}

func TestOOOWindowViaStorage(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithOOOWindow(50))
	require.NoError(t, err)
	ctx := context.Background()

	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{100}, []float64{1}))
	require.NoError(t, err)

	acc, err := s.WriteMetrics(ctx, gaugeBatch("api", "m", []int64{40}, []float64{2})) // older than 100-50
	require.NoError(t, err)
	assert.Equal(t, int64(0), acc.Accepted)
	assert.Equal(t, int64(1), acc.Rejected)
}

// mustEngine unwraps an *EngineFor result in a test/benchmark, panicking on a creation error (the
// constructors only fail when a WAL cannot be opened, which these callers do not configure). It
// takes the (engine, error) pair as a sole multi-value argument: mustEngine(s.engineFor(tid)).
func mustEngine[E any](e E, err error) E {
	if err != nil {
		panic(err)
	}

	return e
}
