package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/tenant"
)

// logBatch builds a one-stream Logs batch for service svc with (ts, severity, body) records.
func logBatch(svc string, recs ...[3]any) log.Logs {
	var ld log.Logs
	rl := ld.AddResource()
	rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}
	sl := rl.AddScope()
	sl.Scope = signal.Scope{Name: []byte("lib")}

	for _, r := range recs {
		rec := sl.AddRecord()
		rec.Timestamp = int64(r[0].(int))
		rec.SeverityNumber = int32(r[1].(int))
		rec.Body = []byte(r[2].(string))
	}

	return ld
}

func logSvcMatcher(svc string) fetch.Matcher {
	want := []byte(svc)

	return fetch.Matcher{Name: []byte("service.name"), Match: func(v signal.Value) bool { return bytes.Equal(v.Str(), want) }}
}

func logBodies(t *testing.T, f fetch.Fetcher, r fetch.Request) []string {
	t.Helper()
	it, err := f.Fetch(context.Background(), r)
	require.NoError(t, err)
	got, err := fetch.Drain(context.Background(), it)
	require.NoError(t, err)

	var out []string
	for _, b := range got {
		col, _ := b.Column("body")
		for _, v := range col.Bytes {
			out = append(out, string(v))
		}
	}

	return out
}

func TestFacadeWriteAndQueryLogs(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	acc, err := s.WriteLogs(context.Background(), logBatch("api",
		[3]any{100, 9, "first"}, [3]any{200, 17, "second"}))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 2}, acc)

	_, err = s.WriteLogs(context.Background(), logBatch("web", [3]any{150, 9, "web log"}))
	require.NoError(t, err)

	got := logBodies(t, s.LogFetcher("default"), fetch.Request{
		Start: 0, End: 1000, Matchers: []fetch.Matcher{logSvcMatcher("api")},
	})
	assert.Equal(t, []string{"first", "second"}, got)
}

func TestFacadeLogsTenantIsolation(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithTenant(func(r signal.Resource, _ signal.Scope) signal.TenantID {
		v, _ := r.Attributes.Get([]byte("service.name"))

		return signal.TenantID(v.Str())
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	_, err = s.WriteLogs(context.Background(), logBatch("team-a", [3]any{100, 1, "a-log"}))
	require.NoError(t, err)
	_, err = s.WriteLogs(context.Background(), logBatch("team-b", [3]any{100, 1, "b-log"}))
	require.NoError(t, err)

	all := fetch.Request{Start: 0, End: 1000}
	assert.Equal(t, []string{"a-log"}, logBodies(t, s.LogFetcher("team-a"), all))
	assert.Equal(t, []string{"b-log"}, logBodies(t, s.LogFetcher("team-b"), all))

	// A no-arg LogFetcher spans both tenants (concatenated).
	both := logBodies(t, s.LogFetcher(), all)
	assert.ElementsMatch(t, []string{"a-log", "b-log"}, both)
}

func TestFacadeLogsAndMetricsCoexist(t *testing.T) {
	t.Parallel()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// Logs and metrics share the facade and tenant routing but separate engines/read seams.
	_, err = s.WriteLogs(context.Background(), logBatch("api", [3]any{100, 9, "log line"}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(context.Background(), gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)

	// The metric read seam sees the metric; the log seam sees the log; neither bleeds.
	mIt, err := s.Fetcher("default").Fetch(context.Background(), fetch.Request{
		Start: 0, End: 1000, Matchers: []fetch.Matcher{nameMatcher("http.requests")},
	})
	require.NoError(t, err)
	mGot, err := fetch.Drain(context.Background(), mIt)
	require.NoError(t, err)
	require.Len(t, mGot, 1)
	assert.Equal(t, []float64{1}, mGot[0].Values)

	logs := logBodies(t, s.LogFetcher("default"), fetch.Request{Start: 0, End: 1000})
	assert.Equal(t, []string{"log line"}, logs)
}

func TestFacadeLogsFlushAndRecover(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "buffered"}))
	require.NoError(t, err)
	s.maintain(ctx) // flush the log head to a part

	e, ok := s.lookupLogEngine("default")
	require.True(t, ok)
	assert.Equal(t, 1, e.PartCount(), "maintenance flushed the logs head")

	got := logBodies(t, s.LogFetcher("default"), fetch.Request{Start: 0, End: 1000})
	assert.Equal(t, []string{"buffered"}, got, "served from the flushed part")
}

func TestWriteLogsAdmissionMaxSeries(t *testing.T) {
	t.Parallel()

	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{MaxSeries: 1}}
	})))
	require.NoError(t, err)

	ctx := context.Background()
	a, err := s.WriteLogs(ctx, logBatch("api", [3]any{100, 1, "x"}))
	require.NoError(t, err)
	assert.Equal(t, int64(1), a.Accepted)

	// A second distinct stream (service) exceeds the per-tenant cardinality cap.
	b, err := s.WriteLogs(ctx, logBatch("web", [3]any{100, 1, "y"}))
	require.NoError(t, err)
	assert.Zero(t, b.Accepted)
	assert.Equal(t, int64(1), b.Rejected)
	assert.Equal(t, "max_series", b.RejectedReason)
	assert.Equal(t, int64(1), s.AdmissionStats("default").RejectedCardinality)
}
