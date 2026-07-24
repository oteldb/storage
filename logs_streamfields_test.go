package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/log"
	"github.com/oteldb/storage/tenant"
)

// churnBatch builds one stream per instance id: the same service and pod, differing only in the
// per-process service.instance.id — the high-churn attribute that mints a stream per restart when
// every resource attribute identifies.
func churnBatch(svc string, instances ...string) log.Logs {
	var ld log.Logs

	for i, inst := range instances {
		rl := ld.AddResource()
		rl.Resource = signal.Resource{Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
			signal.KeyValue{Key: []byte("k8s.pod.name"), Value: signal.StringValue([]byte("pod-1"))},
			signal.KeyValue{Key: []byte("service.instance.id"), Value: signal.StringValue([]byte(inst))},
		)}
		sl := rl.AddScope()
		sl.Scope = signal.Scope{Name: []byte("lib")}
		rec := sl.AddRecord()
		rec.Timestamp, rec.Body = int64(100+i), []byte(inst)
	}

	return ld
}

func instanceIDs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("inst-%03d", i)
	}

	return out
}

// TestLogStreamsCollapseUnderDefaultFields is the point of the stream-field policy: a resource
// attribute that churns per process no longer multiplies streams, while its records stay whole.
func TestLogStreamsCollapseUnderDefaultFields(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	ids := instanceIDs(50)
	_, err = s.WriteLogs(ctx, churnBatch("api", ids...))
	require.NoError(t, err)

	series, err := s.LogSeries(ctx, "default", nil, 0, 0)
	require.NoError(t, err)
	require.Len(t, series, 1, "50 instance ids collapse to one stream")

	assert.Len(t, series[0].Resource.Attributes, 2, "identity carries only the stream fields")
	_, ok := series[0].Resource.Attributes.Get([]byte("service.instance.id"))
	assert.False(t, ok, "the churn attribute is not identifying")

	bodies := logBodies(t, s.LogFetcher("default"), fetch.Request{
		Tenant: "default", Signal: signal.Log, Start: 0, End: 1000,
		Matchers: []fetch.Matcher{logSvcMatcher("api")},
	})
	assert.Len(t, bodies, len(ids), "every record is still readable")
}

// TestLogAllFieldsKeepsPerInstanceStreams pins the opt-out: a tenant that declares its resource
// attributes bounded gets the pre-classification identity back.
func TestLogAllFieldsKeepsPerInstanceStreams(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Streams: tenant.Streams{AllFields: true}}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteLogs(ctx, churnBatch("api", instanceIDs(5)...))
	require.NoError(t, err)

	series, err := s.LogSeries(ctx, "default", nil, 0, 0)
	require.NoError(t, err)
	assert.Len(t, series, 5, "every resource attribute identifies")
}

// TestLogExcludedAttrIsQueryableAsCondition is the correctness half: excluding a key from identity
// must not make it unqueryable — it is answered by a column condition over the resource column.
func TestLogExcludedAttrIsQueryableAsCondition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteLogs(ctx, churnBatch("api", instanceIDs(10)...))
	require.NoError(t, err)

	want := []byte("inst-004")
	req := fetch.Request{
		Tenant: "default", Signal: signal.Log, Start: 0, End: 1000,
		Matchers: []fetch.Matcher{logSvcMatcher("api")},
		Conditions: []fetch.Condition{{
			Column: "service.instance.id",
			Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
			Equal:  &fetch.EqualMatcher{Name: "service.instance.id", Value: "inst-004"},
		}},
		AllConditions: true,
	}

	assert.Equal(t, []string{"inst-004"}, logBodies(t, s.LogFetcher("default"), req))

	// An identifying attribute is stored per record too, so the same mechanism answers it — which is
	// what keeps a later change to the stream-field set from breaking queries over older parts.
	req.Conditions = []fetch.Condition{{
		Column: "service.name",
		Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), []byte("api")) },
	}}
	assert.Len(t, logBodies(t, s.LogFetcher("default"), req), 10)
}

// TestLogKeysReportsExcludedAttrAsCondition pins the routing contract an embedder compiles against:
// an identifying key is postings-resolvable, an excluded one is condition-only, and neither key
// carries both mechanisms.
func TestLogKeysReportsExcludedAttrAsCondition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteLogs(ctx, churnBatch("api", instanceIDs(3)...))
	require.NoError(t, err)

	keys, err := s.LogKeys(ctx, "default", 0, 0)
	require.NoError(t, err)

	got := logKeyScopes(keys)
	assert.Equal(t, KeyScopeResource|KeyScopeIndexed, got["service.name"], "identifying ⇒ postings")
	assert.Equal(t, KeyScopeResource|KeyScopeIndexed, got["k8s.pod.name"])
	assert.Equal(t,
		KeyScopeResource|KeyScopeRecord,
		got["service.instance.id"],
		"excluded ⇒ resource by provenance, condition by mechanism",
	)
}

// TestLogStreamFieldChangeStaysQueryable is the property that makes the policy editable: parts
// written under the old field set keep their ids, and a condition answers across the change.
func TestLogStreamFieldChangeStaysQueryable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	fields := []string{"service.instance.id", "service.name"}
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Streams: tenant.Streams{Fields: fields}}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteLogs(ctx, churnBatch("api", "inst-000", "inst-001"))
	require.NoError(t, err)

	// The operator narrows the set; already-written streams keep their ids.
	fields = []string{"service.name"}

	_, err = s.WriteLogs(ctx, churnBatch("api", "inst-002"))
	require.NoError(t, err)

	series, err := s.LogSeries(ctx, "default", nil, 0, 0)
	require.NoError(t, err)
	assert.Len(t, series, 3, "old streams survive the change; new records form one stream")

	want := []byte("inst-000")
	assert.Equal(t, []string{"inst-000"}, logBodies(t, s.LogFetcher("default"), fetch.Request{
		Tenant: "default", Signal: signal.Log, Start: 0, End: 1000,
		Matchers: []fetch.Matcher{logSvcMatcher("api")},
		Conditions: []fetch.Condition{{
			Column: "service.instance.id",
			Match:  func(v signal.Value) bool { return bytes.Equal(v.Str(), want) },
		}},
		AllConditions: true,
	}), "a condition answers a record written before the key left the stream key")
}
