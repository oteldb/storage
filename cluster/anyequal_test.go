package cluster_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func members(vals ...string) [][]byte {
	out := make([][]byte, len(vals))
	for i, v := range vals {
		out[i] = []byte(v)
	}

	return out
}

// TestFetchRequestAnyEqualCodec pins the point of the whole exercise on the wire: a set-membership
// condition must reach the peer, not be dropped at the boundary the way a Match closure is.
func TestFetchRequestAnyEqualCodec(t *testing.T) {
	t.Parallel()

	in := cluster.FetchRequest{
		Signal: signal.Trace, Tenant: "acme", Start: -1, End: 1<<62 - 1,
		Equal: []fetch.EqualMatcher{{Name: "service.name", Value: "api"}},
		Conditions: []cluster.ConditionHint{
			{Column: "body", Equal: fetch.EqualMatcher{Name: "body", Value: "oops"}},
			{Column: "trace_id", AnyEqual: members("t-1", "t-2", "t-3")},
		},
	}

	got, err := cluster.ParseFetchRequest(in.Encode())
	require.NoError(t, err)
	assert.Equal(t, in, got, "the sets round-trip alongside the equality hints")
}

// TestFetchRequestAnyEqualWireCompatibility pins the rolling upgrade: the sets are a second
// append-only tail, so a peer that predates them reads the prefix it knows and answers with the
// superset it always did.
func TestFetchRequestAnyEqualWireCompatibility(t *testing.T) {
	t.Parallel()

	base := cluster.FetchRequest{
		Signal: signal.Trace, Tenant: "acme", Start: 1, End: 2,
		Conditions: []cluster.ConditionHint{{Column: "trace_id", Equal: fetch.EqualMatcher{Name: "trace_id", Value: "t-1"}}},
	}

	withSets := base
	withSets.Conditions = []cluster.ConditionHint{{
		Column:   "trace_id",
		Equal:    fetch.EqualMatcher{Name: "trace_id", Value: "t-1"},
		AnyEqual: members("t-1", "t-2"),
	}}

	encoded := withSets.Encode()
	assert.True(t, bytes.HasPrefix(encoded, base.Encode()),
		"the sets are an append-only tail on top of the hints")
	assert.Greater(t, len(encoded), len(base.Encode()), "the tail carries the set")

	got, err := cluster.ParseFetchRequest(base.Encode())
	require.NoError(t, err)
	assert.Empty(t, got.Conditions[0].AnyEqual, "a request from a peer that predates sets carries none")

	_, err = cluster.ParseFetchRequest(append(base.Encode(), 0x01))
	require.Error(t, err, "a truncated set tail is rejected, not silently dropped")
}

// TestFetchRequestAnyEqualNormalizesOnDecode pins that a set arriving out of order is normalized
// before it drives the peer's binary search — the sender is another process.
func TestFetchRequestAnyEqualNormalizesOnDecode(t *testing.T) {
	t.Parallel()

	in := cluster.FetchRequest{
		Signal: signal.Log, Tenant: "acme",
		Conditions: []cluster.ConditionHint{{Column: "id", AnyEqual: members("c", "a", "b", "a")}},
	}

	got, err := cluster.ParseFetchRequest(in.Encode())
	require.NoError(t, err)
	assert.Equal(t, members("a", "b", "c"), got.Conditions[0].AnyEqual)
}

func TestConditionHintsCarriesSets(t *testing.T) {
	t.Parallel()

	set := fetch.AnyEqualStrings([]string{"t-2", "t-1"})
	conds := []fetch.Condition{
		{Column: "body", Tokens: [][]byte{[]byte("oops")}},
		{Column: "trace_id", AnyEqual: set},
	}

	hints := cluster.ConditionHints(conds, true)
	assert.Equal(t, []cluster.ConditionHint{{Column: "trace_id", AnyEqual: set}}, hints,
		"a set-only condition is pushable even without an equality")

	assert.Nil(t, cluster.ConditionHints(conds, false),
		"conditions are pushable only under AllConditions")
}

// TestSetOnlyHintRebuildsAPredicate pins the peer side: a hint with no equality still reconstructs a
// usable condition, so the peer prunes and filters instead of scanning its whole window.
func TestSetOnlyHintRebuildsAPredicate(t *testing.T) {
	t.Parallel()

	set := fetch.AnyEqualStrings([]string{"t-1", "t-2"})

	r := cluster.FetchRequest{
		Signal:     signal.Trace,
		Conditions: []cluster.ConditionHint{{Column: "trace_id", AnyEqual: set}},
	}.Request()

	require.Len(t, r.Conditions, 1)
	require.True(t, r.AllConditions)

	c := r.Conditions[0]
	assert.Equal(t, set, c.AnyEqual)
	assert.Nil(t, c.Equal, "a set-only hint carries no equality")
	require.NotNil(t, c.Match)
	assert.True(t, c.Match(signal.StringValue([]byte("t-1"))))
	assert.False(t, c.Match(signal.StringValue([]byte("t-9"))))
}
