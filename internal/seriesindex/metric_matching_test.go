package seriesindex_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestMetricMatchingUnionsOTelSources(t *testing.T) {
	t.Parallel()
	kv := func(value string) signal.Attributes {
		return signal.NewAttributes(signal.KeyValue{Key: []byte("shared"), Value: signal.StringValue([]byte(value))})
	}
	identity := signal.Series{Attributes: kv("point"), Resource: signal.Resource{Attributes: kv("resource")}, Scope: signal.Scope{Attributes: kv("scope")}}
	b := backend.Memory()
	reference := engine.New(engine.Config{Backend: b, Prefix: "matching"})
	t.Cleanup(func() { require.NoError(t, reference.Close(context.Background())) })
	_, err := reference.Append(identity, 10, 1)
	require.NoError(t, err)
	builder := &seriesindex.Builder{}
	require.NoError(t, builder.Add(identity.Hash(), identity, seriesindex.Metadata{Covered: 10}))
	require.NoError(t, builder.WriteObject(t.Context(), b, "index"))
	idx, err := seriesindex.Open(t.Context(), b, "index")
	require.NoError(t, err)
	for _, flushed := range []bool{false, true} {
		if flushed {
			require.NoError(t, reference.Flush(t.Context()))
		}
		for _, value := range []string{"point", "resource", "scope", "missing"} {
			matcher := fetch.Matcher{Name: []byte("shared"), Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == value }}
			it, err := reference.Fetch(t.Context(), fetch.Request{Start: 0, End: 20, Matchers: []fetch.Matcher{matcher}})
			require.NoError(t, err)
			rows, err := fetch.Drain(t.Context(), it)
			require.NoError(t, err)
			refs, err := idx.Resolve(t.Context(), []fetch.Matcher{matcher})
			require.NoError(t, err)
			require.Equal(t, len(rows) > 0, refs.Next())
			require.Equal(t, len(rows) > 0, seriesindex.MatchesSeries(identity, []fetch.Matcher{matcher}))
		}
	}
}
