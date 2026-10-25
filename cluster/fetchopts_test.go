package cluster_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func TestFetchOptionsCodec(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		r    cluster.FetchRequest
	}{
		{"projection", cluster.FetchRequest{Projection: []string{"value", "stack_id"}}},
		{"limit", cluster.FetchRequest{Limit: 10, Reverse: true}},
		{"both", cluster.FetchRequest{Projection: []string{"body"}, Limit: 1}},
		{"condition sets", cluster.FetchRequest{Projection: []string{"body"}, Conditions: []cluster.ConditionHint{
			{Column: "id", AnyEqual: members("a", "b")},
		}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			encoded := tt.r.Encode()
			got, err := cluster.ParseFetchRequest(encoded)
			require.NoError(t, err)
			assert.Equal(t, tt.r.Projection, got.Request().Projection)
			assert.Equal(t, tt.r.Limit, got.Request().Limit)
			assert.Equal(t, tt.r.Reverse, got.Request().Reverse)
			assert.ElementsMatch(t, tt.r.Conditions, got.Conditions)

			prefix := tt.r
			prefix.Projection, prefix.Limit, prefix.Reverse = nil, 0, false
			assert.True(t, bytes.HasPrefix(encoded, prefix.Encode()), "old peers read the unchanged prefix")
			old, err := cluster.ParseFetchRequest(prefix.Encode())
			require.NoError(t, err)
			assert.Empty(t, old.Projection)
			assert.Zero(t, old.Limit)
		})
	}
}

func TestFetchOptionsWire(t *testing.T) {
	t.Parallel()
	r := cluster.FetchRequest{Signal: signal.Log, Tenant: "t", Start: 1, End: 2, Projection: []string{"body"}, Limit: 3, Reverse: true}
	assert.Equal(t, "02017402040000000104626f64790301", hex.EncodeToString(r.Encode()))
}

func TestMalformedFetchOptions(t *testing.T) {
	t.Parallel()
	base := append(cluster.FetchRequest{}.Encode(), 0, 0)
	for _, tt := range []struct {
		name string
		tail []byte
	}{
		{"count", []byte{0x80}},
		{"oversized count", binary.AppendUvarint(nil, 1<<63)},
		{"column", []byte{1, 2, 'x'}},
		{"limit", []byte{0, 0x80}},
		{"oversized limit", append([]byte{0}, binary.AppendUvarint(nil, ^uint64(0))...)},
		{"direction", []byte{0, 1}},
		{"invalid direction", []byte{0, 1, 2}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := cluster.ParseFetchRequest(append(append([]byte(nil), base...), tt.tail...))
			require.Error(t, err)
		})
	}
}

func TestRemoteFetchOptionsSafety(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name       string
		change     func(*fetch.Request)
		projection []string
		limit      int
	}{
		{"plain", func(*fetch.Request) {}, []string{"body"}, 2},
		{"equality", func(r *fetch.Request) {
			r.Matchers = []fetch.Matcher{{Spec: &fetch.EqualMatcher{Name: "job", Value: "api"}}}
		}, []string{"body"}, 2},
		{"residual matcher", func(r *fetch.Request) {
			r.Matchers = []fetch.Matcher{{Name: []byte("job"), Match: func(signal.Value) bool { return true }}}
		}, []string{"body"}, 0},
		{"condition", func(r *fetch.Request) { r.Conditions = []fetch.Condition{{Column: "id"}} }, nil, 0},
		{"equality hint", func(r *fetch.Request) {
			r.AllConditions = true
			r.Conditions = []fetch.Condition{{Column: "id", Equal: &fetch.EqualMatcher{Name: "id", Value: "x"}}}
		}, nil, 0},
		{"second pass", func(r *fetch.Request) { r.SecondPass = func(*fetch.Batch) bool { return true } }, nil, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got fetch.Request
			srv := httptest.NewServer(cluster.NewReadHandler(func(_ context.Context, r fetch.Request) ([]*fetch.Batch, error) { got = r; return nil, nil }))
			t.Cleanup(srv.Close)
			r := fetch.Request{Projection: []string{"body"}, Limit: 2, Reverse: true}
			tt.change(&r)
			rf := cluster.NewRemoteFetcher(signal.Log, strings.TrimPrefix(srv.URL, "http://"), srv.Client())
			it, err := rf.Fetch(t.Context(), r)
			require.NoError(t, err)
			require.NoError(t, it.Close())
			assert.Equal(t, tt.projection, got.Projection)
			assert.Equal(t, tt.limit, got.Limit)
			assert.Equal(t, tt.limit > 0, got.Reverse)
		})
	}
}

func FuzzFetchOptions(f *testing.F) {
	seeds := []cluster.FetchRequest{
		{Projection: []string{"body"}},
		{Limit: 10, Reverse: true},
		{Projection: []string{"value", "stack_id"}, Conditions: []cluster.ConditionHint{{Column: "id", AnyEqual: members("a", "b")}}},
	}
	for i := range seeds {
		f.Add(seeds[i].Encode())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := cluster.ParseFetchRequest(data)
		if err != nil {
			return
		}
		got, err := cluster.ParseFetchRequest(r.Encode())
		if err != nil || r.Limit != got.Limit || r.Reverse != got.Reverse || !slices.Equal(r.Projection, got.Projection) {
			t.Fatal("fetch options did not round-trip")
		}
	})
}
