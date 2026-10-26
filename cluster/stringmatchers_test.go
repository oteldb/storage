package cluster_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	storagepromql "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
)

func stringHint() fetch.StringSetMatcher {
	return fetch.StringSetMatcher{Name: "service.name", Values: fetch.AnyEqualStrings([]string{"api", "web"})}
}

func stringHintSeries() []signal.Series {
	out := make([]signal.Series, 0, 5)
	for _, v := range []signal.Value{
		signal.StringValue([]byte("api")), signal.StringValue([]byte("web")),
		signal.StringValue([]byte("other")), signal.IntValue(7), signal.EmptyValue(),
	} {
		s := signal.Series{}
		if v.Kind() != signal.KindEmpty {
			s.Resource.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("service.name"), Value: v})
		}
		out = append(out, s)
	}
	return out
}

func TestStringMatchersCodec(t *testing.T) {
	t.Parallel()
	for _, options := range []bool{false, true} {
		r := cluster.FetchRequest{Signal: signal.Log, Tenant: "t", Start: 1, End: 10, StringMatchers: []fetch.StringSetMatcher{stringHint()}}
		if options {
			r.Projection = []string{"body"}
			r.Conditions = []cluster.ConditionHint{{Column: "body", AnyEqual: fetch.AnyEqualStrings([]string{"hello"})}}
		}
		encoded := r.Encode()
		decoded, err := cluster.ParseFetchRequest(encoded)
		require.NoError(t, err)
		assert.Equal(t, r.StringMatchers, decoded.StringMatchers)
		assert.Equal(t, r.Projection, decoded.Projection)
		assert.ElementsMatch(t, r.Conditions, decoded.Conditions)
		assert.Len(t, decoded.Request().Matchers, 1)
		r.StringMatchers = nil
		assert.True(t, bytes.HasPrefix(encoded, r.Encode()), "legacy wire prefix is unchanged")
		legacy, err := cluster.ParseFetchRequest(r.Encode())
		require.NoError(t, err)
		assert.Empty(t, legacy.StringMatchers)
	}
}

func TestStringMatchersMalformed(t *testing.T) {
	t.Parallel()
	base := (cluster.FetchRequest{Projection: []string{"body"}}).Encode()
	for _, tail := range [][]byte{
		{0x80}, binary.AppendUvarint(nil, ^uint64(0)),
		{1, 3, 'x'}, {1, 0, 0x80}, {1, 0, 2, 0}, {1, 0, 1, 2, 'x'},
	} {
		_, err := cluster.ParseFetchRequest(append(bytes.Clone(base), tail...))
		require.Error(t, err)
	}
}

func TestStringMatchersSeriesRPC(t *testing.T) {
	t.Parallel()
	series := stringHintSeries()
	var ownerRows int
	srv := httptest.NewServer(cluster.SeriesHandler(func(_ context.Context, _ signal.Signal, _ string, _, _ int64, matchers []fetch.Matcher) ([]signal.Series, error) {
		got := fetch.FilterSeries(slices.Clone(series), matchers)
		ownerRows = len(got)
		return got, nil
	}))
	t.Cleanup(srv.Close)
	got, err := cluster.FetchSeries(t.Context(), srv.Client(), strings.TrimPrefix(srv.URL, "http://"), signal.Log, "t", 0, 10, nil,
		cluster.WithStringMatchers([]fetch.StringSetMatcher{stringHint()}))
	require.NoError(t, err)
	assert.Equal(t, 4, ownerRows, "owner removes unrelated strings, retaining typed/missing values")
	assert.Len(t, got, 4)
	callback := fetch.Matcher{Name: []byte("service.name"), Match: func(v signal.Value) bool {
		return v.Kind() == signal.KindStr && (string(v.Str()) == "api" || string(v.Str()) == "web")
	}}
	assert.Len(t, fetch.FilterSeries(got, []fetch.Matcher{callback}), 2)
}

func TestStringMatchersFetchSafety(t *testing.T) {
	t.Parallel()
	var owner cluster.FetchRequest
	srv := httptest.NewServer(cluster.NewReadHandler(func(_ context.Context, r fetch.Request) ([]*fetch.Batch, error) {
		owner.Limit, owner.Reverse = r.Limit, r.Reverse
		owner.StringMatchers = fetch.StringSpecs(r.Matchers)
		out := make([]*fetch.Batch, 0)
		for _, s := range stringHintSeries() {
			if fetch.MatchesSeries(s, r.Matchers) {
				out = append(out, &fetch.Batch{Series: s, ID: s.Hash(), Timestamps: []int64{1}})
			}
		}
		return out, nil
	}))
	t.Cleanup(srv.Close)
	m := stringHint().Matcher()
	it, err := cluster.NewRemoteFetcher(signal.Log, strings.TrimPrefix(srv.URL, "http://"), srv.Client()).Fetch(t.Context(), fetch.Request{
		Signal: signal.Log, Matchers: []fetch.Matcher{m}, Limit: 1, Reverse: true,
	})
	require.NoError(t, err)
	_, err = fetch.Drain(t.Context(), it)
	require.NoError(t, err)
	assert.Zero(t, owner.Limit, "necessary string hints do not make an ordered limit exact")
	assert.False(t, owner.Reverse)
	assert.Equal(t, []fetch.StringSetMatcher{stringHint()}, owner.StringMatchers)
}

func TestStringMatchersAggregateRPC(t *testing.T) {
	t.Parallel()
	series := stringHintSeries()
	makeAggs := func(ms []fetch.Matcher) []engine.NamedAgg {
		out := make([]engine.NamedAgg, 0, len(series))
		for _, s := range fetch.FilterSeries(slices.Clone(series), ms) {
			out = append(out, engine.NamedAgg{Series: s})
		}
		return out
	}
	mux := http.NewServeMux()
	mux.Handle(cluster.AggregatePath, cluster.AggregateHandler(func(_ context.Context, _ string, _, _, _ int64, ms []fetch.Matcher) ([]engine.NamedAgg, error) {
		return makeAggs(ms), nil
	}))
	mux.Handle(cluster.AggregateWindowPath, cluster.AggregateWindowHandler(func(_ context.Context, _ string, _, _ int64, _ engine.WindowSpec, ms []fetch.Matcher) ([]engine.NamedWindowAgg, error) {
		out := make([]engine.NamedWindowAgg, 0, len(series))
		for _, a := range makeAggs(ms) {
			out = append(out, engine.NamedWindowAgg{Series: a.Series})
		}
		return out, nil
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	aggregator := cluster.NewRemoteAggregator(strings.TrimPrefix(srv.URL, "http://"), srv.Client(), cluster.WithStringMatchers([]fetch.StringSetMatcher{stringHint()}))
	got, err := aggregator.Aggregate(t.Context(), "t", 0, 10, 1, nil)
	require.NoError(t, err)
	assert.Len(t, got, 4)
	windows, err := aggregator.AggregateWindow(t.Context(), "t", 0, 10, engine.WindowSpec{Step: 1, Window: 2}, nil)
	require.NoError(t, err)
	assert.Len(t, windows, 4)
}

func FuzzStringMatchers(f *testing.F) {
	seeds := []cluster.FetchRequest{
		{}, {StringMatchers: []fetch.StringSetMatcher{stringHint()}},
		{Projection: []string{"body"}, StringMatchers: []fetch.StringSetMatcher{{Name: "k", Values: [][]byte{[]byte(""), []byte("foo")}}}},
	}
	for i := range seeds {
		f.Add(seeds[i].Encode())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := cluster.ParseFetchRequest(data)
		if err != nil {
			return
		}
		decoded, err := cluster.ParseFetchRequest(r.Encode())
		if err != nil || len(r.StringMatchers) != len(decoded.StringMatchers) {
			t.Fatalf("string matcher roundtrip: %v", err)
		}
		for i, m := range r.StringMatchers {
			got := decoded.StringMatchers[i]
			if m.Name != got.Name || len(m.Values) != len(got.Values) {
				t.Fatal("string matcher metadata changed")
			}
			for j := range m.Values {
				if !bytes.Equal(m.Values[j], got.Values[j]) {
					t.Fatal("string matcher member changed")
				}
			}
		}
	})
}

func BenchmarkSeriesStringHints(b *testing.B) {
	series := make([]signal.Series, 1000)
	for i := range series {
		series[i].Resource.Attributes = signal.NewAttributes(signal.KeyValue{
			Key: []byte("service.name"), Value: signal.StringValue([]byte(fmt.Sprintf("svc/%04d", i))),
		})
	}
	matcher := storagepromql.PushableMatchers([]*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, "service.name", `svc/000[12]`),
	})[0]
	for _, hints := range []bool{false, true} {
		b.Run(fmt.Sprintf("hints=%t", hints), func(b *testing.B) {
			srv := httptest.NewServer(cluster.SeriesHandler(func(_ context.Context, _ signal.Signal, _ string, _, _ int64, ms []fetch.Matcher) ([]signal.Series, error) {
				return fetch.FilterSeries(slices.Clone(series), ms), nil
			}))
			b.Cleanup(srv.Close)
			var opts []cluster.Option
			if hints {
				opts = append(opts, cluster.WithStringMatchers(fetch.StringSpecs([]fetch.Matcher{matcher})))
			}
			addr := strings.TrimPrefix(srv.URL, "http://")
			b.ReportAllocs()
			for b.Loop() {
				got, err := cluster.FetchSeries(b.Context(), srv.Client(), addr, signal.Log, "t", 0, 10, nil, opts...)
				if err != nil {
					b.Fatal(err)
				}
				if len(fetch.FilterSeries(got, []fetch.Matcher{matcher})) != 2 {
					b.Fatal("unexpected selected series")
				}
			}
		})
	}
}
