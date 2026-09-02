package fetch_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func attrs(kv ...signal.KeyValue) signal.Attributes { return signal.NewAttributes(kv...) }

func kv(k, v string) signal.KeyValue {
	return signal.KeyValue{Key: []byte(k), Value: signal.StringValue([]byte(v))}
}

func equals(name, value string) fetch.Matcher {
	m := fetch.EqualMatcher{Name: name, Value: value}

	return fetch.Matcher{Name: []byte(name), Match: m.Predicate(), Spec: &m}
}

func TestSeriesLabel(t *testing.T) {
	t.Parallel()

	s := signal.Series{
		Attributes: attrs(kv("le", "0.5")),
		Resource:   signal.Resource{Attributes: attrs(kv("service.name", "api"))},
		Scope:      signal.Scope{Name: []byte("otel.sdk"), Version: []byte("1.2"), Attributes: attrs(kv("lib", "x"))},
	}

	for _, tt := range []struct {
		name  string
		label string
		want  string
		ok    bool
	}{
		{"own attribute", "le", "0.5", true},
		{"resource attribute", "service.name", "api", true},
		{"scope attribute", "lib", "x", true},
		{"scope name", signal.LabelScopeName, "otel.sdk", true},
		{"scope version", signal.LabelScopeVersion, "1.2", true},
		{"absent", "nope", "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v, ok := fetch.SeriesLabel(s, []byte(tt.label))
			require.Equal(t, tt.ok, ok)

			if tt.ok {
				assert.Equal(t, tt.want, string(v.AppendText(nil)))
			}
		})
	}
}

func TestMatchesSeries(t *testing.T) {
	t.Parallel()

	s := signal.Series{Resource: signal.Resource{Attributes: attrs(kv("service.name", "api"))}}

	assert.True(t, fetch.MatchesSeries(s, nil))
	assert.True(t, fetch.MatchesSeries(s, []fetch.Matcher{equals("service.name", "api")}))
	assert.False(t, fetch.MatchesSeries(s, []fetch.Matcher{equals("service.name", "web")}))
	// A series that does not carry the label fails, even for a predicate that would accept anything.
	assert.False(t, fetch.MatchesSeries(s, []fetch.Matcher{{
		Name: []byte("absent"), Match: func(signal.Value) bool { return true },
	}}))
}

func TestFilterNarrowsSuperset(t *testing.T) {
	t.Parallel()

	api := signal.Series{Resource: signal.Resource{Attributes: attrs(kv("service.name", "api"))}}
	web := signal.Series{Resource: signal.Resource{Attributes: attrs(kv("service.name", "web"))}}

	inner := fetcherFunc(func(context.Context, fetch.Request) (fetch.Iterator, error) {
		return fetch.NewSliceIterator([]*fetch.Batch{
			{ID: api.Hash(), Series: api},
			{ID: web.Hash(), Series: web},
		}), nil
	})

	f := fetch.Filter(inner)

	batches := drainReq(t, f, fetch.Request{Matchers: []fetch.Matcher{equals("service.name", "api")}})
	require.Len(t, batches, 1)
	assert.Equal(t, api.Hash(), batches[0].ID)

	// No matchers ⇒ nothing to narrow by, the superset is the answer.
	assert.Len(t, drainReq(t, f, fetch.Request{}), 2)
}

func TestEqualitySpecs(t *testing.T) {
	t.Parallel()

	matchers := []fetch.Matcher{
		equals("__name__", "http_requests"),
		{Name: []byte("job"), Match: func(signal.Value) bool { return true }},
	}

	assert.Equal(t, []fetch.EqualMatcher{{Name: "__name__", Value: "http_requests"}}, fetch.EqualitySpecs(matchers))
	assert.Empty(t, fetch.EqualitySpecs(nil))
}

type fetcherFunc func(context.Context, fetch.Request) (fetch.Iterator, error)

func (f fetcherFunc) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	return f(ctx, r)
}

// drainReq is [drain] for a request that carries matchers.
func drainReq(t *testing.T, f fetch.Fetcher, r fetch.Request) []*fetch.Batch {
	t.Helper()

	it, err := f.Fetch(t.Context(), r)
	require.NoError(t, err)

	out, err := fetch.Drain(t.Context(), it)
	require.NoError(t, err)

	return out
}

// traceIDCondition is the shape [Storage.Trace] and the cluster fan-out build: the whole predicate
// lives in a column condition, with no matchers at all.
func traceIDCondition(column, want string) fetch.Condition {
	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return string(v.Str()) == want },
		Equal:  &fetch.EqualMatcher{Name: column, Value: want},
	}
}

func TestFilterAppliesConditions(t *testing.T) {
	t.Parallel()

	series := signal.Series{Resource: signal.Resource{Attributes: attrs(kv("service.name", "api"))}}

	// A producer that ignored the conditions: three rows of two different traces.
	superset := func() []*fetch.Batch {
		return []*fetch.Batch{{
			ID:         series.Hash(),
			Series:     series,
			Timestamps: []int64{1, 2, 3},
			Columns: []fetch.NamedColumn{
				fetch.BytesColumn("trace_id", [][]byte{[]byte("aaa"), []byte("bbb"), []byte("aaa")}),
				fetch.BytesColumn("name", [][]byte{[]byte("root"), []byte("other"), []byte("child")}),
			},
		}}
	}

	f := fetch.Filter(fetcherFunc(func(context.Context, fetch.Request) (fetch.Iterator, error) {
		return fetch.NewSliceIterator(superset()), nil
	}))

	t.Run("keeps only matching rows", func(t *testing.T) {
		t.Parallel()

		batches := drainReq(t, f, fetch.Request{
			Conditions:    []fetch.Condition{traceIDCondition("trace_id", "aaa")},
			AllConditions: true,
		})
		require.Len(t, batches, 1)

		b := batches[0]
		assert.Equal(t, []int64{1, 3}, b.Timestamps)

		names, ok := b.Column("name")
		require.True(t, ok)
		assert.Equal(t, [][]byte{[]byte("root"), []byte("child")}, names.Bytes)
	})

	t.Run("drops a batch no row survives", func(t *testing.T) {
		t.Parallel()

		assert.Empty(t, drainReq(t, f, fetch.Request{
			Conditions:    []fetch.Condition{traceIDCondition("trace_id", "zzz")},
			AllConditions: true,
		}))
	})
}

func TestFilterConditionOnAttribute(t *testing.T) {
	t.Parallel()

	blob := func(k, v string) []byte {
		return signal.NewAttributes(kv(k, v)).AppendHashInput(nil)
	}

	series := signal.Series{Resource: signal.Resource{Attributes: attrs(kv("service.name", "api"))}}
	f := fetch.Filter(fetcherFunc(func(context.Context, fetch.Request) (fetch.Iterator, error) {
		return fetch.NewSliceIterator([]*fetch.Batch{{
			ID:         series.Hash(),
			Series:     series,
			Timestamps: []int64{1, 2},
			Columns: []fetch.NamedColumn{
				fetch.BytesColumn(fetch.AttrsColumn, [][]byte{blob("http.method", "GET"), blob("http.method", "POST")}),
			},
		}}), nil
	}))

	batches := drainReq(t, f, fetch.Request{
		Conditions:    []fetch.Condition{traceIDCondition("http.method", "GET")},
		AllConditions: true,
	})
	require.Len(t, batches, 1)
	assert.Equal(t, []int64{1}, batches[0].Timestamps)
}

func TestFilterSecondPass(t *testing.T) {
	t.Parallel()

	series := signal.Series{Resource: signal.Resource{Attributes: attrs(kv("service.name", "api"))}}
	f := fetch.Filter(fetcherFunc(func(context.Context, fetch.Request) (fetch.Iterator, error) {
		return fetch.NewSliceIterator([]*fetch.Batch{
			{ID: signal.SeriesID{Lo: 1}, Series: series, Timestamps: []int64{1}},
			{ID: signal.SeriesID{Lo: 2}, Series: series, Timestamps: []int64{2}},
		}), nil
	}))

	batches := drainReq(t, f, fetch.Request{
		SecondPass: func(b *fetch.Batch) bool { return b.ID == signal.SeriesID{Lo: 2} },
	})
	require.Len(t, batches, 1)
	assert.Equal(t, signal.SeriesID{Lo: 2}, batches[0].ID)
}
