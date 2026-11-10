package seriesindex

import (
	"bytes"
	"slices"
	"testing"

	"github.com/go-faster/sdk/gold"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func indexSeries() []signal.Series {
	out := []signal.Series{
		{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte("a"))}, signal.KeyValue{Key: []byte("value"), Value: signal.StringValue([]byte("1"))})},
		{Resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))})},
			Scope:      signal.Scope{Name: []byte("scope"), Version: []byte("v1")},
			Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("host"), Value: signal.StringValue([]byte("b"))}, signal.KeyValue{Key: []byte("value"), Value: signal.IntValue(1)})},
		{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("value"), Value: signal.EmptyValue()}, signal.KeyValue{Key: []byte{'a', 0}, Value: signal.BytesValue([]byte{0, 255})})},
		{},
	}
	slices.SortFunc(out, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
	return out
}

func buildIndex(tb testing.TB, src []signal.Series) ([]byte, *Builder) {
	tb.Helper()
	b := &Builder{}
	for i := range src {
		s := &src[i]
		require.NoError(tb, b.Add(s.Hash(), *s, Metadata{Covered: int64(i + 10), HasStats: true,
			Stats: Stats{Count: 3, Sum: 6, Min: 1, Max: 3}}))
	}
	var data bytes.Buffer
	require.NoError(tb, b.Write(&data))
	return data.Bytes(), b
}

func TestIdentityIndexRoundTrip(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	data, builder := buildIndex(t, src)
	b := backend.Memory()
	require.NoError(t, b.Write(t.Context(), "index", data))
	r, err := Open(t.Context(), b, "index")
	require.NoError(t, err)
	require.EqualValues(t, len(src), r.Count())
	require.Greater(t, builder.Bytes(), int64(len(data)))
	for i, s := range src {
		got, meta, ok, err := r.Lookup(t.Context(), s.Hash())
		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, got.Equal(s))
		require.EqualValues(t, i+10, meta.Covered)
		require.Equal(t, Stats{Count: 3, Sum: 6, Min: 1, Max: 3}, meta.Stats)
		require.True(t, meta.HasStats)
	}
	ids := r.IDs(t.Context())
	for _, s := range src {
		require.True(t, ids.Next())
		require.Equal(t, s.Hash(), ids.At())
	}
	require.False(t, ids.Next())
	require.False(t, ids.Seek(src[0].Hash()))
	require.NoError(t, ids.Err())
	_, _, ok, err := r.Lookup(t.Context(), signal.SeriesID{Hi: ^uint64(0), Lo: ^uint64(0)})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestIdentityIndexPostings(t *testing.T) {
	t.Parallel()
	src := indexSeries()
	data, _ := buildIndex(t, src)
	b := backend.Memory()
	require.NoError(t, b.Write(t.Context(), "index", data))
	r, err := Open(t.Context(), b, "index")
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		matchers []fetch.Matcher
	}{
		{"all", nil},
		{"string hint", []fetch.Matcher{{Name: []byte("host"), Spec: &fetch.EqualMatcher{Value: "a"}, Match: func(v signal.Value) bool { return v.Kind() == signal.KindStr && string(v.AppendText(nil)) == "a" }}}},
		{"mixed text hint", []fetch.Matcher{{Name: []byte("value"), Spec: &fetch.EqualMatcher{Value: "1"}, Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == "1" }}}},
		{"missing", []fetch.Matcher{{Name: []byte("host"), Match: func(v signal.Value) bool { return v.Kind() == signal.KindEmpty }}}},
		{"unknown missing", []fetch.Matcher{{Name: []byte("unknown"), Match: func(v signal.Value) bool { return v.Kind() == signal.KindEmpty }}}},
		{"unknown present", []fetch.Matcher{{Name: []byte("unknown"), Match: func(v signal.Value) bool { return v.Kind() != signal.KindEmpty }}}},
		{"scope", []fetch.Matcher{{Name: []byte(signal.LabelScopeName), Match: func(v signal.Value) bool { return string(v.AppendText(nil)) == "scope" }}}},
		{"embedded zero", []fetch.Matcher{{Name: []byte{'a', 0}, Match: func(v signal.Value) bool { return v.Kind() == signal.KindBytes }}}},
		{"intersection", []fetch.Matcher{{Name: []byte("host"), Match: func(v signal.Value) bool { return v.Kind() != signal.KindEmpty }}, {Name: []byte("value"), Match: func(v signal.Value) bool { return v.Kind() == signal.KindInt }}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var want []signal.SeriesID
			for i, s := range src {
				matches := true
				for _, m := range tc.matchers {
					seen, accepted := false, false
					visitLabels(s, func(name []byte, v signal.Value) {
						if bytes.Equal(name, m.Name) {
							seen = true
							accepted = accepted || m.Match(v)
						}
					})
					if !seen {
						accepted = m.Match(signal.EmptyValue())
					}
					matches = matches && accepted
				}
				if matches {
					want = append(want, signal.SeriesID{Lo: uint64(i)})
				}
			}
			it, err := r.Resolve(t.Context(), tc.matchers)
			require.NoError(t, err)
			got, err := postings.ToSlice(it)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestIdentityIndexGolden(t *testing.T) {
	t.Parallel()
	data, _ := buildIndex(t, indexSeries())
	gold.Bytes(t, data, "index")
}

func FuzzIdentityIndex(f *testing.F) {
	data, _ := buildIndex(f, indexSeries())
	f.Add(data)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		b := backend.Memory()
		if err := b.Write(t.Context(), "index", data); err != nil {
			t.Fatal(err)
		}
		r, err := Open(t.Context(), b, "index")
		if err != nil || r.MaxPageBytes() > 4<<20 || r.Count() > 10000 {
			return
		}
		it := r.IDs(t.Context())
		for i := 0; i < 1000 && it.Next(); i++ {
			_, _, _, _ = r.Lookup(t.Context(), it.At())
		}
		_, _ = r.Resolve(t.Context(), []fetch.Matcher{{Name: []byte("host"), Match: func(signal.Value) bool { return true }}})
	})
}
