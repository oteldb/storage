package seriesindex

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func bruteMetricMatch(s signal.Series, matchers []fetch.Matcher) bool {
	for _, m := range matchers {
		seen, accepted := false, false
		test := func(name []byte, value signal.Value) {
			if bytes.Equal(name, m.Name) {
				seen = true
				if m.Match(value) {
					accepted = true
				}
			}
		}
		for _, attrs := range []signal.Attributes{s.Resource.Attributes, s.Scope.Attributes, s.Attributes} {
			for _, kv := range attrs {
				test(kv.Key, kv.Value)
			}
		}
		if len(s.Scope.Name) > 0 {
			test([]byte(signal.LabelScopeName), signal.StringValue(s.Scope.Name))
		}
		if len(s.Scope.Version) > 0 {
			test([]byte(signal.LabelScopeVersion), signal.StringValue(s.Scope.Version))
		}
		if !seen {
			accepted = m.Match(signal.EmptyValue())
		}
		if !accepted {
			return false
		}
	}
	return true
}

func FuzzIndexRoundTrip(f *testing.F) {
	f.Add([]byte{0, 255, 1, 2, 3}, []byte{'l', 0}, []byte{0, 255}, byte(1))
	f.Add([]byte{}, []byte("label"), []byte("value"), byte(4))
	f.Fuzz(func(t *testing.T, input, name, value []byte, mode byte) {
		if len(input)+len(name)+len(value) > 4096 {
			t.Skip()
		}
		count := min(len(input)+1, 32)
		src := make([]signal.Series, count)
		for i := range src {
			attrs := []signal.KeyValue{{Key: []byte("id"), Value: signal.IntValue(int64(i))}}
			if i < len(input) {
				var v signal.Value
				switch input[i] % 5 {
				case 0:
					v = signal.EmptyValue()
				case 1:
					v = signal.StringValue(value)
				case 2:
					v = signal.BytesValue(value)
				case 3:
					v = signal.IntValue(int64(input[i]))
				default:
					v = signal.BoolValue(input[i]&1 == 0)
				}
				attrs = append(attrs, signal.KeyValue{Key: name, Value: v})
			}
			src[i] = signal.Series{Attributes: signal.NewAttributes(attrs...)}
			if i%3 == 0 {
				src[i].Resource.Attributes = signal.NewAttributes(signal.KeyValue{Key: name, Value: signal.StringValue([]byte("resource"))})
			}
		}
		slices.SortFunc(src, func(a, b signal.Series) int { return a.Hash().Compare(b.Hash()) })
		src = slices.CompactFunc(src, func(a, b signal.Series) bool { return a.Hash() == b.Hash() })
		builder := &Builder{}
		for i := range src {
			s := &src[i]
			if err := builder.Add(s.Hash(), *s, Metadata{Covered: int64(i), HasStats: i%2 == 0, Stats: Stats{Count: 2, Sum: 3, Min: 1, Max: 2}}); err != nil {
				t.Fatal(err)
			}
		}
		var data bytes.Buffer
		if err := builder.Write(&data); err != nil {
			t.Fatal(err)
		}
		if int64(data.Len()) > builder.EncodedBound() {
			t.Fatal("encoded bound exceeded")
		}
		b := backend.Memory()
		if err := b.Write(t.Context(), "index", data.Bytes()); err != nil {
			t.Fatal(err)
		}
		r, err := Open(t.Context(), b, "index")
		if err != nil {
			t.Fatal(err)
		}
		for i := range src {
			s := &src[i]
			got, meta, ok, err := r.Lookup(t.Context(), s.Hash())
			if err != nil || !ok || !s.Equal(got) || meta.Covered != int64(i) || meta.HasStats != (i%2 == 0) {
				t.Fatalf("lookup %d: %v", i, err)
			}
		}
		matchers := []fetch.Matcher{{Name: name, Match: func(v signal.Value) bool { return byte(v.Kind())%5 == mode%5 }}}
		refs, err := r.Resolve(t.Context(), matchers)
		if err != nil {
			t.Fatal(err)
		}
		got, err := postings.ToSlice(refs)
		if err != nil {
			t.Fatal(err)
		}
		var want []signal.SeriesID
		for i := range src {
			s := &src[i]
			if bruteMetricMatch(*s, matchers) {
				want = append(want, signal.SeriesID{Lo: uint64(i)})
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("matching got %v want %v", got, want)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, _, _, err := r.Lookup(ctx, src[0].Hash()); err == nil {
			t.Fatal("canceled lookup succeeded")
		}
	})
}
