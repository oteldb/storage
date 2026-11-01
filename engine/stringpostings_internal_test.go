package engine

import (
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

func addStringPostingSeries(h *head, index int, value *signal.Value) signal.SeriesID {
	attrs := signal.Attributes{{Key: []byte("id"), Value: signal.StringValue([]byte(strconv.Itoa(index)))}}
	if value != nil {
		attrs = append(attrs, signal.KeyValue{Key: []byte("label"), Value: *value})
	}
	s := signal.Series{Attributes: signal.NewAttributes(attrs...)}
	id := s.Hash()
	h.register(id, s)
	return id
}

func TestStringPostingsMatchesScan(t *testing.T) {
	t.Parallel()
	for _, mixed := range []bool{false, true} {
		h := newHead()
		values := []signal.Value{signal.StringValue(nil), signal.StringValue([]byte("42")),
			signal.StringValue([]byte("true")), signal.StringValue([]byte("other"))}
		if mixed {
			values = append(values, signal.IntValue(42), signal.DoubleValue(42), signal.BoolValue(true), signal.EmptyValue())
		}
		addStringPostingSeries(h, 0, nil)
		for i, value := range values {
			addStringPostingSeries(h, i+1, &value)
		}
		h.ensureIndexSorted()
		for _, text := range []string{"42", "", "true", "missing"} {
			spec := &fetch.EqualMatcher{Name: "label", Value: text}
			matcher := fetch.Matcher{Name: []byte("label"), Match: spec.Predicate(), Spec: spec}
			want := matcher
			want.Spec = nil
			require.Equal(t, h.resolve([]fetch.Matcher{want}), h.resolve([]fetch.Matcher{matcher}), "mixed=%v text=%q", mixed, text)
			matcher.Spec = nil
			matcher.StringValues = fetch.AnyEqualStrings([]string{"", "42", "true", "other", "missing"})
			require.Equal(t, h.resolve([]fetch.Matcher{want}), h.resolve([]fetch.Matcher{matcher}), "necessary hints are rechecked")
		}
	}
}

func TestStringPostingsSkipsValueScan(t *testing.T) {
	t.Parallel()
	h := newHead()
	for i := range 10000 {
		value := signal.StringValue([]byte("host-" + strconv.Itoa(i)))
		addStringPostingSeries(h, i, &value)
	}
	h.ensureIndexSorted()
	calls := 0
	spec := &fetch.EqualMatcher{Name: "label", Value: "host-9999"}
	matcher := fetch.Matcher{Name: []byte("label"), Spec: spec, Match: func(v signal.Value) bool {
		calls++
		return v.Kind() == signal.KindStr && string(v.Str()) == spec.Value
	}}
	require.Len(t, h.resolve([]fetch.Matcher{matcher}), 1)
	require.Equal(t, 2, calls, "one absence check and one candidate, independent of label cardinality")
	matcher.Spec = nil
	calls = 0
	require.Len(t, h.resolve([]fetch.Matcher{matcher}), 1)
	require.Equal(t, 10001, calls)
}

func TestStringPostingsAfterIdentityRebuild(t *testing.T) {
	t.Parallel()
	h := newHead()
	str, integer := signal.StringValue([]byte("42")), signal.IntValue(42)
	keep := addStringPostingSeries(h, 0, &str)
	addStringPostingSeries(h, 1, &integer)
	snap := h.series.View()
	rebuilt := rebuildIdentity(snap, map[signal.SeriesID]struct{}{keep: {}})
	require.Empty(t, rebuilt.nonStringLabels)
	h.swapIdentity(rebuilt, snap, snap, nil)
	require.Empty(t, h.nonStringLabels)
	addStringPostingSeries(h, 2, &integer)
	h.ensureIndexSorted()
	spec := &fetch.EqualMatcher{Name: "label", Value: "42"}
	require.Len(t, h.resolve([]fetch.Matcher{{Name: []byte("label"), Match: spec.Predicate(), Spec: spec}}), 2)
}

func TestStringPostingsRebuildTail(t *testing.T) {
	t.Parallel()
	h := newHead()
	str, integer := signal.StringValue([]byte("42")), signal.IntValue(42)
	keep := addStringPostingSeries(h, 0, &str)
	snap := h.series.View()
	rebuilt := rebuildIdentity(snap, map[signal.SeriesID]struct{}{keep: {}})
	addStringPostingSeries(h, 1, &integer)
	h.swapIdentity(rebuilt, snap, h.series.View(), nil)
	h.ensureIndexSorted()
	spec := &fetch.EqualMatcher{Name: "label", Value: "42"}
	require.Len(t, h.resolve([]fetch.Matcher{{Name: []byte("label"), Match: spec.Predicate(), Spec: spec}}), 2)
}

func FuzzStringPostings(f *testing.F) {
	for _, seed := range []string{"", "42", "host-0", "\xff\x00"} {
		f.Add(seed, int64(42))
	}
	f.Fuzz(func(t *testing.T, text string, number int64) {
		h := newHead()
		values := []signal.Value{signal.StringValue([]byte(text)), signal.StringValue(nil), signal.StringValue([]byte("42"))}
		for phase := range 2 {
			if phase == 1 {
				values = []signal.Value{signal.IntValue(number), signal.BoolValue(true), signal.EmptyValue()}
			}
			for i, value := range values {
				addStringPostingSeries(h, phase*len(values)+i, &value)
			}
			addStringPostingSeries(h, 100, nil)
			h.ensureIndexSorted()
			spec := &fetch.EqualMatcher{Name: "label", Value: text}
			plain := fetch.Matcher{Name: []byte("label"), Match: spec.Predicate()}
			want := h.resolve([]fetch.Matcher{plain})
			plain.Spec = spec
			require.Equal(t, want, h.resolve([]fetch.Matcher{plain}))
			plain.Spec = nil
			plain.StringValues = fetch.AnyEqualStrings([]string{text, "42"})
			require.Equal(t, want, h.resolve([]fetch.Matcher{plain}))
		}
	})
}

func BenchmarkStringPostings(b *testing.B) {
	for _, cardinality := range []int{1000, 10000, 100000} {
		h := newHead()
		for i := range cardinality {
			value := signal.StringValue([]byte("host-" + strconv.Itoa(i)))
			addStringPostingSeries(h, i, &value)
		}
		h.ensureIndexSorted()
		spec := &fetch.EqualMatcher{Name: "label", Value: "host-" + strconv.Itoa(cardinality-1)}
		for _, hinted := range []bool{false, true} {
			b.Run(strconv.Itoa(cardinality)+"/hinted="+strconv.FormatBool(hinted), func(b *testing.B) {
				matcher := fetch.Matcher{Name: []byte("label"), Match: spec.Predicate()}
				if hinted {
					matcher.Spec = spec
				}
				b.ReportAllocs()
				for b.Loop() {
					ids := h.resolve([]fetch.Matcher{matcher})
					if len(ids) != 1 || !slices.IsSortedFunc(ids, signal.SeriesID.Compare) {
						b.Fatal("unexpected matched identities")
					}
				}
			})
		}
	}
}
