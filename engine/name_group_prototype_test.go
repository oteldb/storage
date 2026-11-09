package engine

import (
	"bytes"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

type prototypeNameGroups struct {
	base       *head
	groups     map[symbols.ID]*head
	unsafeName bool
}

func newPrototypeNameGroups(base *head) *prototypeNameGroups {
	return &prototypeNameGroups{base: base, groups: make(map[symbols.ID]*head)}
}

func (x *prototypeNameGroups) add(id signal.SeriesID, s signal.Series) {
	count := 0
	var name signal.Value
	for _, attrs := range []signal.Attributes{s.Resource.Attributes, s.Scope.Attributes, s.Attributes} {
		for _, attr := range attrs {
			if bytes.Equal(attr.Key, metric.LabelName) {
				count++
				name = attr.Value
			}
		}
	}
	if count != 1 || name.Kind() != signal.KindStr {
		x.unsafeName = true
		return
	}
	nameID, ok := x.base.sym.Lookup(signal.AppendValue(nil, name))
	if !ok {
		panic("name missing from generic symbols")
	}
	group := x.groups[nameID]
	if group == nil {
		group = &head{sym: x.base.sym, series: x.base.series, post: postings.NewDense()}
		x.groups[nameID] = group
	}
	group.indexLabels(id, s)
}

func (x *prototypeNameGroups) sorted() {
	for _, group := range x.groups {
		group.ensureIndexSorted()
	}
}

func (x *prototypeNameGroups) sizeBytes() int64 {
	n := memsize.Map(x.groups)
	for _, group := range x.groups {
		n += memsize.Of[head]() + group.post.SizeBytes() + memsize.Map(group.nonStringLabels)
	}
	return n
}

func (x *prototypeNameGroups) resolve(matchers []fetch.Matcher) []signal.SeriesID {
	if x.unsafeName {
		return x.base.resolve(matchers)
	}
	namePos := -1
	for i := range matchers {
		if bytes.Equal(matchers[i].Name, metric.LabelName) {
			namePos = i
			break
		}
	}
	if namePos < 0 {
		return x.base.resolve(matchers)
	}
	matcher := matchers[namePos]
	if matcher.Match(signal.EmptyValue()) {
		return x.base.resolve(matchers)
	}
	values := matcher.StringValues
	if matcher.Spec != nil {
		values = [][]byte{[]byte(matcher.Spec.Value)}
	}
	if len(values) == 0 {
		return x.base.resolve(matchers)
	}
	residual := make([]fetch.Matcher, 0, len(matchers)-1)
	residual = append(residual, matchers[:namePos]...)
	residual = append(residual, matchers[namePos+1:]...)
	if len(values) == 1 {
		value := signal.StringValue(values[0])
		if !matcher.Match(value) {
			return nil
		}
		var scratch [256]byte
		nameID, ok := x.base.sym.Lookup(signal.AppendValue(scratch[:0], value))
		if !ok {
			return nil
		}
		if group := x.groups[nameID]; group != nil {
			return group.resolve(residual)
		}
		return nil
	}
	var out []signal.SeriesID
	for _, value := range values {
		v := signal.StringValue(value)
		if !matcher.Match(v) {
			continue
		}
		var scratch [256]byte
		nameID, ok := x.base.sym.Lookup(signal.AppendValue(scratch[:0], v))
		if !ok {
			continue
		}
		if group := x.groups[nameID]; group != nil {
			out = append(out, group.resolve(residual)...)
		}
	}
	slices.SortFunc(out, signal.SeriesID.Compare)
	return slices.Compact(out)
}

func prototypeEqual(name, value string) fetch.Matcher {
	spec := &fetch.EqualMatcher{Name: name, Value: value}
	return fetch.Matcher{Name: []byte(name), Match: spec.Predicate(), Spec: spec}
}

func prototypeFixtureIdentity(host, group, cpu, mode int) signal.Series {
	name := "node_cpu_seconds_total"
	if group != 0 {
		name = "node_other_" + strconv.Itoa(group)
	}
	modeName := "idle"
	if mode != 0 {
		modeName = "user"
	}
	id := metric.Identity{
		Series: signal.Series{Resource: signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("node-exporter"))})}, Scope: signal.Scope{Name: []byte("prometheus")}, Attributes: signal.NewAttributes(
			signal.KeyValue{Key: []byte("instance"), Value: signal.StringValue([]byte("host-" + strconv.Itoa(host)))},
			signal.KeyValue{Key: []byte("cpu"), Value: signal.StringValue([]byte(strconv.Itoa(cpu)))},
			signal.KeyValue{Key: []byte("mode"), Value: signal.StringValue([]byte(modeName))},
			signal.KeyValue{Key: []byte("job"), Value: signal.StringValue([]byte("node_exporter"))},
		)}, Name: []byte(name), Unit: []byte("s"), Kind: metric.KindSum, Temporality: metric.TemporalityCumulative, Monotonic: true,
	}
	return id.ToSeries()
}

func newPrototypeQueryFixture(hosts, metricNames, cpus int, withGroups bool) (*head, *prototypeNameGroups) {
	base := newHead()
	groups := newPrototypeNameGroups(base)
	for host := range hosts {
		for name := range metricNames {
			for cpu := range cpus {
				for mode := range 2 {
					s := prototypeFixtureIdentity(host, name, cpu, mode)
					id := s.Hash()
					base.registerIndex(id, s)
					if withGroups {
						groups.add(id, s)
					}
				}
			}
		}
	}
	base.ensureIndexSorted()
	if withGroups {
		groups.sorted()
	}
	return base, groups
}

var prototypeMatchedIDs []signal.SeriesID

func BenchmarkMetricNameScopedQueries(b *testing.B) {
	base, groups := newPrototypeQueryFixture(10000, 4, 8, true)
	name := prototypeEqual("__name__", "node_cpu_seconds_total")
	host := prototypeEqual("instance", "host-4321")
	mode := prototypeEqual("mode", "idle")
	regex := regexp.MustCompile("node_(cpu|other_[12]).*")
	nameRegex := fetch.Matcher{Name: metric.LabelName, Match: func(v signal.Value) bool { return v.Kind() == signal.KindStr && regex.Match(v.Str()) }}
	hinted := nameRegex
	hinted.StringValues = [][]byte{[]byte("node_cpu_seconds_total"), []byte("node_other_1"), []byte("node_other_2")}
	negative := fetch.Matcher{Name: []byte("device"), Match: func(v signal.Value) bool { return string(v.AppendText(nil)) != "loopback" }}
	for _, tc := range []struct {
		name     string
		matchers []fetch.Matcher
	}{
		{"name-only", []fetch.Matcher{name}},
		{"name-host-mode", []fetch.Matcher{name, host, mode}},
		{"host-name-mode", []fetch.Matcher{host, name, mode}},
		{"name-mode", []fetch.Matcher{name, mode}},
		{"multiple-names-host", []fetch.Matcher{hinted, host, mode}},
		{"regex-fallback", []fetch.Matcher{nameRegex, host, mode}},
		{"name-absent-fallback", []fetch.Matcher{host, mode}},
		{"missing-label-negative", []fetch.Matcher{name, host, negative}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			expected := base.resolve(tc.matchers)
			if len(expected) == 0 {
				b.Fatal("fixture unexpectedly matches no series")
			}
			for _, variant := range []string{"generic", "name-scoped"} {
				b.Run(variant, func(b *testing.B) {
					got := groups.resolve(tc.matchers)
					if !slices.Equal(expected, got) {
						b.Fatal("different matching identities")
					}
					b.ReportAllocs()
					if variant == "generic" {
						for b.Loop() {
							prototypeMatchedIDs = base.resolve(tc.matchers)
						}
					} else {
						for b.Loop() {
							prototypeMatchedIDs = groups.resolve(tc.matchers)
						}
					}
					b.ReportMetric(float64(len(expected)), "matched-series")
					b.ReportMetric(float64(base.identityBytes()), "generic-index-B")
					b.ReportMetric(float64(groups.sizeBytes()), "extra-index-B")
				})
			}
		})
	}
}

func BenchmarkMetricNameScopedIndexBuild(b *testing.B) {
	fixture, _ := newPrototypeQueryFixture(1000, 4, 8, false)
	view := fixture.series.View()
	var logicalBytes int64
	var attrs signal.Attributes
	var encoded []byte
	for i := range view.Len() {
		s := view.At(i, attrs)
		attrs = s.Attributes
		encoded = s.AppendHashInput(encoded[:0])
		logicalBytes += int64(len(encoded))
	}
	for _, variant := range []string{"generic", "with-name-scoped"} {
		b.Run(variant, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(logicalBytes)
			var finalBase, finalExtra int64
			for b.Loop() {
				base := newHead()
				groups := newPrototypeNameGroups(base)
				var attrs signal.Attributes
				for i := range view.Len() {
					s := view.At(i, attrs)
					attrs = s.Attributes
					base.registerIndex(view.ID(i), s)
					if variant != "generic" {
						groups.add(view.ID(i), s)
					}
				}
				base.ensureIndexSorted()
				groups.sorted()
				finalBase = base.identityBytes()
				finalExtra = groups.sizeBytes()
			}
			b.ReportMetric(float64(view.Len()), "series/op")
			b.ReportMetric(float64(finalBase), "generic-index-B")
			b.ReportMetric(float64(finalExtra), "extra-index-B")
		})
	}
}

func TestPrototypeNameGroupsMatchGeneric(t *testing.T) {
	t.Parallel()
	base, groups := newPrototypeQueryFixture(12, 4, 2, true)
	tests := []struct {
		name     string
		matchers []fetch.Matcher
	}{
		{"name", []fetch.Matcher{prototypeEqual("__name__", "node_cpu_seconds_total")}},
		{"host", []fetch.Matcher{prototypeEqual("__name__", "node_cpu_seconds_total"), prototypeEqual("instance", "host-7")}},
		{"mode", []fetch.Matcher{prototypeEqual("__name__", "node_cpu_seconds_total"), prototypeEqual("mode", "idle")}},
		{"scope", []fetch.Matcher{prototypeEqual("__name__", "node_cpu_seconds_total"), prototypeEqual(string(labelScopeName), "prometheus")}},
		{"resource", []fetch.Matcher{prototypeEqual("__name__", "node_cpu_seconds_total"), prototypeEqual("service.name", "node-exporter")}},
		{"missing", []fetch.Matcher{prototypeEqual("__name__", "missing")}},
		{"name absent", []fetch.Matcher{prototypeEqual("mode", "idle")}},
		{"negative missing label", []fetch.Matcher{prototypeEqual("__name__", "node_cpu_seconds_total"), {Name: []byte("absent"), Match: func(v signal.Value) bool { return v.Kind() == signal.KindEmpty }}}},
		{"multiple finite names", []fetch.Matcher{{Name: metric.LabelName, StringValues: [][]byte{[]byte("node_cpu_seconds_total"), []byte("node_other_1")}, Match: func(v signal.Value) bool {
			return v.Kind() == signal.KindStr && (bytes.Equal(v.Str(), []byte("node_cpu_seconds_total")) || bytes.Equal(v.Str(), []byte("node_other_1")))
		}}}},
		{"regex fallback", []fetch.Matcher{{Name: metric.LabelName, Match: func(v signal.Value) bool {
			return v.Kind() == signal.KindStr && bytes.HasPrefix(v.Str(), []byte("node_other_"))
		}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := base.resolve(tc.matchers)
			if tc.name != "missing" {
				require.NotEmpty(t, want)
			}
			require.Equal(t, want, groups.resolve(tc.matchers))
		})
	}
}

func TestPrototypeNameGroupsReservedAndTypedFallback(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"point collision", "resource collision", "typed name", "no name"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			base, groups := newPrototypeQueryFixture(2, 2, 1, true)
			s := prototypeFixtureIdentity(99, 0, 0, 0)
			switch kind {
			case "point collision":
				s.Attributes = signal.NewAttributes(append(s.Attributes, signal.KeyValue{Key: metric.LabelName, Value: signal.StringValue([]byte("collision"))})...)
			case "resource collision":
				s.Resource.Attributes = signal.NewAttributes(signal.KeyValue{Key: metric.LabelName, Value: signal.StringValue([]byte("resource-name"))})
			case "typed name":
				s.Attributes = signal.NewAttributes(signal.KeyValue{Key: metric.LabelName, Value: signal.IntValue(5)})
			case "no name":
				s.Attributes = nil
			}
			id := s.Hash()
			base.registerIndex(id, s)
			groups.add(id, s)
			base.ensureIndexSorted()
			groups.sorted()
			require.True(t, groups.unsafeName)
			for _, name := range []string{"node_cpu_seconds_total", "collision", "resource-name", "5", ""} {
				ms := []fetch.Matcher{prototypeEqual("__name__", name)}
				require.Equal(t, base.resolve(ms), groups.resolve(ms))
			}
		})
	}
}

func TestPrototypeNameGroupsTenantLocal(t *testing.T) {
	t.Parallel()
	tenants := map[signal.TenantID]*prototypeNameGroups{}
	for _, tenant := range []signal.TenantID{"tenant-a", "tenant-b"} {
		base, groups := newPrototypeQueryFixture(2, 2, 1, true)
		tenants[tenant] = groups
		require.NotNil(t, base)
	}
	before := tenants["tenant-b"].resolve([]fetch.Matcher{prototypeEqual("__name__", "node_cpu_seconds_total")})
	s := prototypeFixtureIdentity(999, 0, 0, 0)
	id := s.Hash()
	a := tenants["tenant-a"]
	a.base.registerIndex(id, s)
	a.add(id, s)
	a.base.ensureIndexSorted()
	a.sorted()
	require.Equal(t, before, tenants["tenant-b"].resolve([]fetch.Matcher{prototypeEqual("__name__", "node_cpu_seconds_total")}))
	require.NotEqual(t, a.resolve([]fetch.Matcher{prototypeEqual("__name__", "node_cpu_seconds_total")}), before)
}

func FuzzPrototypeNameGroups(f *testing.F) {
	f.Add(uint8(2), uint8(3), uint8(1), true)
	f.Fuzz(func(t *testing.T, host, name, mode uint8, absent bool) {
		base, groups := newPrototypeQueryFixture(8, 4, 2, true)
		ms := []fetch.Matcher{prototypeEqual("__name__", fmt.Sprintf("node_other_%d", name%6)), prototypeEqual("instance", fmt.Sprintf("host-%d", host%12))}
		if mode%2 == 0 {
			ms = append(ms, prototypeEqual("mode", "idle"))
		}
		if absent {
			ms = append(ms, fetch.Matcher{Name: []byte("missing"), Match: func(v signal.Value) bool { return v.Kind() == signal.KindEmpty }})
		}
		if !slices.Equal(base.resolve(ms), groups.resolve(ms)) {
			t.Fatal("name-scoped lookup changed matching series")
		}
	})
}
