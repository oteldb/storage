package promql_test

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	storagepromql "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
)

type countingEngine struct {
	*engine.Engine

	fetches     int
	counts      int
	groups      int
	labelValues int
}

func (e *countingEngine) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	e.fetches++
	return e.Engine.Fetch(ctx, r)
}
func (e *countingEngine) Count(ctx context.Context, r fetch.Request) (int, error) {
	e.counts++
	return e.Engine.Count(ctx, r)
}
func (e *countingEngine) CountBy(ctx context.Context, r fetch.Request, label []byte) (map[string]int, error) {
	e.groups++
	return e.Engine.CountBy(ctx, r, label)
}

func (e *countingEngine) LabelValues(ctx context.Context, r fetch.Request, name []byte) ([]string, error) {
	e.labelValues++
	return e.Engine.LabelValues(ctx, r, name)
}

type seriesCounter interface {
	CountSeries(ctx context.Context, startMs, endMs int64, matchers ...*labels.Matcher) (uint64, error)
	CountSeriesBy(ctx context.Context, startMs, endMs int64, label string, matchers ...*labels.Matcher) (map[string]uint64, error)
}

func TestAbsentMatchersUseNativeCapabilities(t *testing.T) {
	t.Parallel()
	for _, flushed := range []bool{false, true} {
		name := "head"
		if flushed {
			name = "part"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := &countingEngine{Engine: engine.New(engine.Config{Backend: backend.Memory(), Prefix: "matchers"})}
			t.Cleanup(func() { require.NoError(t, e.Close(context.Background())) })
			identities := make([]signal.Series, 0, 5)
			for _, value := range []signal.Value{signal.EmptyValue(), signal.StringValue(nil), signal.StringValue([]byte("dev")), signal.StringValue([]byte("prod")), signal.IntValue(7)} {
				kvs := []signal.KeyValue{{Key: []byte("__name__"), Value: signal.StringValue([]byte("m"))}, {Key: []byte("__unit__"), Value: signal.StringValue([]byte("By"))}}
				if value.Kind() != signal.KindEmpty {
					kvs = append(kvs, signal.KeyValue{Key: []byte("env"), Value: value})
				}
				s := signal.Series{Attributes: signal.NewAttributes(kvs...)}
				identities = append(identities, s)
				_, err := e.Append(s, 1000000, 1)
				require.NoError(t, err)
			}
			outside := signal.Series{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("__name__"), Value: signal.StringValue([]byte("m"))}, signal.KeyValue{Key: []byte("env"), Value: signal.StringValue([]byte("outside"))})}
			_, err := e.Append(outside, 100000000, 1)
			require.NoError(t, err)
			if flushed {
				require.NoError(t, e.Flush(t.Context()))
			}
			for _, tt := range []struct {
				name  string
				typ   labels.MatchType
				value string
				label string
			}{
				{"empty equality", labels.MatchEqual, "", "env"},
				{"negative equality", labels.MatchNotEqual, "dev", "env"},
				{"empty regexp", labels.MatchRegexp, "^$", "env"},
				{"negative regexp", labels.MatchNotRegexp, "dev", "env"},
				{"present", labels.MatchNotEqual, "", "env"},
				{"typed text", labels.MatchEqual, "7", "env"},
				{"hidden absent", labels.MatchEqual, "", "__unit__"},
				{"hidden present", labels.MatchEqual, "By", "__unit__"},
			} {
				t.Run(tt.name, func(t *testing.T) {
					m := labels.MustNewMatcher(tt.typ, tt.label, tt.value)
					ms := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "m"), m}
					want := map[string]uint64{}
					var total uint64
					for _, s := range identities {
						lset := storagepromql.PromLabels(s)
						if storagepromql.MatchesAll(lset, ms) {
							want[lset.Get("env")]++
							total++
						}
					}
					e.fetches, e.counts, e.groups, e.labelValues = 0, 0, 0, 0
					q, err := storagepromql.NewQueryable(e, "default").Querier(0, 10)
					require.NoError(t, err)
					defer func() { require.NoError(t, q.Close()) }()
					c := q.(seriesCounter)
					got, err := c.CountSeries(t.Context(), 0, 10, ms...)
					require.NoError(t, err)
					assert.Equal(t, total, got)
					grouped, err := c.CountSeriesBy(t.Context(), 0, 10, "env", ms...)
					require.NoError(t, err)
					assert.Equal(t, want, grouped)
					if tt.label == "env" {
						values, _, err := q.LabelValues(t.Context(), "env", nil, ms...)
						require.NoError(t, err)
						metadata := append([]signal.Series(nil), identities...)
						if flushed {
							metadata = append(metadata, outside)
						}
						wantValues := make(map[string]struct{})
						for _, s := range metadata {
							ls := storagepromql.PromLabels(s)
							if value := ls.Get("env"); value != "" && storagepromql.MatchesAll(ls, ms) {
								wantValues[value] = struct{}{}
							}
						}
						gotValues := make(map[string]struct{}, len(values))
						for _, value := range values {
							gotValues[value] = struct{}{}
						}
						assert.Equal(t, wantValues, gotValues)
						assert.Equal(t, 1, e.labelValues)
						assert.Zero(t, e.fetches, "sample materialization is unnecessary")
						assert.Equal(t, 1, e.counts)
						assert.Equal(t, 1, e.groups)
					}
				})
			}
		})
	}
}

func TestPushableMatchersEqualityHints(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		typ   labels.MatchType
		value string
		hint  bool
	}{
		{"nonempty equality", labels.MatchEqual, "dev", true},
		{"empty equality", labels.MatchEqual, "", false},
		{"negation", labels.MatchNotEqual, "dev", false},
		{"regexp", labels.MatchRegexp, ".*", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := labels.MustNewMatcher(tt.typ, "env", tt.value)
			ms := storagepromql.PushableMatchers([]*labels.Matcher{m})
			require.Len(t, ms, 1)
			assert.Equal(t, tt.hint, ms[0].Spec != nil)
			for _, v := range []signal.Value{signal.EmptyValue(), signal.StringValue(nil), signal.StringValue([]byte("dev")), signal.IntValue(7)} {
				assert.Equal(t, m.Matches(string(v.AppendText(nil))), ms[0].Match(v))
			}
		})
	}
}

func FuzzMatcherProjection(f *testing.F) {
	for _, value := range []string{"", "dev", "7"} {
		for typ := range uint8(4) {
			f.Add(typ, value, true)
			f.Add(typ, value, false)
		}
	}
	f.Fuzz(func(t *testing.T, typ uint8, value string, present bool) {
		value = strings.ToValidUTF8(value, "?")
		m, err := labels.NewMatcher(labels.MatchType(typ%4), "env", regexp.QuoteMeta(value))
		if err != nil {
			t.Fatal(err)
		}
		s := signal.Series{}
		if present {
			s.Attributes = signal.NewAttributes(signal.KeyValue{Key: []byte("env"), Value: signal.StringValue([]byte(value))})
		}
		ms := []*labels.Matcher{m}
		got := fetch.MatchesSeries(s, storagepromql.PushableMatchers(ms))
		if want := storagepromql.MatchesAll(storagepromql.PromLabels(s), ms); got != want {
			t.Fatalf("predicate=%v projection=%v", got, want)
		}
	})
}
