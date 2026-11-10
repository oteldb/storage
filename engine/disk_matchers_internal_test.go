package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Metrics match every source's value for a repeated name, while projections prefer point attributes.
func TestDiskMetricMatchersDuplicateSources(t *testing.T) {
	t.Parallel()
	for _, sources := range [][3]signal.Value{
		{signal.StringValue([]byte("resource")), signal.StringValue([]byte("scope")), signal.StringValue([]byte("point"))},
		{signal.IntValue(1), signal.BoolValue(true), signal.StringValue([]byte("point"))},
	} {
		s := signal.Series{
			Resource:   signal.Resource{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("zone"), Value: sources[0]})},
			Scope:      signal.Scope{Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("zone"), Value: sources[1]})},
			Attributes: signal.NewAttributes(signal.KeyValue{Key: []byte("zone"), Value: sources[2]}),
		}
		for _, disk := range []bool{false, true} {
			refCfg := Config{Backend: backend.Memory(), Prefix: "duplicate-reference"}
			cfg := Config{Backend: backend.Memory(), Prefix: "duplicate-disk", DiskSeries: disk}
			ref, e := New(refCfg), New(cfg)
			for _, target := range []*Engine{ref, e} {
				_, err := target.Append(s, 10, 1)
				require.NoError(t, err)
			}
			for phase := range 3 {
				if phase == 1 {
					require.NoError(t, ref.Flush(t.Context()))
					require.NoError(t, e.Flush(t.Context()))
				}
				if phase == 2 {
					ref, e = New(refCfg), New(Config{Backend: cfg.Backend, Prefix: cfg.Prefix, DiskSeries: true})
					require.NoError(t, ref.LoadPartsReadOnly(t.Context()))
					require.NoError(t, e.LoadPartsReadOnly(t.Context()))
				}
				want, err := ref.LabelValues(t.Context(), fetch.Request{Start: 0, End: 20}, []byte("zone"))
				require.NoError(t, err)
				got, err := e.LabelValues(t.Context(), fetch.Request{Start: 0, End: 20}, []byte("zone"))
				require.NoError(t, err)
				require.Equal(t, want, got, "unfiltered values disk=%v phase=%d", disk, phase)
				for _, value := range sources {
					spec := fetch.EqualMatcher{Name: "zone", Value: string(value.AppendText(nil))}
					r := fetch.Request{Start: 0, End: 20, Matchers: []fetch.Matcher{{Name: []byte("zone"), Match: spec.Predicate(), Spec: &spec}}}
					for _, target := range []*Engine{ref, e} {
						count, err := target.Count(t.Context(), r)
						require.NoError(t, err)
						require.Equal(t, 1, count, "disk=%v phase=%d value=%s", disk, phase, spec.Value)
						series, err := target.Series(t.Context(), r)
						require.NoError(t, err)
						require.Len(t, series, 1)
						require.True(t, s.Equal(series[0]))
						it, err := target.Fetch(t.Context(), r)
						require.NoError(t, err)
						batches, err := fetch.Drain(t.Context(), it)
						require.NoError(t, err)
						require.Len(t, batches, 1)
						require.Equal(t, []int64{10}, batches[0].Timestamps)
						values, err := target.LabelValues(t.Context(), r, []byte("zone"))
						require.NoError(t, err)
						require.Equal(t, []string{"point"}, values)
						groups, err := target.CountBy(t.Context(), r, []byte("zone"))
						require.NoError(t, err)
						require.Equal(t, map[string]int{"point": 1}, groups)
					}
				}
			}
		}
	}
}
