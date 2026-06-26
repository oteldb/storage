package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/profile"
)

// profileBatch builds a one-service profiles batch: for each (sampleType, value) it adds a profile
// at time ts with one sample over a shared stack ("main" → "work"). Stacks are shared across
// profiles so the symbol store dedups them.
func profileBatch(svc string, ts int64, samples ...sampleSpec) profile.Profiles {
	var pd profile.Profiles
	d := &pd.Dictionary
	st := buildProfStack(d, "work")

	rp := pd.AddResource()
	rp.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte(svc))},
	)}
	sp := rp.AddScope()

	for _, s := range samples {
		pr := sp.AddProfile()
		pr.TimeNanos = ts
		pr.SampleType = profile.ValueType{
			TypeStrindex: d.InternString([]byte(s.typ)),
			UnitStrindex: d.InternString([]byte(s.unit)),
		}
		smp := pr.AddSample()
		smp.StackIndex = st
		smp.Values = []int64{s.value}
	}

	return pd
}

type sampleSpec struct {
	typ, unit string
	value     int64
}

// buildProfStack adds a two-frame stack (main → fn) and returns its index.
func buildProfStack(d *profile.Dictionary, fn string) int32 {
	main := d.AddLocation(profile.Location{Lines: []profile.Line{{
		FunctionIndex: d.AddFunction(profile.Function{NameStrindex: d.InternString([]byte("main"))}),
	}}})
	leaf := d.AddLocation(profile.Location{Lines: []profile.Line{{
		FunctionIndex: d.AddFunction(profile.Function{NameStrindex: d.InternString([]byte(fn))}),
	}}})

	return d.AddStack(leaf, main) // leaf first
}

func profValues(b *fetch.Batch) []int64 {
	col, _ := b.Column(profile.ColValue)

	return col.Int64
}

func TestFacadeWriteAndQueryProfiles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	acc, err := s.WriteProfiles(ctx, profileBatch("api", 1000,
		sampleSpec{"cpu", "nanoseconds", 50},
		sampleSpec{"heap", "bytes", 4096},
	))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 2}, acc)

	all := fetch.Request{
		Signal: signal.Profile, Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{nameMatcherSvc("api")},
	}
	got, err := fetch.Drain(ctx, must(s.ProfileFetcher("default").Fetch(ctx, all)))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.ElementsMatch(t, []int64{50, 4096}, profValues(got[0]))

	// Select only the cpu profile type by its content id — the embedder computes the same hash.
	cpuID := profile.SampleTypeID([]byte("cpu"), []byte("nanoseconds"))
	all.Conditions = []fetch.Condition{{
		Column: profile.ColSampleType,
		Match:  func(v signal.Value) bool { return v.Int() == cpuID },
	}}
	all.AllConditions = true

	got, err = fetch.Drain(ctx, must(s.ProfileFetcher("default").Fetch(ctx, all)))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, []int64{50}, profValues(got[0]))
}

func TestFacadeProfilesCoexistWithOtherSignals(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteProfiles(ctx, profileBatch("api", 1, sampleSpec{"cpu", "nanoseconds", 7}))
	require.NoError(t, err)
	_, err = s.WriteLogs(ctx, logBatch("api", [3]any{100, 9, "log line"}))
	require.NoError(t, err)
	_, err = s.WriteMetrics(ctx, gaugeBatch("api", "http.requests", []int64{100}, []float64{1}))
	require.NoError(t, err)

	profiles, err := fetch.Drain(ctx, must(s.ProfileFetcher("default").Fetch(ctx, fetch.Request{
		Signal: signal.Profile, Start: 0, End: 1 << 60,
	})))
	require.NoError(t, err)
	require.Len(t, profiles, 1)
	assert.Equal(t, []int64{7}, profValues(profiles[0]))

	require.Len(t, mustDrain(t, s.LogFetcher("default"), fetch.Request{Start: 0, End: 1 << 60}), 1)
}

func must(it fetch.Iterator, err error) fetch.Iterator {
	if err != nil {
		panic(err)
	}

	return it
}

func mustDrain(t *testing.T, f fetch.Fetcher, r fetch.Request) []*fetch.Batch {
	t.Helper()
	out, err := fetch.Drain(context.Background(), must(f.Fetch(context.Background(), r)))
	require.NoError(t, err)

	return out
}
