package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/profile"
	"github.com/oteldb/storage/tenant"
)

// attributedProfiles builds one cpu profile per value of the profile-level attribute key, each with
// one sample of value 1.
func attributedProfiles(key string, values ...string) profile.Profiles {
	var pd profile.Profiles

	d := &pd.Dictionary
	st := buildProfStack(d, "work")

	rp := pd.AddResource()
	rp.Resource = signal.Resource{Attributes: signal.NewAttributes(
		signal.KeyValue{Key: []byte("service.name"), Value: signal.StringValue([]byte("api"))},
	)}
	sp := rp.AddScope()

	for _, v := range values {
		pr := sp.AddProfile()
		pr.TimeNanos = 1000
		pr.SampleType = profile.ValueType{TypeStrindex: d.InternString([]byte("cpu"))}
		pr.AttributeIndices = []int32{d.AddAttribute(profile.KeyValueAndUnit{
			KeyStrindex: d.InternString([]byte(key)),
			Value:       signal.StringValue([]byte(v)),
		})}

		smp := pr.AddSample()
		smp.StackIndex = st
		smp.Values = []int64{1}
	}

	return pd
}

func TestProfileAttributesMatchAsLabels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	_, err = s.WriteProfiles(ctx, attributedProfiles("profile.kind", "a", "b"))
	require.NoError(t, err)

	got := mustDrain(t, s.ProfileFetcher("default"), fetch.Request{
		Signal: signal.Profile, Start: 0, End: 1 << 60,
		Matchers: []fetch.Matcher{labelMatcher([]byte("profile.kind"), "b")},
	})
	require.Len(t, got, 1)

	v, ok := got[0].Series.Attributes.Get([]byte("profile.kind"))
	require.True(t, ok)
	assert.Equal(t, "b", string(v.Str()))

	series, err := s.ProfileSeries(ctx, "default", nil, 0, 0)
	require.NoError(t, err)
	assert.Len(t, series, 2, "one stream per profile attribute set")
}

// TestProfileAttributesPerProfileHitMaxSeries pins the cardinality contract: profile attributes
// that vary per profile make a stream per profile, and MaxSeries rejects the excess as cardinality.
func TestProfileAttributesPerProfileHitMaxSeries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s, err := InMemory(WithTenancy(tenant.ResolverFunc(func(signal.TenantID) tenant.Policy {
		return tenant.Policy{Limits: tenant.Limits{MaxSeries: 2}}
	})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(ctx) })

	seqs := make([]string, 5)
	for i := range seqs {
		seqs[i] = strconv.Itoa(i)
	}

	acc, err := s.WriteProfiles(ctx, attributedProfiles("profile.seq", seqs...))
	require.NoError(t, err)
	assert.Equal(t, Accepted{Accepted: 2, Rejected: 3, RejectedReason: "max_series"}, acc)
	assert.Equal(t, int64(3), s.AdmissionStats("default").RejectedCardinality)
}
