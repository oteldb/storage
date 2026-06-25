package signal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleSeries() Series {
	return Series{
		Resource: Resource{
			SchemaURL:  []byte("https://schema/resource"),
			Attributes: NewAttributes(kv("service.name", sv("api")), kv("host", sv("h1"))),
		},
		Scope: Scope{
			Name:       []byte("my.lib"),
			Version:    []byte("1.2.3"),
			SchemaURL:  []byte("https://schema/scope"),
			Attributes: NewAttributes(kv("scope.attr", sv("x"))),
		},
		Attributes: NewAttributes(kv("http.route", sv("/orders"))),
	}
}

// TestIdentityIncludesResourceAndScope is the crux: series that differ only in Resource,
// only in Scope, or only in point attributes have distinct ids — identity is not the
// point-attribute bag alone.
func TestIdentityIncludesResourceAndScope(t *testing.T) {
	t.Parallel()

	a := sampleSeries()

	diffResourceAttr := sampleSeries()
	diffResourceAttr.Resource.Attributes = NewAttributes(kv("service.name", sv("web")), kv("host", sv("h1")))

	diffResourceSchema := sampleSeries()
	diffResourceSchema.Resource.SchemaURL = []byte("https://other")

	diffScopeName := sampleSeries()
	diffScopeName.Scope.Name = []byte("other.lib")

	diffScopeVersion := sampleSeries()
	diffScopeVersion.Scope.Version = []byte("9.9.9")

	diffPointAttr := sampleSeries()
	diffPointAttr.Attributes = NewAttributes(kv("http.route", sv("/items")))

	seen := map[SeriesID]string{a.Hash(): "base"}
	for name, s := range map[string]Series{
		"resourceAttr":   diffResourceAttr,
		"resourceSchema": diffResourceSchema,
		"scopeName":      diffScopeName,
		"scopeVersion":   diffScopeVersion,
		"pointAttr":      diffPointAttr,
	} {
		h := s.Hash()
		if prev, ok := seen[h]; ok {
			t.Fatalf("identity collision: %s == %s", name, prev)
		}

		seen[h] = name
	}

	// Identical identities hash equal.
	assert.Equal(t, a.Hash(), sampleSeries().Hash())
}

func TestSeriesBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	s := sampleSeries()
	got, n, err := DecodeSeries(s.AppendHashInput(nil))
	require.NoError(t, err)
	assert.True(t, s.Equal(got))
	assert.Equal(t, s.Hash(), got.Hash())
	assert.Positive(t, n)

	// Empty/zero-value identity also round-trips.
	zero, _, err := DecodeSeries(Series{}.AppendHashInput(nil))
	require.NoError(t, err)
	assert.True(t, Series{}.Equal(zero))
}

func TestSeriesEqualAndClone(t *testing.T) {
	t.Parallel()

	s := sampleSeries()
	cp := s.Clone()
	assert.True(t, s.Equal(cp))

	cp.Resource.Attributes[0].Value = sv("changed")
	assert.False(t, s.Equal(cp), "clone is independent")

	other := sampleSeries()
	other.Scope.Version = []byte("2.0.0")
	assert.False(t, s.Equal(other))
}

func TestSeriesDecodeRejectsTruncation(t *testing.T) {
	t.Parallel()

	full := sampleSeries().AppendHashInput(nil)
	for n := range full {
		_, _, err := DecodeSeries(full[:n])
		require.Errorf(t, err, "prefix %d should be rejected", n)
	}
}

func FuzzDecodeSeries(f *testing.F) {
	f.Add(sampleSeries().AppendHashInput(nil))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		s, _, err := DecodeSeries(data)
		if err != nil {
			return
		}
		got, _, err := DecodeSeries(s.AppendHashInput(nil))
		require.NoError(t, err)
		assert.True(t, s.Equal(got))
		assert.Equal(t, s.Hash(), got.Hash())
	})
}
