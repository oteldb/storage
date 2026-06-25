package series

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func sv(s string) signal.Value { return signal.StringValue([]byte(s)) }

func attrs(pairs ...string) signal.Attributes {
	kvs := make([]signal.KeyValue, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		kvs = append(kvs, signal.KeyValue{Key: []byte(pairs[i]), Value: sv(pairs[i+1])})
	}

	return signal.NewAttributes(kvs...)
}

// mk builds a series with a fixed resource/scope and the given point attributes.
func mk(service string, pointPairs ...string) signal.Series {
	return signal.Series{
		Resource:   signal.Resource{Attributes: attrs("service.name", service)},
		Scope:      signal.Scope{Name: []byte("lib"), Version: []byte("1.0")},
		Attributes: attrs(pointPairs...),
	}
}

func TestAddIsIdempotent(t *testing.T) {
	t.Parallel()

	ix := New()
	id1 := ix.Add(mk("api", "route", "/x", "code", "200"))
	id2 := ix.Add(mk("api", "code", "200", "route", "/x")) // same identity, attrs reordered

	assert.Equal(t, id1, id2, "equal identities ⇒ same id")
	assert.Equal(t, 1, ix.Len(), "re-adding does not store a second copy")
}

func TestDistinctByResourceAndAttrs(t *testing.T) {
	t.Parallel()

	ix := New()
	api := ix.Add(mk("api", "route", "/x"))
	web := ix.Add(mk("web", "route", "/x")) // differs only in resource
	other := ix.Add(mk("api", "route", "/y"))

	assert.NotEqual(t, api, web, "different resource ⇒ different series")
	assert.NotEqual(t, api, other)
	assert.Equal(t, 3, ix.Len())
}

func TestGetReconstructs(t *testing.T) {
	t.Parallel()

	ix := New()
	want := mk("api", "route", "/x")
	id := ix.Add(want)

	got, ok := ix.Get(id)
	require.True(t, ok)
	assert.True(t, got.Equal(want))

	_, ok = ix.Get(signal.SeriesID{Lo: 123})
	assert.False(t, ok)
	assert.True(t, ix.Has(id))
	assert.False(t, ix.Has(signal.SeriesID{Lo: 123}))
}

func TestAddRetainsACopy(t *testing.T) {
	t.Parallel()

	ix := New()
	s := mk("api", "route", "/x")
	id := ix.Add(s)
	s.Resource.Attributes[0].Value = sv("mutated") // mutate caller's struct after Add

	got, _ := ix.Get(id)
	assert.True(t, got.Equal(mk("api", "route", "/x")), "the index must retain a deep copy")
}

func TestForEach(t *testing.T) {
	t.Parallel()

	ix := New()
	ix.Add(mk("api", "route", "/x"))
	ix.Add(mk("web", "route", "/x"))

	seen := map[signal.SeriesID]bool{}
	ix.ForEach(func(id signal.SeriesID, _ signal.Series) { seen[id] = true })
	assert.Len(t, seen, 2)
}
