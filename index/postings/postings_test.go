package postings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

// buildIndex builds three series:
//
//	id 1: job=api, env=prod
//	id 2: job=api, env=dev
//	id 3: job=web, env=prod
func buildIndex() *MemPostings {
	p := NewMemPostings()
	add := func(s signal.SeriesID, name, value string) { p.Add(s, []byte(name), []byte(value)) }
	add(sid(1), "job", "api")
	add(sid(1), "env", "prod")
	add(sid(2), "job", "api")
	add(sid(2), "env", "dev")
	add(sid(3), "job", "web")
	add(sid(3), "env", "prod")

	return p
}

func eq(name, value string) Matcher {
	return Matcher{Op: MatchEqual, Name: []byte(name), Value: []byte(value)}
}

func TestGetAndPrimitives(t *testing.T) {
	t.Parallel()
	p := buildIndex()

	assert.Equal(t, sortedSet(1, 2), mustSlice(t, p.Get([]byte("job"), []byte("api"))))
	assert.Equal(t, sortedSet(1, 3), mustSlice(t, p.Get([]byte("env"), []byte("prod"))))
	assert.Empty(t, mustSlice(t, p.Get([]byte("job"), []byte("none"))))
	assert.Empty(t, mustSlice(t, p.Get([]byte("missing"), []byte("x"))))

	assert.Equal(t, sortedSet(1, 2, 3), mustSlice(t, p.All()))
	assert.Equal(t, sortedSet(1, 2, 3), mustSlice(t, p.ForName([]byte("job"))))
	assert.Empty(t, mustSlice(t, p.ForName([]byte("region"))))

	assert.Empty(t, mustSlice(t, p.WithoutName([]byte("job"))), "every series has job")
	assert.Equal(t, sortedSet(1, 2, 3), mustSlice(t, p.WithoutName([]byte("region"))), "no series has region")

	assert.Equal(t, [][]byte{[]byte("api"), []byte("web")}, p.LabelValues([]byte("job")))
	assert.Nil(t, p.LabelValues([]byte("region")))
}

func TestResolveMatchers(t *testing.T) {
	t.Parallel()
	p := buildIndex()

	cases := []struct {
		name string
		ms   []Matcher
		want []signal.SeriesID
	}{
		{"none ⇒ all", nil, sortedSet(1, 2, 3)},
		{"equal", []Matcher{eq("job", "api")}, sortedSet(1, 2)},
		{"intersection", []Matcher{eq("job", "api"), eq("env", "prod")}, sortedSet(1)},
		{"not-equal", []Matcher{{Op: MatchNotEqual, Name: []byte("job"), Value: []byte("api")}}, sortedSet(3)},
		{"regexp", []Matcher{{Op: MatchRegexp, Name: []byte("job"), Value: []byte("a.*")}}, sortedSet(1, 2)},
		{"regexp-exact-anchored", []Matcher{{Op: MatchRegexp, Name: []byte("job"), Value: []byte("ap")}}, nil},
		{"not-regexp", []Matcher{{Op: MatchNotRegexp, Name: []byte("env"), Value: []byte("pro.*")}}, sortedSet(2)},
		{"regexp ∩ equal", []Matcher{{Op: MatchRegexp, Name: []byte("job"), Value: []byte(".*")}, eq("env", "dev")}, sortedSet(2)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			it, err := p.Resolve(tc.ms...)
			require.NoError(t, err)

			got, err := ToSlice(it)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestResolveInvalidRegexp(t *testing.T) {
	t.Parallel()
	p := buildIndex()

	_, err := p.Resolve(Matcher{Op: MatchRegexp, Name: []byte("job"), Value: []byte("a(")})
	require.Error(t, err)

	_, err = p.Resolve(Matcher{Op: MatchNotRegexp, Name: []byte("job"), Value: []byte("a(")})
	require.Error(t, err)

	_, err = p.Resolve(Matcher{Op: MatchOp(99), Name: []byte("job")})
	require.Error(t, err)
}

func TestDedupAcrossDuplicateAdds(t *testing.T) {
	t.Parallel()

	p := NewMemPostings()
	p.Add(sid(5), []byte("job"), []byte("api"))
	p.Add(sid(5), []byte("job"), []byte("api")) // same series, same label (e.g. WAL replay)

	assert.Equal(t, sortedSet(5), mustSlice(t, p.Get([]byte("job"), []byte("api"))))
	assert.Equal(t, sortedSet(5), mustSlice(t, p.All()))
}

func TestAddThenReadThenAdd(t *testing.T) {
	t.Parallel()

	p := buildIndex()
	_ = mustSlice(t, p.All()) // forces a sort
	p.Add(sid(4), []byte("job"), []byte("api"))

	assert.Equal(t, sortedSet(1, 2, 4), mustSlice(t, p.Get([]byte("job"), []byte("api"))), "re-sorts after a later add")
}
