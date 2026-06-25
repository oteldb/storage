package postings

import (
	"regexp"
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

// re builds a fully-anchored regexp predicate, as a query-language layer would.
func re(pattern string) func([]byte) bool {
	r := regexp.MustCompile("^(?:" + pattern + ")$")

	return r.Match
}

func matcher(name string, match func([]byte) bool) Matcher {
	return Matcher{Name: []byte(name), Match: match}
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

func TestSelectPredicate(t *testing.T) {
	t.Parallel()
	p := buildIndex()

	// regexp predicate (supplied by the caller, not storage)
	assert.Equal(t, sortedSet(1, 2), mustSlice(t, p.Select([]byte("job"), re("a.*"))))
	assert.Equal(t, sortedSet(1, 2, 3), mustSlice(t, p.Select([]byte("env"), re("(prod|dev)"))))
	// arbitrary custom predicate: values starting with 'w'
	assert.Equal(t, sortedSet(3), mustSlice(t, p.Select([]byte("job"), func(v []byte) bool { return v[0] == 'w' })))
	// no match, and unknown name
	assert.Empty(t, mustSlice(t, p.Select([]byte("job"), re("nope"))))
	assert.Empty(t, mustSlice(t, p.Select([]byte("region"), re(".*"))))
}

func TestResolveCallbackMatchers(t *testing.T) {
	t.Parallel()
	p := buildIndex()

	cases := []struct {
		name string
		ms   []Matcher
		want []signal.SeriesID
	}{
		{"none ⇒ all", nil, sortedSet(1, 2, 3)},
		{"single predicate", []Matcher{matcher("job", re("a.*"))}, sortedSet(1, 2)},
		{"intersection", []Matcher{matcher("job", re("a.*")), matcher("env", re("prod"))}, sortedSet(1)},
		{"anchored, no partial", []Matcher{matcher("job", re("ap"))}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ToSlice(p.Resolve(tc.ms...))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestComposedNegation shows the language layer composes negation and equality from the
// storage primitives, keeping storage free of operators.
func TestComposedNegation(t *testing.T) {
	t.Parallel()
	p := buildIndex()

	// job="api" (fast path via Get)
	assert.Equal(t, sortedSet(1, 2), mustSlice(t, p.Get([]byte("job"), []byte("api"))))
	// job!="api" = All \ Get(job,api)
	assert.Equal(t, sortedSet(3), mustSlice(t, Without(p.All(), p.Get([]byte("job"), []byte("api")))))
	// job!~"a.*" = All \ Select(job, /a.*/)
	assert.Equal(t, sortedSet(3), mustSlice(t, Without(p.All(), p.Select([]byte("job"), re("a.*")))))
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
