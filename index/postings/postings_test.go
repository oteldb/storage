package postings

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/signal"
)

// Symbol ids for the test fixtures (in production these come from index/symbols).
const (
	nJob  uint32 = 1
	nEnv  uint32 = 2
	nCode uint32 = 3

	vAPI  uint32 = 10
	vWeb  uint32 = 11
	vProd uint32 = 12
	vDev  uint32 = 13
	vInt5 uint32 = 20
	vStr5 uint32 = 21
)

// buildIndex builds three series:
//
//	id 1: job=api, env=prod
//	id 2: job=api, env=dev
//	id 3: job=web, env=prod
func buildIndex() *MemPostings {
	p := NewMemPostings()
	p.Add(sid(1), nJob, vAPI)
	p.Add(sid(1), nEnv, vProd)
	p.Add(sid(2), nJob, vAPI)
	p.Add(sid(2), nEnv, vDev)
	p.Add(sid(3), nJob, vWeb)
	p.Add(sid(3), nEnv, vProd)

	return p
}

func TestGetAndPrimitives(t *testing.T) {
	t.Parallel()
	p := buildIndex()

	assert.Equal(t, sortedSet(1, 2), mustSlice(t, p.Get(nJob, vAPI)))
	assert.Equal(t, sortedSet(1, 3), mustSlice(t, p.Get(nEnv, vProd)))
	assert.Empty(t, mustSlice(t, p.Get(nJob, 999)))
	assert.Empty(t, mustSlice(t, p.Get(404, vAPI)))

	assert.Equal(t, sortedSet(1, 2, 3), mustSlice(t, p.All()))
	assert.Equal(t, sortedSet(1, 2, 3), mustSlice(t, p.ForName(nJob)))
	assert.Empty(t, mustSlice(t, p.ForName(404)))

	assert.Empty(t, mustSlice(t, p.WithoutName(nJob)), "every series has job")
	assert.Equal(t, sortedSet(1, 2, 3), mustSlice(t, p.WithoutName(404)), "no series has that name")

	assert.Equal(t, []uint32{vAPI, vWeb}, p.LabelValues(nJob))
	assert.Nil(t, p.LabelValues(404))
}

// TestSelectTypedCallback resolves a value predicate via a caller-side decoder, the way
// a query language would: the predicate receives a value id, decodes it to a typed
// signal.Value, and applies a rule.
func TestSelectTypedCallback(t *testing.T) {
	t.Parallel()
	p := buildIndex()

	decode := map[uint32]signal.Value{
		vAPI:  signal.StringValue([]byte("api")),
		vWeb:  signal.StringValue([]byte("web")),
		vProd: signal.StringValue([]byte("prod")),
		vDev:  signal.StringValue([]byte("dev")),
	}
	re := regexp.MustCompile("^(?:a.*)$")

	got := mustSlice(t, p.Select(nJob, func(valueID uint32) bool {
		return re.Match(decode[valueID].Str())
	}))
	assert.Equal(t, sortedSet(1, 2), got)

	// A custom, non-regexp rule.
	got = mustSlice(t, p.Select(nJob, func(valueID uint32) bool {
		return len(decode[valueID].Str()) == 3 && decode[valueID].Str()[0] == 'w'
	}))
	assert.Equal(t, sortedSet(3), got)

	assert.Empty(t, mustSlice(t, p.Select(404, func(uint32) bool { return true })))
}

// TestTypedValueBucketsAreDistinct is the crux of keying on typed value ids: int 5 and
// string "5" interned from their type-tagged encodings land in different buckets, so a
// typed predicate can tell them apart.
func TestTypedValueBucketsAreDistinct(t *testing.T) {
	t.Parallel()

	p := NewMemPostings()
	p.Add(sid(4), nCode, vInt5) // series 4: code is the integer five
	p.Add(sid(5), nCode, vStr5) // series 5: code is the string five

	assert.Equal(t, sortedSet(4), mustSlice(t, p.Get(nCode, vInt5)))
	assert.Equal(t, sortedSet(5), mustSlice(t, p.Get(nCode, vStr5)))

	decode := map[uint32]signal.Value{vInt5: signal.IntValue(5), vStr5: signal.StringValue([]byte("5"))}
	got := mustSlice(t, p.Select(nCode, func(valueID uint32) bool {
		v := decode[valueID]

		return v.Kind() == signal.KindInt && v.Int() == 5
	}))
	assert.Equal(t, sortedSet(4), got, `typed predicate matches int 5, not string "5"`)
}

func TestResolveMatchers(t *testing.T) {
	t.Parallel()
	p := buildIndex()

	decode := map[uint32]signal.Value{
		vAPI: signal.StringValue([]byte("api")), vWeb: signal.StringValue([]byte("web")),
		vProd: signal.StringValue([]byte("prod")), vDev: signal.StringValue([]byte("dev")),
	}
	jobRe := regexp.MustCompile("^(?:a.*)$")
	envRe := regexp.MustCompile("^(?:prod)$")

	// job=~"a.*" AND env="prod"
	got := mustSlice(t, p.Resolve(
		Matcher{NameID: nJob, Match: func(v uint32) bool { return jobRe.Match(decode[v].Str()) }},
		Matcher{NameID: nEnv, Match: func(v uint32) bool { return envRe.Match(decode[v].Str()) }},
	))
	assert.Equal(t, sortedSet(1), got)

	assert.Equal(t, sortedSet(1, 2, 3), mustSlice(t, p.Resolve()), "no matchers ⇒ all")
}

// TestComposedNegation shows the language layer composes negation and equality from the
// storage primitives, keeping storage free of operators.
func TestComposedNegation(t *testing.T) {
	t.Parallel()
	p := buildIndex()

	// job="api" (fast path via Get)
	assert.Equal(t, sortedSet(1, 2), mustSlice(t, p.Get(nJob, vAPI)))
	// job!="api" = All \ Get(job, api)
	assert.Equal(t, sortedSet(3), mustSlice(t, Without(p.All(), p.Get(nJob, vAPI))))
}

func TestDedupAcrossDuplicateAdds(t *testing.T) {
	t.Parallel()

	p := NewMemPostings()
	p.Add(sid(5), nJob, vAPI)
	p.Add(sid(5), nJob, vAPI) // same series, same label (e.g. WAL replay)

	assert.Equal(t, sortedSet(5), mustSlice(t, p.Get(nJob, vAPI)))
	assert.Equal(t, sortedSet(5), mustSlice(t, p.All()))
}

func TestAddThenReadThenAdd(t *testing.T) {
	t.Parallel()

	p := buildIndex()
	_ = mustSlice(t, p.All()) // forces a sort
	p.Add(sid(4), nJob, vAPI)

	assert.Equal(t, sortedSet(1, 2, 4), mustSlice(t, p.Get(nJob, vAPI)), "re-sorts after a later add")
}
