package fetch

import (
	"context"

	"github.com/oteldb/storage/signal"
)

// A producer may answer with a superset of what a [Request] selects: the cluster fan-out forwards
// only the serializable ([Matcher.Spec]) subset of a matcher set, so a regex or negated matcher
// never reaches the peer that produced the answer. Narrowing it back is the requester's job, and
// this is the one implementation of it — a consumer of any superset-returning Fetcher needs it, so
// it lives on the seam rather than in each of them.

// MatchesSeries reports whether s satisfies every matcher, each over the present value of its named
// label as [SeriesLabel] resolves it.
//
// The semantics are positive: a series that does not carry a matcher's label fails it, the same way
// the engine's postings resolution selects. Absent-label and negated handling stays in the language
// layer, which resolves them against the full label set rather than one series' identity.
func MatchesSeries(s signal.Series, matchers []Matcher) bool {
	for i := range matchers {
		v, ok := SeriesLabel(s, matchers[i].Name)
		if !ok || !matchers[i].Match(v) {
			return false
		}
	}

	return true
}

// SeriesLabel resolves a label value from a series the way the engine indexes it for matching
// (recordengine's indexLabels, a metric's series labels): the series' own attributes, then the
// resource and scope attributes, then the reserved scope name/version labels. That is what lets a
// matcher on a resource label (service.name, say) re-filter a record signal's superset correctly.
func SeriesLabel(s signal.Series, name []byte) (signal.Value, bool) {
	if v, ok := s.Attributes.Get(name); ok {
		return v, true
	}

	if v, ok := s.Resource.Attributes.Get(name); ok {
		return v, true
	}

	if v, ok := s.Scope.Attributes.Get(name); ok {
		return v, true
	}

	switch string(name) {
	case signal.LabelScopeName:
		if len(s.Scope.Name) > 0 {
			return signal.StringValue(s.Scope.Name), true
		}
	case signal.LabelScopeVersion:
		if len(s.Scope.Version) > 0 {
			return signal.StringValue(s.Scope.Version), true
		}
	}

	return signal.Value{}, false
}

// Filter wraps f so that batches whose identity fails the request's matchers are dropped, turning a
// superset answer into the exact one the request asks for.
//
// It is deliberately not an [Unwraper]: the pushdown capabilities reached through the wrapper chain
// ([Counter], [SeriesLister]) would answer about the unfiltered inner result, which is a wrong
// answer, not a coarse one.
func Filter(f Fetcher) Fetcher { return filterFetcher{inner: f} }

type filterFetcher struct {
	inner Fetcher
}

func (f filterFetcher) Fetch(ctx context.Context, r Request) (Iterator, error) {
	it, err := f.inner.Fetch(ctx, r)
	if err != nil {
		return nil, err
	}

	batches, err := Drain(ctx, it)
	if err != nil {
		return nil, err
	}

	if len(r.Matchers) == 0 {
		return NewSliceIterator(batches), nil
	}

	kept := batches[:0]
	for _, b := range batches {
		if MatchesSeries(b.Series, r.Matchers) {
			kept = append(kept, b)
		}
	}

	return NewSliceIterator(kept), nil
}

// FilterSeries returns the identities of series satisfying every matcher, reusing the input's
// backing array. It is [MatchesSeries] over a list — what an enumeration RPC's superset needs.
func FilterSeries(series []signal.Series, matchers []Matcher) []signal.Series {
	if len(matchers) == 0 {
		return series
	}

	kept := series[:0]
	for i := range series {
		if MatchesSeries(series[i], matchers) {
			kept = append(kept, series[i])
		}
	}

	return kept
}

// EqualitySpecs extracts the serializable subset of a matcher set — the only part a peer can apply,
// and so what a fan-out pushes down (see [Matcher.Spec]).
func EqualitySpecs(matchers []Matcher) []EqualMatcher {
	var eq []EqualMatcher

	for i := range matchers {
		if matchers[i].Spec != nil {
			eq = append(eq, *matchers[i].Spec)
		}
	}

	return eq
}
