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

// Filter wraps f so that a superset answer is narrowed to exactly what the request asks for:
// batches whose identity fails the request's matchers are dropped, surviving batches are compacted
// to the rows satisfying every columnar condition, and a batch rejected by SecondPass is dropped.
//
// The conditions matter as much as the matchers. A request may carry no matchers at all and express
// its whole predicate columnar-side — trace-by-id is an equality condition on the trace id and
// nothing else — and those never cross the wire ([Request.AllConditions] lets a producer skip them
// outright). Narrowing only by matchers would return the producer's full window for such a request.
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

	if len(r.Matchers) == 0 && len(r.Conditions) == 0 && r.SecondPass == nil {
		return NewSliceIterator(batches), nil
	}

	kept := batches[:0]
	for _, b := range batches {
		if !MatchesSeries(b.Series, r.Matchers) {
			continue
		}

		// A producer that did not apply the columnar conditions answers with every row of the
		// matched identities, so they are re-applied per row here. Without this a request whose
		// whole predicate is a condition (trace-by-id) would keep the entire window.
		if len(r.Conditions) > 0 && FilterRows(b, r.Conditions) == 0 {
			continue
		}

		if r.SecondPass != nil && !r.SecondPass(b) {
			continue
		}

		kept = append(kept, b)
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

// AttrsColumn is the conventional name of a record schema's serialized per-record attributes column
// (the [signal] codec's key→value blob). A condition naming a column the schema does not carry is
// resolved against it, mirroring how the record engine evaluates an attribute predicate.
const AttrsColumn = "attrs"

// TimestampColumn is the conventional name of a record's implicit timestamp column, which a batch
// carries as [Batch.Timestamps] rather than a [NamedColumn].
const TimestampColumn = "ts"

// RowValue returns the typed value of the named column for row i of b, mirroring the record
// engine's per-row column access: the implicit timestamp, then a materialized column (int64 →
// [signal.IntValue], float64 → [signal.DoubleValue], bytes → [signal.StringValue]), then a
// per-record attribute decoded from the [AttrsColumn] blob.
//
// The bool reports whether the row carries the column at all; absence is the caller's to judge (see
// [Condition]), not a non-match this decides.
func RowValue(b *Batch, i int, name string) (signal.Value, bool) {
	if name == TimestampColumn {
		return signal.IntValue(b.Timestamps[i]), true
	}

	if col, ok := b.Column(name); ok {
		switch {
		case col.Int64 != nil:
			return signal.IntValue(col.Int64[i]), true
		case col.Float64 != nil:
			return signal.DoubleValue(col.Float64[i]), true
		case col.Bytes != nil:
			return signal.StringValue(col.Bytes[i]), true
		}
	}

	attrs, ok := b.Column(AttrsColumn)
	if !ok || attrs.Bytes == nil {
		return signal.Value{}, false
	}

	v, found, err := signal.LookupAttribute(attrs.Bytes[i], name)
	if err != nil || !found {
		return signal.Value{}, false
	}

	return v, true
}

// MatchesRow reports whether row i of b satisfies every condition (logical AND). A column the row
// does not carry is offered to the predicate as [signal.EmptyValue].
func MatchesRow(b *Batch, i int, conds []Condition) bool {
	for j := range conds {
		v, ok := RowValue(b, i, conds[j].Column)
		if !ok {
			v = signal.EmptyValue()
		}

		if !conds[j].Match(v) {
			return false
		}
	}

	return true
}

// FilterRows compacts b in place to the rows satisfying every condition, reusing the backing arrays,
// and reports how many survived. It is [MatchesRow] over a batch — what a superset answer from a
// producer that did not apply the conditions needs.
func FilterRows(b *Batch, conds []Condition) int {
	idx := make([]int, 0, len(b.Timestamps))
	for i := range b.Timestamps {
		if MatchesRow(b, i, conds) {
			idx = append(idx, i)
		}
	}

	if len(idx) == len(b.Timestamps) {
		return len(idx)
	}

	gatherInt64(&b.Timestamps, idx)
	gatherFloat64(&b.Values, idx)
	gatherFloat64(&b.ScaleFactors, idx)

	for k := range b.Columns {
		gatherInt64(&b.Columns[k].Int64, idx)
		gatherFloat64(&b.Columns[k].Float64, idx)
		gatherBytes(&b.Columns[k].Bytes, idx)
	}

	return len(idx)
}

func gatherInt64(dst *[]int64, idx []int) {
	s := *dst
	if s == nil {
		return
	}

	for p, i := range idx {
		s[p] = s[i]
	}

	*dst = s[:len(idx)]
}

func gatherFloat64(dst *[]float64, idx []int) {
	s := *dst
	if s == nil {
		return
	}

	for p, i := range idx {
		s[p] = s[i]
	}

	*dst = s[:len(idx)]
}

func gatherBytes(dst *[][]byte, idx []int) {
	s := *dst
	if s == nil {
		return
	}

	for p, i := range idx {
		s[p] = s[i]
	}

	*dst = s[:len(idx)]
}
