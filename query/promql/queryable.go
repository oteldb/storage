// Package promql is an optional adapter that bridges the storage fetch contract
// (query/fetch) to the Prometheus storage.Queryable interface. It does not own a PromQL
// engine: the embedder (e.g. go-faster/oteldb, which already has PromQL/LogQL/TraceQL
// engines) drives its own promql.Engine over the [Queryable] this package returns. The
// storage library proper stops at the fetch seam and stays language- and Prometheus-free;
// this package is the only one importing github.com/prometheus/prometheus, and importing it
// is opt-in.
//
// Condition extraction lives here (the language layer), never in storage: a Prometheus
// matcher that can match the empty string (e.g. `!=`, `!~`, `=""`) would wrongly exclude
// series that lack the label if pushed into the postings index, so only index-safe matchers
// are pushed down; every returned series is then re-checked against the full matcher set.
package promql

import (
	"context"
	"math"
	"sort"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"
)

// msToNs and nsToMs convert between Prometheus' millisecond timeline and the storage
// nanosecond timeline.
const nsPerMs = int64(1e6)

// hiddenLabels are the internal reserved labels folded into a metric series' identity that
// are not part of its PromQL label set (unlike __name__, which is). They are stripped when
// projecting a series to Prometheus labels.
var hiddenLabels = map[string]struct{}{
	string(metric.LabelUnit):        {},
	string(metric.LabelKind):        {},
	string(metric.LabelTemporality): {},
	string(metric.LabelMonotonic):   {},
}

// Queryable adapts a [fetch.Fetcher] (one tenant's engine) to the Prometheus
// storage.Queryable interface for a single tenant.
type Queryable struct {
	fetcher fetch.Fetcher
	tenant  signal.TenantID
}

// NewQueryable returns a Prometheus storage.Queryable backed by fetcher for the given tenant.
func NewQueryable(fetcher fetch.Fetcher, tenant signal.TenantID) *Queryable {
	return &Queryable{fetcher: fetcher, tenant: tenant}
}

// Querier returns a querier over [mint, maxt] (Prometheus milliseconds).
func (q *Queryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &querier{fetcher: q.fetcher, tenant: q.tenant, mint: mint, maxt: maxt}, nil
}

type querier struct {
	fetcher fetch.Fetcher
	tenant  signal.TenantID
	mint    int64
	maxt    int64
}

// Select resolves the matchers to series over [mint, maxt]. Index-safe matchers are pushed
// into the fetch request; every fetched series is then re-checked against all matchers
// (with absent labels treated as the empty string) for exact Prometheus semantics.
func (q *querier) Select(ctx context.Context, sortSeries bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	req := fetch.Request{
		Tenant:   q.tenant,
		Start:    msToNsClamp(q.mint, math.MinInt64),
		End:      msToNsClamp(q.maxt, math.MaxInt64),
		Matchers: PushableMatchers(matchers),
	}

	it, err := q.fetcher.Fetch(ctx, req)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	series := make([]storage.Series, 0, len(batches))
	for _, b := range batches {
		lset := PromLabels(b.Series)
		if !MatchesAll(lset, matchers) {
			continue
		}

		series = append(series, storage.NewListSeries(lset, floatSamples(b.Timestamps, b.Values)))
	}

	if sortSeries {
		sort.Slice(series, func(i, j int) bool { return labels.Compare(series[i].Labels(), series[j].Labels()) < 0 })
	}

	return newSliceSeriesSet(series)
}

// LabelValues returns the distinct values of name across the series matching matchers over the
// querier window. It backs the Prometheus /api/v1/label/<name>/values endpoint (and so Grafana's
// metric/label browser). The promql.Engine never calls it for evaluation.
func (q *querier) LabelValues(
	ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	sets, err := q.seriesLabels(ctx, matchers)
	if err != nil {
		return nil, nil, err
	}

	seen := map[string]struct{}{}
	for _, lset := range sets {
		if v := lset.Get(name); v != "" {
			seen[v] = struct{}{}
		}
	}

	return sortedKeys(seen), nil, nil
}

// LabelNames returns the distinct label names across the series matching matchers over the querier
// window. It backs the Prometheus /api/v1/labels endpoint.
func (q *querier) LabelNames(
	ctx context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	sets, err := q.seriesLabels(ctx, matchers)
	if err != nil {
		return nil, nil, err
	}

	seen := map[string]struct{}{}
	for _, lset := range sets {
		lset.Range(func(l labels.Label) { seen[l.Name] = struct{}{} })
	}

	return sortedKeys(seen), nil, nil
}

func (q *querier) Close() error { return nil }

// seriesLabels fetches the matching series over the querier window and projects each to its
// Prometheus label set. It mirrors Select's matching (push the index-safe matchers, then re-check
// every series against the full set) but keeps only the identities, not the samples.
func (q *querier) seriesLabels(ctx context.Context, matchers []*labels.Matcher) ([]labels.Labels, error) {
	req := fetch.Request{
		Tenant:   q.tenant,
		Start:    msToNsClamp(q.mint, math.MinInt64),
		End:      msToNsClamp(q.maxt, math.MaxInt64),
		Matchers: PushableMatchers(matchers),
	}

	it, err := q.fetcher.Fetch(ctx, req)
	if err != nil {
		return nil, err
	}

	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		return nil, err
	}

	out := make([]labels.Labels, 0, len(batches))
	for _, b := range batches {
		lset := PromLabels(b.Series)
		if !MatchesAll(lset, matchers) {
			continue
		}

		out = append(out, lset)
	}

	return out, nil
}

// sortedKeys returns the keys of set in sorted order (Prometheus label APIs return sorted results).
func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

// PushableMatchers returns fetch matchers for the index-safe subset: matchers that do not
// match the empty string. A matcher that matches "" (negated/absent) cannot prune via the
// postings index without wrongly dropping series that lack the label, so it is enforced only
// by the post-fetch re-check in [MatchesAll].
//
// It is exported so an embedder building a pushdown path over the fetch/aggregate seam (e.g.
// oteldb's *_over_time aggregate pushdown) can lower the same index-safe matcher set the
// [Queryable] uses, keeping matcher translation in one place.
func PushableMatchers(ms []*labels.Matcher) []fetch.Matcher {
	out := make([]fetch.Matcher, 0, len(ms))
	for _, m := range ms {
		if m.Matches("") {
			continue
		}

		fm := fetch.Matcher{Name: []byte(m.Name), Match: valuePredicate(m)}
		if m.Type == labels.MatchEqual {
			// Equality is serializable and exact: let the cluster fan-out push it to peers.
			fm.Spec = &fetch.EqualMatcher{Name: m.Name, Value: m.Value}
		}

		out = append(out, fm)
	}

	return out
}

// valuePredicate lowers a Prometheus matcher to a fetch predicate over the typed value: the
// value's canonical text projection is matched by the matcher.
func valuePredicate(m *labels.Matcher) func(signal.Value) bool {
	return func(v signal.Value) bool { return m.Matches(string(v.AppendText(nil))) }
}

// MatchesAll reports whether a series' labels satisfy every matcher, treating an absent
// label as the empty string (Prometheus semantics). It is the post-fetch re-check companion to
// [PushableMatchers]: exported so an embedder's pushdown path re-checks the full matcher set the
// same way [Queryable] does.
func MatchesAll(lset labels.Labels, ms []*labels.Matcher) bool {
	for _, m := range ms {
		if !m.Matches(lset.Get(m.Name)) {
			return false
		}
	}

	return true
}

// PromLabels projects a storage series identity to a Prometheus label set: every resource,
// scope, and (folded) point attribute becomes a label, with the internal reserved labels
// (except __name__) hidden. Scope name/version are exposed under the otel.scope.* keys, the
// same labels the head indexes.
//
// PromLabels is exported so an embedder can render a [signal.Series] (e.g. the identity carried
// alongside an [github.com/oteldb/storage.Storage.AggregateMetrics] result) as PromQL labels
// without duplicating this projection.
func PromLabels(s signal.Series) labels.Labels {
	b := labels.NewScratchBuilder(0)

	add := func(name string, v signal.Value) {
		if _, hidden := hiddenLabels[name]; hidden {
			return
		}

		b.Add(name, string(v.AppendText(nil)))
	}

	for i := range s.Resource.Attributes {
		add(string(s.Resource.Attributes[i].Key), s.Resource.Attributes[i].Value)
	}

	if len(s.Scope.Name) > 0 {
		b.Add("otel.scope.name", string(s.Scope.Name))
	}

	if len(s.Scope.Version) > 0 {
		b.Add("otel.scope.version", string(s.Scope.Version))
	}

	for i := range s.Scope.Attributes {
		add(string(s.Scope.Attributes[i].Key), s.Scope.Attributes[i].Value)
	}

	for i := range s.Attributes {
		add(string(s.Attributes[i].Key), s.Attributes[i].Value)
	}

	b.Sort()

	return b.Labels()
}

// floatSamples converts the storage ns timeline to Prometheus float samples (ms).
func floatSamples(ts []int64, values []float64) []chunks.Sample {
	out := make([]chunks.Sample, len(ts))
	for i := range ts {
		out[i] = chunkSample{t: ts[i] / nsPerMs, v: values[i]}
	}

	return out
}

func msToNsClamp(ms, clamp int64) int64 {
	// Any ms outside the range representable in nanoseconds collapses to the open-ended clamp:
	// otherwise ms*nsPerMs overflows into a garbage window. This covers the MinInt64/MaxInt64
	// sentinels and the Prometheus MinTime/MaxTime an unbounded label/metadata query arrives with.
	const maxMs = math.MaxInt64 / nsPerMs
	if ms < -maxMs || ms > maxMs {
		return clamp
	}

	return ms * nsPerMs
}

// chunkSample is a minimal float-only chunks.Sample.
type chunkSample struct {
	t int64
	v float64
}

func (s chunkSample) T() int64                    { return s.t }
func (chunkSample) ST() int64                     { return 0 } // no created/start timestamp for float-only samples
func (s chunkSample) F() float64                  { return s.v }
func (chunkSample) H() *histogram.Histogram       { return nil }
func (chunkSample) FH() *histogram.FloatHistogram { return nil }
func (chunkSample) Type() chunkenc.ValueType      { return chunkenc.ValFloat }
func (s chunkSample) Copy() chunks.Sample         { return s }

// sliceSeriesSet is a storage.SeriesSet over a fixed slice of series.
type sliceSeriesSet struct {
	series []storage.Series
	i      int
}

func newSliceSeriesSet(series []storage.Series) *sliceSeriesSet {
	return &sliceSeriesSet{series: series, i: -1}
}

func (s *sliceSeriesSet) Next() bool {
	s.i++

	return s.i < len(s.series)
}

func (s *sliceSeriesSet) At() storage.Series                { return s.series[s.i] }
func (s *sliceSeriesSet) Err() error                        { return nil }
func (s *sliceSeriesSet) Warnings() annotations.Annotations { return nil }
