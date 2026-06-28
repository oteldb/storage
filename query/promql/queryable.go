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
	"sync"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
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
	labels  *labelCache
}

// NewQueryable returns a Prometheus storage.Queryable backed by fetcher for the given tenant. A
// single Queryable reused across queries (the embedder's per-tenant adapter) memoizes label
// projections, so repeated queries pay the projection cost once per series.
func NewQueryable(fetcher fetch.Fetcher, tenant signal.TenantID) *Queryable {
	return &Queryable{fetcher: fetcher, tenant: tenant, labels: newLabelCache()}
}

// Querier returns a querier over [mint, maxt] (Prometheus milliseconds).
func (q *Queryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &querier{fetcher: q.fetcher, tenant: q.tenant, mint: mint, maxt: maxt, labels: q.labels}, nil
}

// labelCache memoizes a series identity's projected Prometheus label set. The projection is a pure
// function of the content-addressed [signal.SeriesID], so a cached entry is always valid; the cache
// is bounded by series cardinality (the same set Prometheus keeps resident) and shared across the
// Queryable's queriers.
type labelCache struct {
	mu sync.RWMutex
	m  map[signal.SeriesID]labels.Labels
}

func newLabelCache() *labelCache { return &labelCache{m: make(map[signal.SeriesID]labels.Labels)} }

func (c *labelCache) get(id signal.SeriesID) (labels.Labels, bool) {
	c.mu.RLock()
	l, ok := c.m[id]
	c.mu.RUnlock()

	return l, ok
}

func (c *labelCache) put(id signal.SeriesID, l labels.Labels) {
	c.mu.Lock()
	c.m[id] = l
	c.mu.Unlock()
}

type querier struct {
	fetcher fetch.Fetcher
	tenant  signal.TenantID
	mint    int64
	maxt    int64
	labels  *labelCache
	// held are the fetched batches whose buffers back the returned series (zero-copy). They are kept
	// alive until [querier.Close] (after the engine has evaluated) and then released, recycling the
	// engine's result buffers via the fetch [fetch.Request.Recycle] lifecycle.
	held []*fetch.Batch
	// lb / scratch are reused across a Select's series to avoid per-series label-builder and per-label
	// text allocations.
	lb      labels.ScratchBuilder
	scratch []byte
}

// Select resolves the matchers to series over [mint, maxt]. Index-safe matchers are pushed
// into the fetch request; every fetched series is then re-checked against all matchers
// (with absent labels treated as the empty string) for exact Prometheus semantics.
//
// The returned series are zero-copy: each one's iterator reads its batch's timestamp/value slices
// directly (no per-sample copy or interface boxing). Those buffers stay valid until [querier.Close],
// which releases the batches — opting into the engine's buffer-recycling lifecycle (Recycle).
func (q *querier) Select(ctx context.Context, sortSeries bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	req := fetch.Request{
		Tenant:   q.tenant,
		Start:    msToNsClamp(q.mint, math.MinInt64),
		End:      msToNsClamp(q.maxt, math.MaxInt64),
		Matchers: PushableMatchers(matchers),
		Recycle:  true,
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
		lset, ok := q.labels.get(b.ID)
		if !ok {
			lset = q.promLabels(b.Series)
			q.labels.put(b.ID, lset)
		}

		if !MatchesAll(lset, matchers) {
			b.Release() // not part of the result — recycle its buffers now

			continue
		}

		q.held = append(q.held, b) // keep alive until Close; the series aliases its buffers
		series = append(series, &batchSeries{labels: lset, ts: b.Timestamps, vs: b.Values})
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

func (q *querier) Close() error {
	for _, b := range q.held {
		b.Release() // the engine is done evaluating — recycle the result buffers
	}

	q.held = nil

	return nil
}

// promLabels is [PromLabels] reusing the querier's scratch builder + text buffer across a Select's
// series (the per-series result labels are still freshly cloned by ScratchBuilder.Labels).
func (q *querier) promLabels(s signal.Series) labels.Labels {
	return promLabelsInto(&q.lb, &q.scratch, s)
}

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
	var (
		b       labels.ScratchBuilder
		scratch []byte
	)

	return promLabelsInto(&b, &scratch, s)
}

// promLabelsInto is [PromLabels] writing through a caller-owned scratch builder and text buffer so a
// loop over many series reuses both (only ScratchBuilder.Labels' final clone is per-series). The
// builder is reset on entry; scratch holds the most recent label's encoded text between calls.
func promLabelsInto(b *labels.ScratchBuilder, scratch *[]byte, s signal.Series) labels.Labels {
	b.Reset()

	add := func(name string, v signal.Value) {
		if _, hidden := hiddenLabels[name]; hidden {
			return
		}

		*scratch = v.AppendText((*scratch)[:0])
		b.Add(name, string(*scratch))
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

// batchSeries is a zero-copy storage.Series over a fetch batch: its iterator reads the batch's
// timestamp/value slices directly (converting the storage ns timeline to Prometheus ms on the fly),
// so a Select materializes no per-sample copy or interface boxing. The aliased buffers stay valid
// until the producing querier is closed (which releases the batch).
type batchSeries struct {
	labels labels.Labels
	ts     []int64
	vs     []float64
}

func (s *batchSeries) Labels() labels.Labels { return s.labels }

func (s *batchSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	if r, ok := it.(*batchSeriesIterator); ok { // reuse the engine's recycled iterator
		r.reset(s.ts, s.vs)

		return r
	}

	r := &batchSeriesIterator{}
	r.reset(s.ts, s.vs)

	return r
}

// batchSeriesIterator is a float-only chunkenc.Iterator over a batch's parallel ts/value slices.
type batchSeriesIterator struct {
	ts []int64
	vs []float64
	i  int
}

func (it *batchSeriesIterator) Next() chunkenc.ValueType {
	if it.i+1 < len(it.ts) {
		it.i++

		return chunkenc.ValFloat
	}

	it.i = len(it.ts)

	return chunkenc.ValNone
}

//nolint:govet // chunkenc.Iterator.Seek's signature (int64) ValueType is not io.Seeker's.
func (it *batchSeriesIterator) Seek(t int64) chunkenc.ValueType {
	if it.i < 0 {
		it.i = 0
	}

	for ; it.i < len(it.ts); it.i++ {
		if it.ts[it.i]/nsPerMs >= t {
			return chunkenc.ValFloat
		}
	}

	return chunkenc.ValNone
}

func (it *batchSeriesIterator) At() (int64, float64) { return it.ts[it.i] / nsPerMs, it.vs[it.i] }
func (it *batchSeriesIterator) AtT() int64           { return it.ts[it.i] / nsPerMs }
func (*batchSeriesIterator) AtST() int64             { return 0 } // float-only: no start timestamp

func (*batchSeriesIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

func (*batchSeriesIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

func (*batchSeriesIterator) Err() error { return nil }

func (it *batchSeriesIterator) reset(ts []int64, vs []float64) {
	it.ts, it.vs, it.i = ts, vs, -1
}

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
