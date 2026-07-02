// Package fetch is the storage seam: the contract every query language compiles to and
// every data source (head, parts, cluster fan-out) implements. A [Request] of label
// matchers + a time window resolves to an [Iterator] of lazily-produced [Batch]es.
//
// The contract is dual-shape. For metrics, a batch is one matching series carrying its sample
// columns. For logs, label Matchers resolve a stream and columnar Conditions filter its records;
// a batch carries the per-record Columns. Projection narrows the materialized columns and an
// optional SecondPass post-filters. Nested reconstruction (traces) extends it later; the seam
// stays the same.
package fetch

import (
	"context"
	"errors"
	"io"

	"github.com/oteldb/storage/signal"
)

// Matcher is one label condition: the predicate Match selects which values of the
// attribute Name satisfy it. The condition is a **callback**, not an operator enum, so
// the contract carries no query-language semantics — a language supplies the predicate (a
// compiled regexp, an exact compare, a typed numeric range, a custom rule) over the typed
// [signal.Value]. A [Fetcher] applies Match while scanning the label's distinct values.
//
// Negation and absent-label semantics compose at the language layer (a fetcher selects
// the matching values; the language decides whether to complement the result).
type Matcher struct {
	Name  []byte
	Match func(value signal.Value) bool

	// Spec is an optional **serializable** form of an equality predicate, set by the language
	// layer when Match is an exact compare. It lets the cluster fan-out push a selective
	// matcher (e.g. `__name__="metric"`) to a peer node — the Match closure cannot cross the
	// wire. It is metadata only: a [Fetcher] always matches via Match; a peer reconstructs an
	// equivalent closure from Spec. Only equality is carried (it is exact, so a peer's pushdown
	// never drops a matching series); other predicates fall back to the requester's re-check.
	Spec *EqualMatcher
}

// EqualMatcher is the serializable form of an exact label-equality predicate (see
// [Matcher.Spec]). [EqualMatcher.Predicate] reconstructs the equivalent [Matcher.Match].
type EqualMatcher struct {
	Name  string
	Value string
}

// Predicate returns the Match closure equivalent to this equality: the label's canonical text
// projection equals Value (the same comparison the language layer's exact matcher makes).
func (m EqualMatcher) Predicate() func(signal.Value) bool {
	return func(v signal.Value) bool { return string(v.AppendText(nil)) == m.Value }
}

// Request selects series for a tenant within an inclusive time window, filtered by all
// matchers (their intersection).
//
// The contract is **dual-shape** (DESIGN §7): Matchers resolve identity over the postings index
// (a metric series, a log stream), while Conditions filter the per-record columns *within* that
// identity (a log record's severity, body, attributes). Metrics use only Matchers; logs use both.
// The columnar fields are all zero-valued for a metrics request, so the metrics path is unaffected.
type Request struct {
	Tenant     signal.TenantID
	Signal     signal.Signal // 0 ⇒ metric (the default vertical); Log for the logs read path
	Start, End int64         // unix nanos, inclusive
	Matchers   []Matcher

	// Conditions are columnar predicates applied per record (logs). Each names a column and
	// carries an operator-free Match callback over the row's typed value (mirroring Matcher).
	Conditions []Condition
	// AllConditions, when true, ANDs the conditions; a fetcher may still return a superset (an
	// approximate index like a bloom is re-checked by the requester). False ⇒ the fetcher need
	// not apply them at all (pure fetch-all; the caller filters).
	AllConditions bool
	// Projection names the columns to materialize for surviving rows (the second pass). Empty ⇒
	// the fetcher's default column set. Filter columns are decoded regardless of Projection.
	Projection []string
	// SecondPass, when set, is an engine-side row filter applied after the column Conditions —
	// for predicates not expressible as a single-column Match (e.g. a per-record attribute
	// decoded from the serialized attrs column). It sees the candidate row's materialized Batch.
	SecondPass func(*Batch) bool

	// Limit, when > 0, bounds the records returned to the most recent (Reverse) or oldest by
	// timestamp across all matched streams — the ordered top-N pushdown for limited log queries.
	// The result is a SUPERSET: the fetcher returns the Limit rows beyond the boundary timestamp
	// plus any rows that tie at that boundary, so a caller applying its own exact ordering+limit
	// never loses a boundary row (the fetch contract already permits a superset). It composes with
	// Matchers/Conditions/SecondPass — filtering happens first, the limit selects over survivors.
	// 0 ⇒ unlimited (every matching record). Honored by the record engine (logs/traces/profiles);
	// the metric engine ignores it (PromQL needs every sample).
	Limit int
	// Reverse selects the Limit direction: true keeps the newest records (largest timestamps, the
	// usual log-query default); false keeps the oldest. Ignored when Limit == 0.
	Reverse bool

	// Recycle opts the fetch into buffer pooling: the caller promises to call [Batch.Release] on
	// each returned batch once done with it, so the engine may hand out (and later reuse) pooled
	// result buffers. Default false — the engine allocates fresh buffers and the caller need not
	// release (so the non-recycling path takes no pool overhead at all). Misuse (reading a batch
	// after Release, or not releasing while Recycle is set) only forfeits the reuse, except that
	// reading after Release is undefined — never do it.
	Recycle bool
}

// Condition is one columnar predicate (logs): the rows whose value in column Column satisfy
// Match. Like [Matcher] it is operator-free — the language layer supplies the predicate.
//
// Two optional, serializable hints let a fetcher prune whole parts before scanning (the engine
// always re-checks Match per row, so a hint only ever skips work, never changes results):
//   - Tokens: the full-text tokens the column value must contain (lowered) — consulted against a
//     per-part token bloom for a `contains` condition (an empty Tokens ⇒ not full-text).
//   - Equal: an exact column=value equality — consulted against a per-part value bloom. For a
//     per-record attribute condition, Column is the attribute key and Equal carries key=value.
type Condition struct {
	Column string
	Match  func(value signal.Value) bool
	Tokens [][]byte
	Equal  *EqualMatcher
}

// Batch is one matching identity (a metric series, or a log stream) and its rows within the
// request window. For metrics the rows are (Timestamps, Values) samples. For logs the rows are
// the per-record Columns (the projected set); Timestamps still carries each record's time.
type Batch struct {
	ID         signal.SeriesID
	Series     signal.Series
	Timestamps []int64
	Values     []float64

	// ScaleFactors carries each sample's lossy-sampling weight (metrics only): a kept sample
	// with ScaleFactors[i] = N "represents" N original samples that budgeted sampling dropped
	// (DESIGN §8a). It is nil when no sampling occurred (every weight is 1), so the common path
	// is unaffected; when non-nil its length matches Values. The storage layer only *carries* the
	// weight — an embedder's aggregation multiplies it back into count/sum/rate to stay unbiased
	// (a gauge read ignores it). Use [Batch.ScaleFactor] to read it with the nil default.
	ScaleFactors []float64

	// Columns are the materialized per-record columns (logs); nil for metrics. Each column's
	// length matches Timestamps. The named layout is the engine's (e.g. severity, body, attrs).
	Columns []NamedColumn

	// release, when set by the producing fetcher, returns the batch's buffers to a pool. It takes the
	// batch so the producer can use one shared closure for every batch (no per-batch allocation),
	// reading the buffers off b. A nil hook (the default) means the GC reclaims, exactly as before.
	release func(*Batch)

	// recycleState is an opaque handle a producer attaches so its shared release closure can recover
	// the pool entry backing this batch when that entry is more than the batch's own slices — e.g.
	// the record engine's per-stream accumulator (a *recordCols whose columns back Columns). Holding
	// a pointer here costs no allocation (it fits the interface word). nil for producers (metrics)
	// whose buffers are recoverable directly from the batch.
	recycleState any
}

// SetRelease installs the buffer-reclamation hook a producing fetcher uses to pool a batch's
// backing slices. The producer passes one shared closure (it reads the buffers from the batch), so
// installing it costs no per-batch allocation. Only the fetcher that allocated the buffers sets it.
func (b *Batch) SetRelease(fn func(*Batch)) { b.release = fn }

// SetReleaseState attaches an opaque pool handle (see [Batch.recycleState]) that the release hook
// recovers via [Batch.ReleaseState]. A producer uses it when the pool entry backing the batch isn't
// the batch's own slices (the record engine's accumulator). Pass a pointer to avoid allocation.
func (b *Batch) SetReleaseState(s any) { b.recycleState = s }

// ReleaseState returns the handle set by [Batch.SetReleaseState] (nil if none). The producer's
// shared release closure type-asserts it back to recover the pooled entry.
func (b *Batch) ReleaseState() any { return b.recycleState }

// Release returns the batch's backing buffers to the producer's pool, if it set a hook. It is
// **opt-in**: a consumer done with a batch may call it to enable reuse; one that never does simply
// lets the GC reclaim (identical to the pre-hook behavior — no allocation is added to the
// non-releasing path). After Release the batch and its slices MUST NOT be read or retained. Release
// is idempotent and safe on a nil-hook batch.
//
// Pass-through decorators ([Merge] with one child, split, cluster fan-out) forward the hook
// unchanged. A decorator that *retains* a batch (the results cache, or a multi-child merge) deep-
// copies it, so the copy has no hook and the original is safe to release.
func (b *Batch) Release() {
	if b.release != nil {
		b.release(b)
		b.release = nil
		b.recycleState = nil
	}
}

// ScaleFactor returns sample i's lossy-sampling weight, defaulting to 1 when no sampling occurred
// (ScaleFactors is nil). It is the safe accessor for consumers that honor the weight.
func (b *Batch) ScaleFactor(i int) float64 {
	if b.ScaleFactors == nil {
		return 1
	}

	return b.ScaleFactors[i]
}

// NamedColumn is one materialized column of a log [Batch]: its name and exactly one populated
// typed slice (Int64/Float64/Bytes), matching the physical column kind. Row i of the batch is
// Int64[i] / Float64[i] / Bytes[i] for that column.
type NamedColumn struct {
	Name    string
	Int64   []int64
	Float64 []float64
	Bytes   [][]byte
}

// Column returns the named column of the batch and whether it is present.
func (b *Batch) Column(name string) (NamedColumn, bool) {
	for i := range b.Columns {
		if b.Columns[i].Name == name {
			return b.Columns[i], true
		}
	}

	return NamedColumn{}, false
}

// Iterator yields batches lazily; Next returns (nil, io.EOF) at the end.
type Iterator interface {
	Next(ctx context.Context) (*Batch, error)
	Close() error
}

// Fetcher resolves a [Request] to an [Iterator]. It is implemented by the head, each
// part, and (later) the cluster fan-out.
type Fetcher interface {
	Fetch(ctx context.Context, r Request) (Iterator, error)
}

// Counter is an optional [Fetcher] capability: it returns the number of series matching
// r.Matchers with at least one sample in [r.Start, r.End] without materializing samples or
// labels. It backs the PromQL `count(<selector>)` pushdown. A Fetcher that does not implement
// it simply opts out of the pushdown (the caller falls back to Fetch).
type Counter interface {
	Count(ctx context.Context, r Request) (int, error)
}

// GroupCounter is an optional [Fetcher] capability, the grouped variant of [Counter]: CountBy
// returns, for each distinct canonical-text value of the label among the series matching
// r.Matchers, how many of them have at least one sample in [r.Start, r.End] — without
// materializing samples or projecting labels into results. It backs the PromQL
// `count by (label)(<selector>)` pushdown (and, via the map's length,
// `count(count by (label)(...))` = distinct label values). Matched series without the label group
// under the "" key. A Fetcher that does not implement it opts out of the pushdown (the caller
// falls back to Fetch).
type GroupCounter interface {
	CountBy(ctx context.Context, r Request, label []byte) (map[string]int, error)
}

// GroupCounterOf is [CounterOf] for the grouped-count capability: it walks the wrapper chain (via
// [Unwraper]) starting at f and returns the first [GroupCounter], or nil if none.
func GroupCounterOf(f Fetcher) GroupCounter {
	for f != nil {
		if c, ok := f.(GroupCounter); ok {
			return c
		}

		u, ok := f.(Unwraper)
		if !ok {
			return nil
		}

		f = u.Unwrap()
	}

	return nil
}

// Unwraper is implemented by Fetcher decorators that wrap a single inner Fetcher (logging,
// caching, scoping, splitting). Multi-child fan-outs (merge, remote) are NOT Unwrapers — their
// count semantics are not a simple delegation, so [CounterOf] opts them out of the pushdown.
type Unwraper interface {
	Unwrap() Fetcher
}

// CounterOf walks the wrapper chain (via [Unwraper]) starting at f and returns the first
// [Counter] it finds, or nil if none. This lets a queryable reach the engine's Count through
// the decorators that wrap it (seed/scoped/cache/split) without each one having to re-declare
// Count, while multi-child fan-outs (which would need dedup-aware counting) correctly opt out.
func CounterOf(f Fetcher) Counter {
	for f != nil {
		if c, ok := f.(Counter); ok {
			return c
		}

		u, ok := f.(Unwraper)
		if !ok {
			return nil
		}

		f = u.Unwrap()
	}

	return nil
}

// SliceIterator is an [Iterator] over a fixed slice of batches — for simple fetchers and
// tests.
type SliceIterator struct {
	batches []*Batch
	i       int
}

// NewSliceIterator returns an iterator over batches.
func NewSliceIterator(batches []*Batch) *SliceIterator {
	return &SliceIterator{batches: batches}
}

// Next returns the next batch, or (nil, io.EOF) when exhausted.
func (it *SliceIterator) Next(context.Context) (*Batch, error) {
	if it.i >= len(it.batches) {
		return nil, io.EOF
	}

	b := it.batches[it.i]
	it.i++

	return b, nil
}

// Close releases the iterator (a no-op for a slice).
func (it *SliceIterator) Close() error { return nil }

// Drain reads an iterator to completion and returns all batches.
func Drain(ctx context.Context, it Iterator) ([]*Batch, error) {
	var out []*Batch

	for {
		b, err := it.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}

			return out, err
		}

		out = append(out, b)
	}
}
