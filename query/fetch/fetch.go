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
}

// Condition is one columnar predicate (logs): the rows whose value in column Column satisfy
// Match. Like [Matcher] it is operator-free — the language layer supplies the predicate. Tokens,
// when set, are the full-text tokens the column's value must contain (lowered), used to consult a
// per-part token bloom for pushdown; an empty Tokens means the condition is not full-text.
type Condition struct {
	Column string
	Match  func(value signal.Value) bool
	Tokens [][]byte
}

// Batch is one matching identity (a metric series, or a log stream) and its rows within the
// request window. For metrics the rows are (Timestamps, Values) samples. For logs the rows are
// the per-record Columns (the projected set); Timestamps still carries each record's time.
type Batch struct {
	ID         signal.SeriesID
	Series     signal.Series
	Timestamps []int64
	Values     []float64

	// Columns are the materialized per-record columns (logs); nil for metrics. Each column's
	// length matches Timestamps. The named layout is the engine's (e.g. severity, body, attrs).
	Columns []NamedColumn
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
