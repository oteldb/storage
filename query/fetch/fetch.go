// Package fetch is the storage seam: the contract every query language compiles to and
// every data source (head, parts, cluster fan-out) implements. A [Request] of label
// matchers + a time window resolves to an [Iterator] of lazily-produced [Batch]es.
//
// This is the metrics shape of the contract — one batch per matching series, carrying its
// sample columns. The columnar/projection/second-pass and nested-reconstruction aspects
// (for logs/traces) extend it in later milestones; the seam itself stays the same.
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
}

// Request selects series for a tenant within an inclusive time window, filtered by all
// matchers (their intersection).
type Request struct {
	Tenant     signal.TenantID
	Start, End int64 // unix nanos, inclusive
	Matchers   []Matcher
}

// Batch is one matching series and its samples within the request window, time-ordered.
type Batch struct {
	ID         signal.SeriesID
	Series     signal.Series
	Timestamps []int64
	Values     []float64
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
