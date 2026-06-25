// Package series is the series-identity index: it maps a content-addressed
// [signal.SeriesID] to its attribute set and back. Adding a series is idempotent (the
// id is the hash of its attributes), so the same series ingested twice — or replayed
// from the WAL — resolves to one entry. The stored attributes let a query reconstruct
// labels from an id.
package series

import (
	"github.com/oteldb/storage/signal"
)

// Index maps [signal.SeriesID] → [signal.Attributes]. The zero value is not usable;
// create one with [New]. Not safe for concurrent use; callers own synchronization.
type Index struct {
	byID map[signal.SeriesID]signal.Attributes
	buf  []byte // reused hash pre-image buffer (zero-alloc Add)
}

// New returns an empty [Index].
func New() *Index {
	return &Index{byID: make(map[signal.SeriesID]signal.Attributes)}
}

// Add interns a series and returns its [signal.SeriesID]. It is idempotent: re-adding an
// equal attribute set returns the same id without storing a second copy. The attributes
// must be sorted (use [signal.NewAttributes]); a deep copy is retained, so the caller may
// reuse its buffers.
func (ix *Index) Add(attrs signal.Attributes) signal.SeriesID {
	ix.buf = attrs.AppendHashInput(ix.buf[:0])
	id := signal.HashBytes(ix.buf)

	if _, ok := ix.byID[id]; !ok {
		ix.byID[id] = attrs.Clone()
	}

	return id
}

// Get returns the attribute set for id and whether it is known.
func (ix *Index) Get(id signal.SeriesID) (signal.Attributes, bool) {
	a, ok := ix.byID[id]

	return a, ok
}

// Has reports whether id is known.
func (ix *Index) Has(id signal.SeriesID) bool {
	_, ok := ix.byID[id]

	return ok
}

// Len returns the number of distinct series.
func (ix *Index) Len() int { return len(ix.byID) }

// ForEach calls fn for each (id, attributes) pair. Iteration order is unspecified.
func (ix *Index) ForEach(fn func(id signal.SeriesID, attrs signal.Attributes)) {
	for id, attrs := range ix.byID {
		fn(id, attrs)
	}
}
