// Package series is the series-identity index: it maps a content-addressed
// [signal.SeriesID] to its full identity ([signal.Series] — Resource + Scope + data-point
// attributes) and back. Adding a series is idempotent (the id is the hash of the
// identity), so the same series ingested twice — or replayed from the WAL — resolves to
// one entry. The stored identity lets a query reconstruct a series' labels from an id.
package series

import (
	"github.com/oteldb/storage/signal"
)

// Index maps [signal.SeriesID] → [signal.Series]. The zero value is not usable; create
// one with [New]. Not safe for concurrent use; callers own synchronization.
type Index struct {
	byID map[signal.SeriesID]signal.Series
	buf  []byte // reused hash pre-image buffer (zero-alloc Add)
}

// New returns an empty [Index].
func New() *Index {
	return &Index{byID: make(map[signal.SeriesID]signal.Series)}
}

// Add interns a series identity and returns its [signal.SeriesID]. It is idempotent:
// re-adding an equal identity returns the same id without storing a second copy. A deep
// copy of s is retained, so the caller may reuse its buffers.
func (ix *Index) Add(s signal.Series) signal.SeriesID {
	ix.buf = s.AppendHashInput(ix.buf[:0])
	id := signal.HashBytes(ix.buf)

	if _, ok := ix.byID[id]; !ok {
		ix.byID[id] = s.Clone()
	}

	return id
}

// Get returns the identity for id and whether it is known.
func (ix *Index) Get(id signal.SeriesID) (signal.Series, bool) {
	s, ok := ix.byID[id]

	return s, ok
}

// Has reports whether id is known.
func (ix *Index) Has(id signal.SeriesID) bool {
	_, ok := ix.byID[id]

	return ok
}

// Len returns the number of distinct series.
func (ix *Index) Len() int { return len(ix.byID) }

// ForEach calls fn for each (id, identity) pair. Iteration order is unspecified.
func (ix *Index) ForEach(fn func(id signal.SeriesID, s signal.Series)) {
	for id := range ix.byID {
		fn(id, ix.byID[id])
	}
}
