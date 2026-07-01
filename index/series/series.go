// Package series is the series-identity index: it maps a content-addressed
// [signal.SeriesID] to its full identity ([signal.Series] — Resource + Scope + data-point
// attributes) and back. Adding a series is idempotent (the id is the hash of the
// identity), so the same series ingested twice — or replayed from the WAL — resolves to
// one entry. The stored identity lets a query reconstruct a series' labels from an id.
package series

import (
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/signal"
)

// Index maps [signal.SeriesID] → [signal.Series]. The zero value is not usable; create
// one with [New]. Not safe for concurrent use; callers own synchronization.
type Index struct {
	byID map[signal.SeriesID]signal.Series
	// sym interns every key/value byte string across all stored identities, so the index holds one
	// owned copy per distinct label/attribute string (referenced by every series sharing it) instead
	// of a private clone per series. Under a steady metrics workload the same resource/scope and
	// many label values repeat across series, so this collapses the dominant identity-storage cost.
	sym *symbols.Table
	// res / scope dedup whole Resource / Scope objects by content: node_exporter-shaped ingest has a
	// handful of resources (one per scrape target) and one scope shared across ~all series, so byte
	// interning alone still leaves a private []KeyValue structure per series. Interning the *set*
	// makes every series carrying an identical resource/scope share one owned copy — collapsing the
	// per-series attribute-set structure that byte interning cannot. Point attributes are near-unique
	// per series, so they are only byte-interned (a per-set cache there would store keys it cannot
	// dedup).
	res   map[string]signal.Resource
	scope map[string]signal.Scope
	buf   []byte // reused series hash pre-image buffer (zero-alloc Add)
	kbuf  []byte // reused resource/scope canonical-bytes buffer for set interning
}

// New returns an empty [Index].
func New() *Index {
	return &Index{
		byID:  make(map[signal.SeriesID]signal.Series),
		sym:   symbols.New(),
		res:   make(map[string]signal.Resource),
		scope: make(map[string]signal.Scope),
	}
}

// Add interns a series identity and returns its [signal.SeriesID]. It is idempotent:
// re-adding an equal identity returns the same id without storing a second copy. The identity's
// byte payloads are interned (not cloned) and its resource/scope sets are deduplicated, so the
// caller may reuse its buffers and each distinct string — and each distinct resource/scope — is
// stored once across the whole index.
func (ix *Index) Add(s signal.Series) signal.SeriesID {
	ix.buf = s.AppendHashInput(ix.buf[:0])
	id := signal.HashBytes(ix.buf)

	if _, ok := ix.byID[id]; !ok {
		ix.byID[id] = signal.Series{
			Resource:   ix.internResource(s.Resource),
			Scope:      ix.internScope(s.Scope),
			Attributes: s.Attributes.Intern(ix.sym.Bytes),
		}
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

// internResource returns the shared, byte-interned Resource equal to r, deduplicating identical
// resources by their canonical content so series sharing a resource hold one owned copy.
func (ix *Index) internResource(r signal.Resource) signal.Resource {
	ix.kbuf = r.AppendHashInput(ix.kbuf[:0])
	if got, ok := ix.res[string(ix.kbuf)]; ok { // alloc-free lookup
		return got
	}

	interned := r.Intern(ix.sym.Bytes)
	ix.res[string(ix.kbuf)] = interned // allocates the key once per distinct resource

	return interned
}

// internScope is the Scope analog of internResource.
func (ix *Index) internScope(s signal.Scope) signal.Scope {
	ix.kbuf = s.AppendHashInput(ix.kbuf[:0])
	if got, ok := ix.scope[string(ix.kbuf)]; ok {
		return got
	}

	interned := s.Intern(ix.sym.Bytes)
	ix.scope[string(ix.kbuf)] = interned

	return interned
}
