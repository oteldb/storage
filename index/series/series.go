// Package series is the series-identity index: it maps a content-addressed
// [signal.SeriesID] to its full identity ([signal.Series] — Resource + Scope + data-point
// attributes) and back. Adding a series is idempotent (the id is the hash of the
// identity), so the same series ingested twice — or replayed from the WAL — resolves to
// one entry. The stored identity lets a query reconstruct a series' labels from an id.
package series

import (
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

// Index maps [signal.SeriesID] → [signal.Series]. The zero value is not usable; create
// one with [New]. Not safe for concurrent use; callers own synchronization.
type Index struct {
	// byID maps an id to its position in entries. Holding a position rather than the identity keeps
	// the map small (a wide value inflates every slot, live or empty) and makes the identities
	// themselves live in one append-only log — see entries.
	byID map[signal.SeriesID]int32
	// entries is the append-only registration log, in insertion order: an entry is never moved,
	// mutated or removed, so a slice of it is a stable, immutable view of the identities registered
	// up to that point ([Index.Snapshot]) — which is what lets an identity rebuild run without the
	// caller's lock while registration continues.
	entries []Entry
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
	// bytes accumulates the structures this index owns outside the symbol table — the byID entries,
	// each identity's attribute slice, and the resource/scope dedup maps — on insert, so
	// [Index.SizeBytes] is O(1). The index is identity state that outlives every flush, so its size
	// is metered rather than walked.
	bytes int64
}

// Entry is one registered identity: its content-addressed id and the identity itself.
type Entry struct {
	ID     signal.SeriesID
	Series signal.Series
}

// New returns an empty [Index].
func New() *Index {
	return &Index{
		byID:  make(map[signal.SeriesID]int32),
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
		stored := signal.Series{
			Resource:   ix.internResource(s.Resource),
			Scope:      ix.internScope(s.Scope),
			Attributes: s.Attributes.Intern(ix.sym.Bytes),
		}

		before := memsize.Slice(ix.entries)
		ix.byID[id] = int32(len(ix.entries))
		ix.entries = append(ix.entries, Entry{ID: id, Series: stored})
		// The resource/scope attribute sets are shared (counted once, where they are interned); only
		// the point attributes are this identity's own.
		ix.bytes += seriesEntryBytes + attrBytes(stored.Attributes) + memsize.Slice(ix.entries) - before
	}

	return id
}

// Snapshot returns the identities registered so far, in registration order. The result stays valid
// and immutable **without the caller's lock**: entries are only ever appended, so a later [Index.Add]
// either writes past the returned slice's end or moves to a new array, and never touches what this
// one refers to. A caller rebuilding the index off-lock snapshots here, and picks up whatever was
// registered meanwhile as the tail past its length. It must not mutate the entries.
func (ix *Index) Snapshot() []Entry { return ix.entries }

// Get returns the identity for id and whether it is known.
func (ix *Index) Get(id signal.SeriesID) (signal.Series, bool) {
	pos, ok := ix.byID[id]
	if !ok {
		return signal.Series{}, false
	}

	return ix.entries[pos].Series, true
}

// Has reports whether id is known.
func (ix *Index) Has(id signal.SeriesID) bool {
	_, ok := ix.byID[id]

	return ok
}

// Len returns the number of distinct series.
func (ix *Index) Len() int { return len(ix.entries) }

// Sizes of the index's owned structures. The identity payloads themselves (label bytes) live in
// the symbol table and are counted there.
var (
	seriesEntryBytes = memsize.MapEntry[signal.SeriesID, int32]()
	resEntryBytes    = memsize.MapEntry[string, signal.Resource]()
	scopeEntryBytes  = memsize.MapEntry[string, signal.Scope]()
)

// attrBytes returns the bytes of an attribute set's backing array. A nested (slice- or map-valued)
// attribute is counted by its header only — identity attributes are flat in practice.
func attrBytes(a signal.Attributes) int64 { return memsize.Slice(a) }

// SizeBytes returns the index's resident footprint: the interned symbol table, the id→identity
// entries, the deduplicated resource/scope sets, and the reusable scratch buffers. It is the
// index's share of the engine's identity state, which no sample/record byte counter sees.
func (ix *Index) SizeBytes() int64 {
	return ix.bytes + ix.sym.SizeBytes() + memsize.Slice(ix.buf) + memsize.Slice(ix.kbuf)
}

// ForEach calls fn for each (id, identity) pair, in registration order.
func (ix *Index) ForEach(fn func(id signal.SeriesID, s signal.Series)) {
	for i := range ix.entries {
		fn(ix.entries[i].ID, ix.entries[i].Series)
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
	ix.bytes += resEntryBytes + int64(len(ix.kbuf)) + attrBytes(interned.Attributes)

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
	ix.bytes += scopeEntryBytes + int64(len(ix.kbuf)) + attrBytes(interned.Attributes)

	return interned
}
