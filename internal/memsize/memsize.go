// Package memsize estimates the resident footprint of in-memory index structures, so identity
// state — the symbol table, the series index and the postings lists, none of which the buffered
// sample/record byte counters see — can be metered and reported to an operator.
//
// The estimates are structural: they count the bytes a structure owns (backing arrays, map slots,
// interned payloads), not the exact allocator footprint, so they ignore size-class rounding and
// allocation headers. They are accurate enough to size a cap against and to make growth visible;
// they are not a heap profiler.
//
// Every helper counts the *fixed* part of a type only — the bytes a value that a pointer, slice
// header or interface field refers to are counted where they are owned, so a shared payload
// (an interned label) is charged once rather than to each of its references. It is the single
// place in the module that needs unsafe.Sizeof; callers state their intent with types instead.
package memsize

import "unsafe"

// mapLoad models a Go map's occupancy between growths. A swiss map stores its entries in groups of
// eight slots with a one-byte control word per slot, and grows at 7/8 full — so a map holds
// between ~44 % and ~87 % of its slots live, and an entry costs its key and value plus the empty
// slots' share. The midpoint (~1.5× the raw key+value size) is the estimate; a map that has just
// doubled is over-counted and one about to grow under-counted, both by well under the error a
// per-entry model has anyway.
const (
	mapLoadNum = 3
	mapLoadDen = 2
)

// MapBase is the runtime map header a map variable points at (a Go map value is one pointer word;
// the header holding the seed, the directory and the counters is a separate allocation). It is
// paid once per map regardless of contents, which only matters where maps are per-key rather than
// per-structure — the postings index keeps one value map per label name.
const MapBase = 48

// Of returns the fixed size of T: what a variable of that type occupies, excluding anything it
// points at.
func Of[T any]() int64 {
	var v T

	return int64(unsafe.Sizeof(v))
}

// Slice returns the bytes of s's backing array — its **capacity**, not its length, since that is
// what stays resident (an in-place dedup shrinks the length and frees nothing).
func Slice[T any](s []T) int64 { return int64(cap(s)) * Of[T]() }

// MapEntry returns the estimated bytes one entry of a map[K]V occupies: its key, its value, and
// its share of the slot bookkeeping and the map's occupancy slack. The map header itself is
// [MapBase], counted separately because it is per-map rather than per-entry.
func MapEntry[K comparable, V any]() int64 {
	return (Of[K]() + Of[V]() + 1) * mapLoadNum / mapLoadDen
}

// Map returns the estimated bytes of a whole map: its header plus its current entries.
func Map[K comparable, V any](m map[K]V) int64 {
	return MapBase + int64(len(m))*MapEntry[K, V]()
}
