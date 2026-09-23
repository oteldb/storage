package recordengine

import (
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/pool"
)

// mergeSplitDict enables the merge's split (dictionary + ids) carry of byte columns. It is a package
// test seam: flipping it off forces every column onto the flat path, which must produce
// byte-identical parts (TestMergeSplitDictMatchesFlat). Never changed outside tests.
var mergeSplitDict = true

// mergeSplitObserver, when non-nil, receives each merge's per-byte-column split decision. Test seam
// only (see export_test.go); nil in every non-test build.
var mergeSplitObserver func(split []bool)

// mergeDict is the union of one byte column's dictionary entries across a merge's sources, built as
// the sources are read: a granule's entries are looked up, and added when new, once per granule rather
// than once per row. entries is distinct by value — the precondition [block.Column]'s split form
// imposes — and owns its bytes, since a streamed granule's entries alias a decode buffer that the
// next granule reuses.
//
// Its order is first-seen, not any source's. That is free to differ from a whole decode's, because
// the writer renumbers per granule and emits the same object for any entry order.
type mergeDict struct {
	entries [][]byte
	// index is taken from its pool on the first entry, not when the merge arms the column: columns
	// open one after another and each hands its index back when its sources have all opened, so a
	// merge reuses one grown index across columns rather than growing one per column.
	index   *pool.ByteIntMap
	settled bool
	slab    []byte
}

// mergeDictSlabBytes is the arena chunk union entries are copied into, so a union of small values
// costs one allocation per chunk rather than one per entry.
const mergeDictSlabBytes = 64 << 10

func newMergeDict() *mergeDict { return &mergeDict{} }

// id returns v's union id, adding a copy of v when the union does not hold it yet.
func (m *mergeDict) id(v []byte) int32 { return m.add(v, false) }

// add returns v's union id, adding v when the union does not hold it yet: as is when stable says v
// outlives the merge's use of the union, as a copy otherwise.
func (m *mergeDict) add(v []byte, stable bool) int32 {
	if m.index == nil {
		if m.settled {
			panic("recordengine: entry added to a settled merge union")
		}

		m.index = pool.NewByteIntMap()
	}

	if id, ok := m.index.Get(v); ok {
		return int32(id)
	}

	if !stable {
		v = m.own(v)
	}

	id := len(m.entries)
	m.entries = append(m.entries, v)
	m.index.Put(v, id)

	return int32(id)
}

// remap appends the union id of every entry of src to dst[:0]; stable is as for [mergeDict.add].
func (m *mergeDict) remap(dst []int32, src [][]byte, stable bool) []int32 {
	dst = dst[:0]
	for _, v := range src {
		dst = append(dst, m.add(v, stable))
	}

	return dst
}

func (m *mergeDict) own(v []byte) []byte {
	if len(v) > mergeDictSlabBytes/8 {
		return append([]byte(nil), v...)
	}

	if cap(m.slab)-len(m.slab) < len(v) {
		m.slab = make([]byte, 0, mergeDictSlabBytes)
	}

	m.slab = append(m.slab, v...)

	return m.slab[len(m.slab)-len(v) : len(m.slab) : len(m.slab)]
}

// release returns the lookup index to its pool; the union takes no entry after it. The entries stay
// valid for whoever still holds them.
func (m *mergeDict) release() {
	m.settled = true

	if m.index != nil {
		m.index.PutBack()
		m.index = nil
	}
}

// mergeUnionEntriesPerSource bounds a union at this many entries per source part: the most a column
// can carry while every source holds a real dictionary of its own. A package var only so a test can
// reach the bound without writing parts of that many distinct values.
var mergeUnionEntriesPerSource = 1 << 16

// mergeCarry is one merge's byte-column carry: per column, the union dictionary its rows move as ids
// into, or nil where the column moves as copied cells, together with the two accumulators armed with
// them. The per-column choice starts from the codec and can only fall back, never return: see
// [mergeCarry.flatten].
type mergeCarry struct {
	dicts []*mergeDict
	// lazy counts per column the sources that resolve entries into its union as they read it.
	lazy       []int
	acc, buf   *recordCols
	maxEntries int
	// flatBlob is the blob the output buffer reserves per column should it fall back, for a column
	// that falls back before its average cell size is known.
	flatBlob []int
}

// newMergeCarry arms acc and buf for a merge of sources parts. A column takes the split carry when
// the schema's codec accepts the split form; a raw column has no dictionary for the writer to take.
func newMergeCarry(schema *Schema, sources int, acc, buf *recordCols) *mergeCarry {
	m := &mergeCarry{
		dicts:      make([]*mergeDict, schema.numBytes()),
		lazy:       make([]int, schema.numBytes()),
		acc:        acc,
		buf:        buf,
		maxEntries: max(sources, 1) * mergeUnionEntriesPerSource,
	}

	split := make([]bool, len(m.dicts))

	for k := range m.dicts {
		codec := schema.byteColumn(k).Codec
		if mergeSplitDict && (codec == chunk.CodecNone || codec == chunk.CodecDict) {
			m.dicts[k] = newMergeDict()
			split[k] = true
		}
	}

	observeMergeSplit(split)

	acc.armSplit(m.dicts)
	buf.armSplit(m.dicts)

	return m
}

// settle drops column k's lookup index once no source will add to its union any more: when every
// source was read whole, and so resolved into it as it opened. Only a source read granule by granule
// meets new entries during the stream sweep.
func (m *mergeCarry) settle(k int) {
	if d := m.dicts[k]; d != nil && m.lazy[k] == 0 {
		d.release()
	}
}

// grew checks column k's union after it took new entries, falling the column back to the flat
// carry once the union outgrows the bound.
func (m *mergeCarry) grew(k int) {
	if d := m.dicts[k]; d != nil && len(d.entries) > m.maxEntries {
		m.flatten(k)
	}
}

// flatten moves byte column k onto the flat carry for the rest of the merge, expanding the ids both
// accumulators already hold. A column falls back when its union grows past the bound or a source
// hands it a granule with no dictionary at all — the >65536-distinct fallback — whose rows would
// otherwise each add an entry. The output is the same object either way; only what the merge holds
// to produce it changes.
func (m *mergeCarry) flatten(k int) {
	d := m.dicts[k]
	if d == nil {
		return
	}

	var reserve int
	if k < len(m.flatBlob) {
		reserve = m.flatBlob[k]
	}

	m.acc.unsplit(k, 0)
	m.buf.unsplit(k, reserve)
	d.release()
	m.dicts[k] = nil
}

func (m *mergeCarry) release() {
	for _, d := range m.dicts {
		if d != nil {
			d.release()
		}
	}
}

// splitCol carries one byte column of a merge accumulator as ids into a shared [mergeDict] rather
// than as a blob of copied cells.
//
// bytes is Σ len(entries[ids[i]]) maintained on append. Size accounting must stay *expanded*: it is
// what seals an output part (`buf.byteSize() >= capBytes`) and what bounds the merge's working set,
// and both are denominated in decoded bytes. Reporting the id array instead would inflate an output
// part by the column's compression ratio and remove the bound the cap exists for. Maintaining it
// incrementally keeps `byteSize`, which the merge loop calls once per stream, O(1) per row.
type splitCol struct {
	dict    *mergeDict
	ids     []int32
	scratch []int32 // ts-sort permutation target, swapped with ids (see [recordCols.sortByTs])
	bytes   int64
}

func (s *splitCol) rows() int { return len(s.ids) }

func (s *splitCol) at(i int) []byte { return s.dict.entries[s.ids[i]] }

// append records one row holding union entry id.
func (s *splitCol) append(id int32) {
	s.ids = append(s.ids, id)
	s.bytes += int64(len(s.dict.entries[id]))
}

// appendIDs bulk-appends a contiguous id range already expressed in the same union.
func (s *splitCol) appendIDs(ids []int32) {
	s.ids = append(s.ids, ids...)

	entries := s.dict.entries
	for _, id := range ids {
		s.bytes += int64(len(entries[id]))
	}
}

// ensure re-arms the column for a fresh accumulation of up to n rows, keeping the backing array.
func (s *splitCol) ensure(n int) {
	if cap(s.ids) >= n {
		s.ids = s.ids[:0]
	} else {
		// At least doubling, like [byteCol.ensureBytes]: a shape that creeps up by a few rows per
		// round must not reallocate every round.
		s.ids = make([]int32, 0, max(n, 2*cap(s.ids)))
	}

	s.bytes = 0
}

// permute reorders the ids by idx into the scratch array and swaps the two, mirroring what the byte
// columns do. The expanded total is permutation-invariant.
func (s *splitCol) permute(idx []int) {
	dst := s.scratch
	if cap(dst) < len(idx) {
		dst = make([]int32, 0, len(idx))
	}

	dst = dst[:0]
	for _, j := range idx {
		dst = append(dst, s.ids[j])
	}

	s.ids, s.scratch = dst, s.ids
}

// keep retains only rows [lo, hi), recomputing the expanded total over the survivors.
func (s *splitCol) keep(lo, hi int) {
	s.ids = s.ids[lo:hi]

	s.bytes = 0
	for _, id := range s.ids {
		s.bytes += int64(len(s.dict.entries[id]))
	}
}

func observeMergeSplit(split []bool) {
	if mergeSplitObserver != nil {
		mergeSplitObserver(split)
	}
}
