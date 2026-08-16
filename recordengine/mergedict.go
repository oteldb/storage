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

// mergeDict is the union of the selected source parts' byte-column dictionaries for one column,
// built once per merge. entries is distinct by value — the precondition [block.Column]'s split form
// imposes — and remap[p][id] is the union id of source part p's entry id.
//
// Building it costs one hash probe per distinct entry per source part, never one per row: a column
// of tens of millions of rows over a few thousand distinct values pays a few thousand probes, where
// the flat path copies and re-hashes every cell twice.
//
// The union may exceed 65536 entries with no fallback needed. The writer renumbers per granule, so
// the emitted id width is bounded by a granule's distinct count, not the union's.
type mergeDict struct {
	entries [][]byte
	remap   [][]int32
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

// buildMergeDicts builds the per-byte-column union dictionary for a merge, or nil where the column
// must stay on the flat path. The decision is per column and made once, before any row is appended:
// a column takes the split path only when *every* selected source decoded to a real dictionary and
// the schema's codec accepts the split form. A mixed set is therefore normal — one column carries
// ids while its neighbor carries a blob, in the same merge and the same accumulator.
//
// A single flat source (the >65536-distinct dictionary fallback, or a [chunk.CodecBytesRaw] column
// such as trace_id, both of which decode with IDWidth 0) has no entry table to union, so its column
// keeps the flat path for the whole merge rather than being expanded into a synthetic one.
func buildMergeDicts(schema *Schema, decoded []*decodedPart) []*mergeDict {
	if !mergeSplitDict || len(decoded) == 0 {
		observeMergeSplit(make([]bool, schema.numBytes()))

		return nil
	}

	var (
		out  = make([]*mergeDict, schema.numBytes())
		some bool
	)

	for k := range out {
		out[k] = buildMergeDict(schema.byteColumn(k), decoded, k)
		some = some || out[k] != nil
	}

	if mergeSplitObserver != nil {
		split := make([]bool, len(out))
		for k := range out {
			split[k] = out[k] != nil
		}

		observeMergeSplit(split)
	}

	if !some {
		return nil
	}

	return out
}

func observeMergeSplit(split []bool) {
	if mergeSplitObserver != nil {
		mergeSplitObserver(split)
	}
}

func buildMergeDict(col Column, decoded []*decodedPart, k int) *mergeDict {
	// The split form is dictionary-only at the block seam; a raw column would be rejected there.
	if col.Codec != chunk.CodecNone && col.Codec != chunk.CodecDict {
		return nil
	}

	for _, d := range decoded {
		if d.bytes[k].dict == nil {
			return nil
		}
	}

	m := &mergeDict{remap: make([][]int32, len(decoded))}

	idx := pool.NewByteIntMap()
	defer idx.PutBack()

	for p, d := range decoded {
		src := d.bytes[k].dict.Entries
		remap := make([]int32, len(src))

		for i, e := range src {
			id, existed := idx.PutOrGet(e, len(m.entries))
			if !existed {
				m.entries = append(m.entries, e)
			}

			remap[i] = int32(id)
		}

		m.remap[p] = remap
	}

	return m
}
