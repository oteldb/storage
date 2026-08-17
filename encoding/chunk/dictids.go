package chunk

import (
	"fmt"
	"sync"
)

// dictCells is the (entries, ids) form of [cellSeq]: cell i is entries[ids[i]].
type dictCells struct {
	entries [][]byte
	ids     []int32
}

func (d dictCells) rows() int       { return len(d.ids) }
func (d dictCells) at(i int) []byte { return d.entries[d.ids[i]] }

var dictRemapScratchPool = sync.Pool{
	New: func() any { return &dictRemapScratch{} },
}

// dictRemapScratch maps a caller's entry index to the output dictionary id. Entries assigned by an
// earlier call are invalidated by bumping gen rather than by refilling stamp: the caller's entry
// table can be far larger than the row range being encoded (one column dictionary, many granules),
// so a per-call fill would cost more than the encode itself.
type dictRemapScratch struct {
	out   []int32
	stamp []uint32
	gen   uint32
}

func newDictRemapScratch(entries int) *dictRemapScratch {
	s := dictRemapScratchPool.Get().(*dictRemapScratch)
	s.arm(entries)

	return s
}

// arm re-arms the scratch for a call over entries indices, invalidating the previous call's
// assignments. Split from [newDictRemapScratch] so the generation wraparound is reachable from a
// test without 2³² encodes.
func (s *dictRemapScratch) arm(entries int) {
	if cap(s.stamp) < entries {
		s.out = make([]int32, entries)
		s.stamp = make([]uint32, entries)
		s.gen = 0
	}

	s.out = s.out[:entries]
	s.stamp = s.stamp[:entries]

	// The clear covers the whole backing array, not just the armed prefix: a later call with more
	// entries re-slices past it, and a stale stamp there matches whichever generation eventually
	// reaches its value — a hit on an entry the call never assigned, emitting a wrong dictionary id.
	if s.gen == ^uint32(0) {
		clear(s.stamp[:cap(s.stamp)])

		s.gen = 0
	}

	s.gen++
}

func (s *dictRemapScratch) get(entry int32) (int32, bool) {
	if s.stamp[entry] != s.gen {
		return 0, false
	}

	return s.out[entry], true
}

func (s *dictRemapScratch) set(entry, out int32) {
	s.stamp[entry] = s.gen
	s.out[entry] = out
}

func (s *dictRemapScratch) putBack() {
	s.out = s.out[:0]
	s.stamp = s.stamp[:0]
	dictRemapScratchPool.Put(s)
}

// EncodeBytesDict appends a dictionary-encoded []byte column to dst for a column already held in
// split form — a deduplicated entry table plus one entry index per row — and returns the extended
// slice. It is [EncodeBytes] without the per-row hashing: the entry indices are remapped to output
// dictionary ids through an array, so no value is hashed or compared.
//
// The output is byte-identical to EncodeBytes over the materialized rows (entries[ids[i]] for each
// i), including the flat fallback above 65536 distinct values, provided the caller honors the
// preconditions:
//
//   - entries must be distinct by value. EncodeBytes deduplicates by value; this deduplicates by
//     index, so two indices holding equal bytes produce two dictionary entries — a valid stream, but
//     not the same one.
//   - every ids[i] must be in [0, len(entries)). This one *is* checked, and panics naming the row,
//     the id and the table size — the two halves come from different places in the caller, so the
//     bare bounds error it replaces was the hard kind to debug. Distinctness is not checked: it
//     costs a hash per entry, which is most of what this function exists to avoid.
func EncodeBytesDict(dst []byte, entries [][]byte, ids []int32) []byte {
	return encodeBytesDict(dst, entries, ids)
}

// EncodeBytesDictRange is [EncodeBytesDict] over rows [lo,hi) of ids, for block-framed encoding
// where each granule is an independent stream with its own dictionary (see [EncodeBytesBlobRange]).
// It panics unless 0 <= lo <= hi <= len(ids).
func EncodeBytesDictRange(dst []byte, entries [][]byte, ids []int32, lo, hi int) []byte {
	if lo < 0 || hi < lo || hi > len(ids) {
		panic(fmt.Sprintf("chunk: dictionary id range [%d,%d) is invalid for %d rows", lo, hi, len(ids)))
	}

	return encodeBytesDict(dst, entries, ids[lo:hi])
}

func encodeBytesDict(dst []byte, entries [][]byte, ids []int32) []byte {
	n := len(ids)
	if n == 0 {
		return appendEmpty(dst)
	}

	remap := newDictRemapScratch(len(entries))
	defer remap.putBack()

	scratch := newDictEncodeScratch(n)
	defer scratch.putBack()

	dict := scratch.entries[:0]
	rowIDs := scratch.ids

	dictEntryBytes := 0
	flat := false

	for i, entry := range ids {
		// Explicit rather than left to the remap's own bounds check: the caller supplies the entry
		// table and the ids separately, and an "index out of range" raised deep inside the scratch
		// names neither the row nor the table it disagrees with. One unsigned compare per row — the
		// same one the bounds check already makes — for a panic that says what the caller got wrong.
		if uint32(entry) >= uint32(len(entries)) {
			panic(fmt.Sprintf(
				"chunk: dictionary id %d at row %d is out of range for %d entries", entry, i, len(entries)))
		}

		out, seen := remap.get(entry)
		if !seen {
			if len(dict) == maxDictEntries {
				flat = true

				break
			}

			v := entries[entry]
			out = int32(len(dict))

			remap.set(entry, out)

			dict = append(dict, v)
			dictEntryBytes += uvarintLen(uint64(len(v))) + len(v)
		}

		rowIDs[i] = uint16(out)
	}

	scratch.entries = dict

	if flat {
		return appendFlat(dst, dictCells{entries: entries, ids: ids})
	}

	return appendDictPayload(dst, dict, rowIDs, dictEntryBytes)
}
