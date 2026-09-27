package profile

import (
	"encoding/binary"
	"maps"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// Tables is a decoded, read-only set of symbol tables: one part's sidecars, a peer's reply, or a
// snapshot of the live accumulator. A [Resolver] reads a stack of them. Safe for concurrent reads.
type Tables struct {
	t      symTables
	bodies [5][]byte // the arrays the entries slice; retained whole, so charged by capacity
	size   int64
}

// entryOverhead approximates the resident cost of one map entry beyond its bytes: the 16-byte key,
// the slice header, and the swiss table's control byte and load-factor slack.
const entryOverhead = 48

// DecodeTables decodes named tables, as a part's sidecars or [SymbolStore.Encode] hold them. An
// absent table is empty. The result does not alias tables.
func DecodeTables(tables map[string][]byte) (*Tables, error) {
	out := &Tables{t: newSymTables()}

	// Decompression reserves its bound plus a fixed slack of about 128 KiB, and the entry views would
	// pin that whole array: a small table would hold several times its size. It decompresses into
	// reused scratch instead, and only an exact-length copy is retained.
	scratch, _ := bodyScratch.Get().(*[]byte)
	defer bodyScratch.Put(scratch)

	for i, name := range tableNames {
		data, ok := tables[name]
		if !ok {
			continue
		}

		body, err := tableBodyTo((*scratch)[:0], data)
		if err != nil {
			return nil, errors.Wrapf(err, "decode table %q", name)
		}

		if binary.BigEndian.Uint32(data[4:]) != symVersionRaw && cap(body) > cap(*scratch) {
			*scratch = body[:0]
		}

		body = exactCopy(body)

		if out.t.t[i], err = decodeEntriesView(body); err != nil {
			return nil, errors.Wrapf(err, "decode table %q", name)
		}

		out.bodies[i] = body
		out.size += int64(cap(body)) + int64(len(out.t.t[i]))*entryOverhead
	}

	return out, nil
}

var bodyScratch = sync.Pool{New: func() any { return new([]byte) }}

func exactCopy(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)

	return out
}

// Size is the approximate resident size of t in bytes.
func (t *Tables) Size() int64 { return t.size }

// Tables returns a snapshot of the accumulator that stays valid once the engine's lock is released.
// Absorbed entries are never mutated, so only the maps are copied.
func (s *SymbolStore) Tables() *Tables {
	out := &Tables{}
	for i := range s.acc.t {
		out.t.t[i] = maps.Clone(s.acc.t[i])
		out.size += int64(len(out.t.t[i])) * entryOverhead
	}

	return out
}

// EncodeTables returns the union of layers as named tables in the in-memory form, for a peer to
// decode with [DecodeTables].
func EncodeTables(layers []*Tables) map[string][]byte {
	out := make(map[string][]byte, len(tableNames))

	for i, name := range tableNames {
		merged := unionTable(layers, i)
		out[name] = encodeTable(merged, memoryCompressor)
	}

	return out
}

func unionTable(layers []*Tables, i int) map[signal.SeriesID][]byte {
	switch len(layers) {
	case 0:
		return nil
	case 1:
		return layers[0].t.t[i]
	}

	size := 0
	for _, l := range layers {
		size = max(size, len(l.t.t[i]))
	}

	merged := make(map[signal.SeriesID][]byte, size)

	for _, l := range layers {
		for id, entry := range l.t.t[i] {
			if _, ok := merged[id]; !ok {
				merged[id] = entry
			}
		}
	}

	return merged
}

// minEntryBytes is the smallest encoded entry: a 16-byte id and a one-byte length.
const minEntryBytes = 17

// decodeEntriesView is [decodeEntries] into a new map, without the per-entry copy: each entry
// slices p, so the caller must own p and never modify it.
func decodeEntriesView(p []byte) (map[signal.SeriesID][]byte, error) {
	count, n := binary.Uvarint(p)
	if n <= 0 || count > uint64(len(p)/minEntryBytes) {
		return nil, errCorrupt("count")
	}

	p = p[n:]
	dst := make(map[signal.SeriesID][]byte, count)

	for range count {
		if len(p) < 16 {
			return nil, errCorrupt("id")
		}

		id := signal.SeriesID{Hi: binary.BigEndian.Uint64(p), Lo: binary.BigEndian.Uint64(p[8:])}
		p = p[16:]

		ln, n := binary.Uvarint(p)
		if n <= 0 || ln > uint64(len(p)-n) {
			return nil, errCorrupt("entry len")
		}

		p = p[n:]

		if _, ok := dst[id]; !ok {
			dst[id] = p[:ln:ln]
		}

		p = p[ln:]
	}

	return dst, nil
}
