package profile

import (
	"encoding/binary"
	"iter"

	"github.com/oteldb/storage/signal"
)

// RefColumn names the column whose cells are the stack ids records reference.
func (s *SymbolStore) RefColumn() string { return ColStackID }

// Retain keeps only the accumulated entries reachable from the stack ids in refs. An id missing from
// the accumulator, or an entry too malformed to follow, keeps nothing past it.
func (s *SymbolStore) Retain(refs iter.Seq[[]byte]) {
	kept := newSymTables()

	for ref := range refs {
		if id, ok := idFromBytes(ref); ok {
			s.keepStack(&kept, id)
		}
	}

	s.acc = kept
}

// keep copies id's entry of table i into dst and returns it, or reports false when it was kept
// already or is absent.
func (s *SymbolStore) keep(dst *symTables, i int, id signal.SeriesID) ([]byte, bool) {
	if _, done := dst.t[i][id]; done {
		return nil, false
	}

	entry, ok := s.acc.t[i][id]
	if ok {
		dst.t[i][id] = entry
	}

	return entry, ok
}

func (s *SymbolStore) keepStack(dst *symTables, id signal.SeriesID) {
	entry, ok := s.keep(dst, tableStacks, id)
	if !ok {
		return
	}

	locs, _ := readIDList(entry)
	for _, loc := range locs {
		s.keepLocation(dst, loc)
	}
}

// keepLocation follows a location's [mapping id][uvarint address][uvarint lines] then per line
// [function id][varint line][varint column].
func (s *SymbolStore) keepLocation(dst *symTables, id signal.SeriesID) {
	p, ok := s.keep(dst, tableLocations, id)
	if !ok {
		return
	}

	mapping, p, ok := readID(p)
	if !ok {
		return
	}

	s.keepMapping(dst, mapping)

	p, ok = skipUvarint(p)
	if !ok {
		return
	}

	lines, n := binary.Uvarint(p)
	if n <= 0 {
		return
	}

	p = p[n:]

	for range lines {
		var fn signal.SeriesID
		if fn, p, ok = readID(p); !ok {
			return
		}

		s.keepFunction(dst, fn)

		for range 2 {
			if _, n := binary.Varint(p); n > 0 {
				p = p[n:]
			} else {
				return
			}
		}
	}
}

// keepMapping follows a mapping's [uvarint start][uvarint limit][uvarint offset][filename id].
func (s *SymbolStore) keepMapping(dst *symTables, id signal.SeriesID) {
	p, ok := s.keep(dst, tableMappings, id)
	if !ok {
		return
	}

	for range 3 {
		if p, ok = skipUvarint(p); !ok {
			return
		}
	}

	if file, _, ok := readID(p); ok {
		s.keep(dst, tableStrings, file)
	}
}

// keepFunction follows a function's [name id][system name id][filename id].
func (s *SymbolStore) keepFunction(dst *symTables, id signal.SeriesID) {
	p, ok := s.keep(dst, tableFunctions, id)
	if !ok {
		return
	}

	for range 3 {
		var str signal.SeriesID
		if str, p, ok = readID(p); !ok {
			return
		}

		s.keep(dst, tableStrings, str)
	}
}

func readID(p []byte) (signal.SeriesID, []byte, bool) {
	if len(p) < 16 {
		return signal.SeriesID{}, p, false
	}

	return signal.SeriesID{Hi: binary.BigEndian.Uint64(p), Lo: binary.BigEndian.Uint64(p[8:])}, p[16:], true
}

func skipUvarint(p []byte) ([]byte, bool) {
	if _, n := binary.Uvarint(p); n > 0 {
		return p[n:], true
	}

	return p, false
}
