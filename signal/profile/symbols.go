package profile

import (
	"encoding/binary"
	"hash/crc32"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/signal"
)

// The profiles symbol store is a content-addressed, deduplicated set of five tables — strings,
// mappings, functions, locations, stacks — keyed by a 16-byte content hash ([signal.SeriesID]).
// Ids are computed Merkle-style bottom-up: a string's id hashes its bytes; a function's id hashes
// its (content-id) string references; a location's id hashes its function/mapping references; a
// stack's id hashes its location references. Because an entry's id depends only on its content, the
// same entry has the same id in every batch, part, and node — so the engine's side-store union is a
// plain dedup with no id remap (see [recordengine.SideStore]).
//
// Each stored entry's bytes are the hash pre-image without the table tag; the tag distinguishes
// tables so unrelated entries never collide on an id.

// ErrCorruptSymbols is returned when a serialized symbol table/delta fails to parse.
var ErrCorruptSymbols = errors.New("profile: corrupt symbol table")

// Table tags (also the leading byte of each id's hash pre-image).
const (
	tagString byte = iota + 1
	tagMapping
	tagFunction
	tagLocation
	tagStack
)

// Sidecar table names, in delta order.
var tableNames = []string{"strings", "mappings", "functions", "locations", "stacks"}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// hashEntry returns the content id of a table entry: xxh3-128 over the table tag followed by the
// entry's canonical bytes.
func hashEntry(tag byte, entry []byte) signal.SeriesID {
	pre := make([]byte, 0, 1+len(entry))
	pre = append(pre, tag)
	pre = append(pre, entry...)

	return signal.HashBytes(pre)
}

// symTables is a set of the five content-addressed tables (id → entry bytes). Used both as a
// per-batch delta (only referenced entries) and as the accumulator's running state.
type symTables struct {
	t [5]map[signal.SeriesID][]byte
}

func newSymTables() symTables {
	var s symTables
	for i := range s.t {
		s.t[i] = make(map[signal.SeriesID][]byte)
	}

	return s
}

// put records an entry under id in table i (dedup: first writer wins; content-addressing guarantees
// identical bytes for an id).
func (s *symTables) put(i int, id signal.SeriesID, entry []byte) {
	if _, ok := s.t[i][id]; !ok {
		s.t[i][id] = entry
	}
}

// ---- builder: resolve a batch [Dictionary] into content ids, collecting entries into a delta ----

// builder resolves one batch's referenced symbols into content ids, recording every entry it touches
// into tables (the delta to ship as Batch.Side). Index→id memos keep the walk near-linear and
// dedup within the batch. Out-of-range dictionary indices resolve to the zero id (defensive: the
// dictionary is untrusted).
type builder struct {
	dict    *Dictionary
	tables  symTables
	strMemo map[int32]signal.SeriesID
	fnMemo  map[int32]signal.SeriesID
	mapMemo map[int32]signal.SeriesID
	locMemo map[int32]signal.SeriesID
}

func newBuilder(d *Dictionary) *builder {
	return &builder{
		dict:    d,
		tables:  newSymTables(),
		strMemo: map[int32]signal.SeriesID{},
		fnMemo:  map[int32]signal.SeriesID{},
		mapMemo: map[int32]signal.SeriesID{},
		locMemo: map[int32]signal.SeriesID{},
	}
}

func inRange[T any](s []T, i int32) bool { return i >= 0 && int(i) < len(s) }

func (b *builder) stringID(idx int32) signal.SeriesID {
	if id, ok := b.strMemo[idx]; ok {
		return id
	}

	var raw []byte
	if inRange(b.dict.Strings, idx) {
		raw = b.dict.Strings[idx]
	}

	id := hashEntry(tagString, raw)
	b.tables.put(0, id, append([]byte(nil), raw...))
	b.strMemo[idx] = id

	return id
}

func (b *builder) mappingID(idx int32) signal.SeriesID {
	if id, ok := b.mapMemo[idx]; ok {
		return id
	}

	var m Mapping
	if inRange(b.dict.Mappings, idx) {
		m = b.dict.Mappings[idx]
	}

	entry := binary.AppendUvarint(nil, m.MemoryStart)
	entry = binary.AppendUvarint(entry, m.MemoryLimit)
	entry = binary.AppendUvarint(entry, m.FileOffset)
	entry = b.stringID(m.FilenameStrindex).AppendBinary(entry)

	id := hashEntry(tagMapping, entry)
	b.tables.put(1, id, entry)
	b.mapMemo[idx] = id

	return id
}

func (b *builder) functionID(idx int32) signal.SeriesID {
	if id, ok := b.fnMemo[idx]; ok {
		return id
	}

	var f Function
	if inRange(b.dict.Functions, idx) {
		f = b.dict.Functions[idx]
	}

	entry := b.stringID(f.NameStrindex).AppendBinary(nil)
	entry = b.stringID(f.SystemNameStrindex).AppendBinary(entry)
	entry = b.stringID(f.FilenameStrindex).AppendBinary(entry)
	entry = binary.AppendVarint(entry, f.StartLine)

	id := hashEntry(tagFunction, entry)
	b.tables.put(2, id, entry)
	b.fnMemo[idx] = id

	return id
}

func (b *builder) locationID(idx int32) signal.SeriesID {
	if id, ok := b.locMemo[idx]; ok {
		return id
	}

	var l Location
	if inRange(b.dict.Locations, idx) {
		l = b.dict.Locations[idx]
	}

	entry := b.mappingID(l.MappingIndex).AppendBinary(nil)
	entry = binary.AppendUvarint(entry, l.Address)
	entry = binary.AppendUvarint(entry, uint64(len(l.Lines)))

	for _, ln := range l.Lines {
		entry = b.functionID(ln.FunctionIndex).AppendBinary(entry)
		entry = binary.AppendVarint(entry, ln.Line)
		entry = binary.AppendVarint(entry, ln.Column)
	}

	id := hashEntry(tagLocation, entry)
	b.tables.put(3, id, entry)
	b.locMemo[idx] = id

	return id
}

// stackID resolves a batch-local stack index to its content id, recording the stack and every symbol
// it transitively references into the delta. Stacks are not memoized (each sample's stack index is
// typically distinct); the recursion is memoized at the location level and below.
func (b *builder) stackID(idx int32) signal.SeriesID {
	var st Stack
	if inRange(b.dict.Stacks, idx) {
		st = b.dict.Stacks[idx]
	}

	entry := binary.AppendUvarint(nil, uint64(len(st.LocationIndices)))
	for _, li := range st.LocationIndices {
		entry = b.locationID(li).AppendBinary(entry)
	}

	id := hashEntry(tagStack, entry)
	b.tables.put(4, id, entry)

	return id
}

// ---- table / delta encoding (CRC-framed, fuzz-safe decode) ----

const (
	symMagic   uint32 = 0x4F545350 // "OTSP"
	symVersion uint32 = 1
)

// encodeTable serializes one table (sorted by id) as [magic][version][uvarint count] then per entry
// [16B id][uvarint len][bytes], with a trailing CRC32C.
func encodeTable(m map[signal.SeriesID][]byte) []byte {
	ids := make([]signal.SeriesID, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}

	slices.SortFunc(ids, func(a, b signal.SeriesID) int { return a.Compare(b) })

	out := binary.BigEndian.AppendUint32(nil, symMagic)
	out = binary.BigEndian.AppendUint32(out, symVersion)
	out = binary.AppendUvarint(out, uint64(len(ids)))

	for _, id := range ids {
		out = id.AppendBinary(out)
		entry := m[id]
		out = binary.AppendUvarint(out, uint64(len(entry)))
		out = append(out, entry...)
	}

	return binary.BigEndian.AppendUint32(out, crc32.Checksum(out, castagnoli))
}

// decodeTable parses encodeTable's output into dst (merging, dedup), verifying the CRC and
// bounds-checking every read.
func decodeTable(dst map[signal.SeriesID][]byte, data []byte) error {
	if len(data) < 12 { // magic+version+crc
		return ErrCorruptSymbols
	}

	body := data[:len(data)-4]
	if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(data[len(data)-4:]) {
		return errors.Wrap(ErrCorruptSymbols, "crc mismatch")
	}

	if binary.BigEndian.Uint32(body) != symMagic || binary.BigEndian.Uint32(body[4:]) != symVersion {
		return errors.Wrap(ErrCorruptSymbols, "bad header")
	}

	p := body[8:]

	count, n := binary.Uvarint(p)
	if n <= 0 || count > uint64(len(p)) {
		return errCorrupt("count")
	}

	p = p[n:]

	for range count {
		if len(p) < 16 {
			return errCorrupt("id")
		}

		id := signal.SeriesID{Hi: binary.BigEndian.Uint64(p), Lo: binary.BigEndian.Uint64(p[8:])}
		p = p[16:]

		ln, n := binary.Uvarint(p)
		if n <= 0 || ln > uint64(len(p)-n) {
			return errCorrupt("entry len")
		}

		p = p[n:]
		entry := append([]byte(nil), p[:ln]...)
		p = p[ln:]

		if _, ok := dst[id]; !ok {
			dst[id] = entry
		}
	}

	return nil
}

func errCorrupt(what string) error { return errors.Wrap(ErrCorruptSymbols, what) }

// encodeDelta serializes all five tables in order, each as a length-prefixed [encodeTable] blob.
func encodeDelta(s symTables) []byte {
	var out []byte
	for i := range s.t {
		blob := encodeTable(s.t[i])
		out = binary.AppendUvarint(out, uint64(len(blob)))
		out = append(out, blob...)
	}

	return out
}

// decodeDeltaInto merges a delta (five length-prefixed table blobs) into dst.
func decodeDeltaInto(dst *symTables, data []byte) error {
	p := data
	for i := range dst.t {
		ln, n := binary.Uvarint(p)
		if n <= 0 || ln > uint64(len(p)-n) {
			return errCorrupt("delta table len")
		}

		p = p[n:]
		if err := decodeTable(dst.t[i], p[:ln]); err != nil {
			return err
		}

		p = p[ln:]
	}

	return nil
}

// ---- SymbolStore: the recordengine.SideStore implementation ----

// SymbolStore is the per-engine accumulator of the profiles symbol tables. It implements
// recordengine.SideStore: it absorbs each batch's delta, encodes the accumulated tables as part
// sidecars on flush, and unions sidecars on merge. Not safe for concurrent use (the engine serializes
// access under its lock).
type SymbolStore struct {
	acc symTables
}

// NewSymbolStore returns an empty symbol store for use as a recordengine.SideStore.
func NewSymbolStore() *SymbolStore { return &SymbolStore{acc: newSymTables()} }

// Absorb merges one batch's encoded symbol delta into the accumulator.
func (s *SymbolStore) Absorb(delta []byte) error { return decodeDeltaInto(&s.acc, delta) }

// Encode returns the accumulated tables as named sidecar payloads.
func (s *SymbolStore) Encode() map[string][]byte {
	out := make(map[string][]byte, len(tableNames))
	for i, name := range tableNames {
		out[name] = encodeTable(s.acc.t[i])
	}

	return out
}

// Reset clears the accumulator.
func (s *SymbolStore) Reset() { s.acc = newSymTables() }

// Names returns the sidecar table names.
func (s *SymbolStore) Names() []string { return tableNames }

// Union merges the loaded sidecars of compacted parts (one map per part) into one merged set of
// named tables. Pure: it does not read the live accumulator.
func (s *SymbolStore) Union(parts []map[string][]byte) (map[string][]byte, error) {
	merged := newSymTables()

	for _, part := range parts {
		for i, name := range tableNames {
			data, ok := part[name]
			if !ok {
				continue
			}

			if err := decodeTable(merged.t[i], data); err != nil {
				return nil, err
			}
		}
	}

	out := make(map[string][]byte, len(tableNames))
	for i, name := range tableNames {
		out[name] = encodeTable(merged.t[i])
	}

	return out, nil
}
