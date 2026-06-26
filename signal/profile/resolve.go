package profile

import (
	"encoding/binary"

	"github.com/oteldb/storage/signal"
)

// Frame is one resolved stack frame: a function (and its source file/line). It is what an embedder
// turns into a flamegraph node name when merging the [ColStackID] column of a sample fetch.
type Frame struct {
	Function string
	File     string
	Line     int64
}

// Resolver resolves a content-addressed stack id (the [ColStackID] column) to its [Frame]s, leaf
// first. Build one from a [recordengine.Engine.SideSnapshot] (the tenant's unioned symbol store) via
// [NewResolver]; it is read-only and safe for concurrent use.
type Resolver struct {
	strings   map[signal.SeriesID][]byte
	functions map[signal.SeriesID][]byte
	locations map[signal.SeriesID][]byte
	stacks    map[signal.SeriesID][]byte
}

// NewResolver decodes the symbol-store tables (as produced by [SymbolStore.Encode]/Union) into a
// resolver. Absent tables are treated as empty.
func NewResolver(tables map[string][]byte) (*Resolver, error) {
	decoded := make([]map[signal.SeriesID][]byte, len(tableNames))
	for i, name := range tableNames {
		m := map[signal.SeriesID][]byte{}
		if data, ok := tables[name]; ok {
			if err := decodeTable(m, data); err != nil {
				return nil, err
			}
		}

		decoded[i] = m
	}

	// tableNames order: strings, mappings, functions, locations, stacks.
	return &Resolver{
		strings:   decoded[0],
		functions: decoded[2],
		locations: decoded[3],
		stacks:    decoded[4],
	}, nil
}

// Resolve returns the frames of the stack identified by stackID (16 big-endian bytes, as stored in
// the [ColStackID] column), leaf first. An unknown stack (or a stack id of the wrong length) yields
// nil. Malformed entries are skipped, so resolution never panics.
func (r *Resolver) Resolve(stackID []byte) []Frame {
	id, ok := idFromBytes(stackID)
	if !ok {
		return nil
	}

	entry, ok := r.stacks[id]
	if !ok {
		return nil
	}

	locIDs, ok := readIDList(entry)
	if !ok {
		return nil
	}

	var frames []Frame
	for _, lid := range locIDs {
		frames = r.appendLocationFrames(frames, lid)
	}

	return frames
}

// appendLocationFrames resolves one location's lines (a location may carry several inlined frames)
// and appends a [Frame] per line.
func (r *Resolver) appendLocationFrames(dst []Frame, locID signal.SeriesID) []Frame {
	entry, ok := r.locations[locID]
	if !ok {
		return dst
	}

	p := entry
	if len(p) < 16 { // mapping id
		return dst
	}

	p = p[16:]

	if _, n := binary.Uvarint(p); n > 0 { // address
		p = p[n:]
	} else {
		return dst
	}

	nLines, n := binary.Uvarint(p)
	if n <= 0 {
		return dst
	}

	p = p[n:]

	for range nLines {
		if len(p) < 16 {
			return dst
		}

		fnID := signal.SeriesID{Hi: binary.BigEndian.Uint64(p), Lo: binary.BigEndian.Uint64(p[8:])}
		p = p[16:]

		line, n := binary.Varint(p) // line
		if n <= 0 {
			return dst
		}

		p = p[n:]

		if _, n := binary.Varint(p); n > 0 { // column (unused)
			p = p[n:]
		} else {
			return dst
		}

		dst = append(dst, r.frame(fnID, line))
	}

	return dst
}

// frame builds a [Frame] from a function id and source line, resolving the function's name/file
// strings (empty when absent).
func (r *Resolver) frame(fnID signal.SeriesID, line int64) Frame {
	f := Frame{Line: line}

	entry, ok := r.functions[fnID]
	if !ok || len(entry) < 48 { // nameID + sysID + fileID (3 × 16)
		return f
	}

	nameID := signal.SeriesID{Hi: binary.BigEndian.Uint64(entry), Lo: binary.BigEndian.Uint64(entry[8:])}
	fileID := signal.SeriesID{Hi: binary.BigEndian.Uint64(entry[32:]), Lo: binary.BigEndian.Uint64(entry[40:])}
	f.Function = string(r.strings[nameID])
	f.File = string(r.strings[fileID])

	return f
}

// idFromBytes parses a 16-byte big-endian content id.
func idFromBytes(b []byte) (signal.SeriesID, bool) {
	if len(b) != 16 {
		return signal.SeriesID{}, false
	}

	return signal.SeriesID{Hi: binary.BigEndian.Uint64(b), Lo: binary.BigEndian.Uint64(b[8:])}, true
}

// readIDList reads a [uvarint count][count × 16-byte id] list (a stack's location ids).
func readIDList(entry []byte) ([]signal.SeriesID, bool) {
	count, n := binary.Uvarint(entry)
	if n <= 0 || count > uint64(len(entry)) {
		return nil, false
	}

	p := entry[n:]
	if len(p) < int(count)*16 {
		return nil, false
	}

	ids := make([]signal.SeriesID, count)
	for i := range ids {
		ids[i] = signal.SeriesID{Hi: binary.BigEndian.Uint64(p), Lo: binary.BigEndian.Uint64(p[8:])}
		p = p[16:]
	}

	return ids, true
}
