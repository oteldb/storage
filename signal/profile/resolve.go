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

// Table indices, in [tableNames] order.
const (
	tableStrings = iota
	tableMappings
	tableFunctions
	tableLocations
	tableStacks
)

// Resolver resolves a content-addressed stack id (the [ColStackID] column) to its [Frame]s, leaf
// first, over a stack of [Tables] layers. It is read-only and safe for concurrent use.
type Resolver struct {
	layers []*Tables
}

// NewResolver decodes the symbol-store tables (as produced by [SymbolStore.Encode]/Union) into a
// resolver. Absent tables are treated as empty.
func NewResolver(tables map[string][]byte) (*Resolver, error) {
	t, err := DecodeTables(tables)
	if err != nil {
		return nil, err
	}

	return NewResolverFrom(t), nil
}

// NewResolverFrom returns a resolver over layers, nil layers skipped. Content addressing makes every
// layer's entry for an id identical, so the order only sets the lookup cost: put first the layers
// most stacks resolve from.
func NewResolverFrom(layers ...*Tables) *Resolver {
	r := &Resolver{layers: make([]*Tables, 0, len(layers))}

	for _, l := range layers {
		if l != nil {
			r.layers = append(r.layers, l)
		}
	}

	return r
}

// Resolve returns the frames of the stack identified by stackID (16 big-endian bytes, as stored in
// the [ColStackID] column), leaf first. An unknown stack (or a stack id of the wrong length) yields
// nil. Malformed entries are skipped, so resolution never panics.
func (r *Resolver) Resolve(stackID []byte) []Frame {
	id, ok := idFromBytes(stackID)
	if !ok {
		return nil
	}

	entry, layer, ok := r.lookup(tableStacks, id, 0)
	if !ok {
		return nil
	}

	locIDs, ok := readIDList(entry)
	if !ok {
		return nil
	}

	var frames []Frame
	for _, lid := range locIDs {
		frames = r.appendLocationFrames(frames, lid, layer)
	}

	return frames
}

// lookup finds id in the table, trying layer hint first and then the rest in order, and returns the
// layer that held it. A stack's whole closure is in the layer that holds the stack — flush writes the
// accumulator every batch's closure was absorbed into, and merge unions whole sidecars — so passing
// the stack's layer as the hint resolves a frame in one probe instead of one per newer layer.
func (r *Resolver) lookup(table int, id signal.SeriesID, hint int) ([]byte, int, bool) {
	if hint < len(r.layers) {
		if entry, ok := r.layers[hint].t.t[table][id]; ok {
			return entry, hint, true
		}
	}

	for i, l := range r.layers {
		if i == hint {
			continue
		}

		if entry, ok := l.t.t[table][id]; ok {
			return entry, i, true
		}
	}

	return nil, hint, false
}

// appendLocationFrames resolves one location's lines (a location may carry several inlined frames)
// and appends a [Frame] per line.
func (r *Resolver) appendLocationFrames(dst []Frame, locID signal.SeriesID, hint int) []Frame {
	entry, hint, ok := r.lookup(tableLocations, locID, hint)
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

		dst = append(dst, r.frame(fnID, line, hint))
	}

	return dst
}

// frame builds a [Frame] from a function id and source line, resolving the function's name/file
// strings (empty when absent).
func (r *Resolver) frame(fnID signal.SeriesID, line int64, hint int) Frame {
	f := Frame{Line: line}

	entry, hint, ok := r.lookup(tableFunctions, fnID, hint)
	if !ok || len(entry) < 48 { // nameID + sysID + fileID (3 × 16)
		return f
	}

	nameID := signal.SeriesID{Hi: binary.BigEndian.Uint64(entry), Lo: binary.BigEndian.Uint64(entry[8:])}
	fileID := signal.SeriesID{Hi: binary.BigEndian.Uint64(entry[32:]), Lo: binary.BigEndian.Uint64(entry[40:])}
	name, _, _ := r.lookup(tableStrings, nameID, hint)
	file, _, _ := r.lookup(tableStrings, fileID, hint)
	f.Function = string(name)
	f.File = string(file)

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
