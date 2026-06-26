// Package recordengine is the shared storage engine for **record-shaped** signals — logs and
// traces (later, profiles' sample table). A record-shaped signal is a stream (a Resource+Scope
// identity, indexed by the postings layer) of rows that each carry a primary timestamp plus a
// fixed set of typed columns. The engine is the structural twin of package engine (metrics) but
// generic over a [Schema]: it owns the in-memory head, columnar flush to immutable parts, the
// durable bucket-index + identity-index stateless read path, append-only merge with retention,
// per-part column blooms, lazy column decode, and the [fetch.Fetcher] contract — none of which is
// signal-specific. A signal package supplies the column [Schema] and projects its model into the
// engine's column vectors; the engine treats the columns opaquely.
//
// The timestamp (the sort key) and the int128 stream id (the Resource+Scope hash) are implicit and
// not part of the [Schema]; the schema lists only the per-record columns.
package recordengine

import "github.com/oteldb/storage/encoding/chunk"

// Kind is a column's physical type. Records use only int64 and byte-string columns (floats live in
// the metrics engine); a typed value is projected onto one of these at the language edge.
type Kind uint8

const (
	// KindInt64 is a signed 64-bit column (timestamps-relative, severities, durations, ids, …).
	KindInt64 Kind = iota
	// KindBytes is a byte-string column (bodies, names, trace ids, serialized attributes, …).
	KindBytes
)

// BloomMode says whether and how a column feeds its per-part bloom for predicate pruning.
type BloomMode uint8

const (
	// BloomNone builds no bloom for the column.
	BloomNone BloomMode = iota
	// BloomFullText tokenizes the column's value (lowercased words) so a `contains token`
	// condition can prune a part whose bloom lacks the token (e.g. a log body, a span name).
	BloomFullText
	// BloomAttrs treats the column as a serialized [signal.Attributes] blob and adds key-scoped
	// equality (`key‖value`) and full-text (`key‖word`) tokens, so per-record attribute equality
	// and contains conditions prune.
	BloomAttrs
	// BloomEquality adds the column's exact value as a token so a `column == value` condition can
	// prune (e.g. trace-by-id over the trace_id column).
	BloomEquality
)

// Column is one per-record column of a [Schema]: its name, physical kind, on-disk codec (zero ⇒
// the kind's default), and bloom contribution.
type Column struct {
	Name  string
	Kind  Kind
	Codec chunk.Codec
	Bloom BloomMode
}

// colRef locates a column within its kind's vector (recordCols.ints or recordCols.bytes).
type colRef struct {
	kind Kind
	idx  int
}

// Schema is the ordered set of per-record columns a signal stores. It is immutable after
// construction and shared by every engine, part, and record batch of that signal.
type Schema struct {
	cols     []Column
	byName   map[string]colRef
	intCols  []int // indices into cols of the Int64 columns, in declaration order
	byteCols []int // indices into cols of the Bytes columns, in declaration order
}

// NewSchema builds a [Schema] from its columns (declaration order is preserved within each kind).
func NewSchema(cols ...Column) *Schema {
	s := &Schema{cols: cols, byName: make(map[string]colRef, len(cols))}
	for i := range cols {
		switch cols[i].Kind {
		case KindInt64:
			s.byName[cols[i].Name] = colRef{KindInt64, len(s.intCols)}
			s.intCols = append(s.intCols, i)
		case KindBytes:
			s.byName[cols[i].Name] = colRef{KindBytes, len(s.byteCols)}
			s.byteCols = append(s.byteCols, i)
		}
	}

	return s
}

// numInts / numBytes are the column counts per kind.
func (s *Schema) numInts() int  { return len(s.intCols) }
func (s *Schema) numBytes() int { return len(s.byteCols) }

// ref returns the location of the named column, or false if the schema has no such column (e.g. a
// per-record attribute key, which is not a fixed column).
func (s *Schema) ref(name string) (colRef, bool) {
	r, ok := s.byName[name]
	return r, ok
}

// intColumn / byteColumn return the Column metadata for the k-th column of that kind.
func (s *Schema) intColumn(k int) Column  { return s.cols[s.intCols[k]] }
func (s *Schema) byteColumn(k int) Column { return s.cols[s.byteCols[k]] }

// attrsByteCol returns the index (within the byte vector) of the column marked [BloomAttrs] — the
// serialized per-record attributes — and whether the schema has one. Attribute conditions resolve
// against it.
func (s *Schema) attrsByteCol() (int, bool) {
	for k := range s.byteCols {
		if s.byteColumn(k).Bloom == BloomAttrs {
			return k, true
		}
	}

	return 0, false
}
