// Package identity is the on-disk form of a part's identity set: the [signal.Series] of every
// series the part holds, encoded against a symbol table private to the object.
//
// It is what makes identity **part-scoped**. A whole-set identity object at the engine prefix has
// to be rewritten whenever the set changes and never shrinks with the data, so identities outlive
// the samples that named them and a node's resident index spans every series it has ever seen.
// Scoping identity to the part that holds it makes retention self-cleaning (deleting a part deletes
// its identities), makes a flush cost what it added rather than what the tenant has, and gives every
// node — owner or replica — a live identity set derived from its own parts. Prometheus' block index
// and VictoriaMetrics' per-partition indexDB have the same shape; the duplication of an identity
// across the parts that share it is the accepted price, folded back by merges.
//
// # Layout
//
//	[magic "OTID" <4b>][version <1b>]
//	  section bytes …
//	[TOC: uvarint count, then per section: kind <1b>, uvarint offset, uvarint length, CRC32C <4b>]
//	[TOC offset <4b BE>][CRC32C of the TOC <4b>]
//
// Sections are addressed by the trailing table of contents, and the TOC's own offset is the last
// fixed-width field, so a reader can find it from the object's tail alone — a section can be read by
// byte range without fetching the whole object, and a section kind added later without breaking a
// reader (unknown kinds are skipped). Today there are two: the symbol table and the series records.
//
// The object is deliberately **not compressed**: interning already removes the repetition a
// compressor would find (measured ~3.8× on a churn-shaped set), and a compressed body would defeat
// the range-addressability the layout exists to preserve.
package identity

import (
	"encoding/binary"
	"hash/crc32"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/signal"
)

const (
	magic   uint32 = 0x4F544944 // "OTID"
	version byte   = 1

	// tocTrailer is the fixed-width tail: the TOC's offset followed by its checksum.
	tocTrailer = 8
	// headerLen is the magic and version preceding the first section.
	headerLen = 5
)

// Section kinds. A reader skips a kind it does not know, so a later version can add one (the
// postings and offset tables a per-part index would need) without a format break.
const (
	sectionSymbols byte = 1
	sectionSeries  byte = 2
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt is returned when an identity object fails to parse. Decoding never panics on
// malformed input.
var ErrCorrupt = errors.New("identity: corrupt identity object")

// Encode appends the identity object for entries to dst. Entries are written in ascending id order
// (the caller's slice is not modified), matching the part's series-index sidecar so the two can be
// read positionally later.
func Encode(dst []byte, entries []series.Entry) []byte {
	order := make([]int, len(entries))
	for i := range order {
		order[i] = i
	}

	slices.SortFunc(order, func(a, b int) int { return entries[a].ID.Compare(entries[b].ID) })

	// The series section is built first so its symbol references exist by the time the symbol
	// section is written; the sections are then emitted symbols-first, as a reader needs them.
	tab := symbols.New()
	enc := encoder{tab: tab}

	var body []byte

	body = binary.AppendUvarint(body, uint64(len(order)))
	for _, i := range order {
		body = enc.appendSeries(body, entries[i])
	}

	start := len(dst)
	dst = binary.BigEndian.AppendUint32(dst, magic)
	dst = append(dst, version)

	var (
		toc      []byte
		sections int
	)

	for _, sec := range []struct {
		kind byte
		data []byte
	}{
		{sectionSymbols, tab.Encode(nil)},
		{sectionSeries, body},
	} {
		dst, toc = appendSection(dst, toc, start, sec.kind, sec.data)
		sections++
	}

	tocOff := len(dst) - start
	dst = binary.AppendUvarint(dst, uint64(sections))
	dst = append(dst, toc...)

	tocBytes := dst[start+tocOff:]
	dst = binary.BigEndian.AppendUint32(dst, uint32(tocOff))

	return binary.BigEndian.AppendUint32(dst, crc32.Checksum(tocBytes, castagnoli))
}

// appendSection writes one section's bytes and records its TOC entry (kind, offset relative to the
// object start, length, checksum).
func appendSection(dst, toc []byte, start int, kind byte, data []byte) ([]byte, []byte) {
	off := len(dst) - start
	dst = append(dst, data...)

	toc = append(toc, kind)
	toc = binary.AppendUvarint(toc, uint64(off))
	toc = binary.AppendUvarint(toc, uint64(len(data)))
	toc = binary.BigEndian.AppendUint32(toc, crc32.Checksum(data, castagnoli))

	return dst, toc
}

// encoder writes identities against a symbol table it fills as it goes.
type encoder struct {
	tab  *symbols.Table
	vbuf []byte
}

func (e *encoder) appendSeries(dst []byte, ent series.Entry) []byte {
	dst = binary.BigEndian.AppendUint64(dst, ent.ID.Hi)
	dst = binary.BigEndian.AppendUint64(dst, ent.ID.Lo)

	dst = e.appendSym(dst, ent.Series.Resource.SchemaURL)
	dst = e.appendAttrs(dst, ent.Series.Resource.Attributes)

	dst = e.appendSym(dst, ent.Series.Scope.Name)
	dst = e.appendSym(dst, ent.Series.Scope.Version)
	dst = e.appendSym(dst, ent.Series.Scope.SchemaURL)
	dst = e.appendAttrs(dst, ent.Series.Scope.Attributes)

	return e.appendAttrs(dst, ent.Series.Attributes)
}

func (e *encoder) appendSym(dst, b []byte) []byte {
	return binary.AppendUvarint(dst, uint64(e.tab.Intern(b)))
}

// appendAttrs writes an attribute set as (key symbol, value symbol) pairs. The value is interned
// from its **type-tagged** encoding, so int 5, "5" and 5.0 stay distinct — the same rule the
// postings index uses for its value ids.
func (e *encoder) appendAttrs(dst []byte, a signal.Attributes) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(a)))

	for i := range a {
		e.vbuf = signal.AppendValue(e.vbuf[:0], a[i].Value)
		dst = e.appendSym(dst, a[i].Key)
		dst = e.appendSym(dst, e.vbuf)
	}

	return dst
}
