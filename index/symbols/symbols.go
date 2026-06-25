// Package symbols is the string-interning symbol table. It maps attribute name and
// value byte strings to small integer ids and back, so identity and columns store ids
// instead of repeating bytes. Keys are []byte (never string) and looked up through a
// pool.ByteIntMap, keeping interning allocation-free for repeated symbols.
package symbols

import (
	"encoding/binary"
	"hash/crc32"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/bitstream"
	"github.com/oteldb/storage/pool"
)

// ID is a symbol's integer id, assigned sequentially from 0 in insertion order.
type ID uint32

const (
	symbolsMagic   uint32 = 0x4F545359 // "OTSY"
	symbolsVersion uint32 = 1
)

// ErrCorrupt is returned when a serialized symbol table fails to parse.
var ErrCorrupt = errors.New("symbols: corrupt symbol table")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Table interns byte strings to [ID]s and back. The zero value is not usable; create
// one with [New]. Not safe for concurrent use.
type Table struct {
	ids  *pool.ByteIntMap // bytes → id
	syms [][]byte         // id → bytes (owned copies)
}

// New returns an empty [Table].
func New() *Table {
	return &Table{ids: pool.NewByteIntMap()}
}

// Intern returns the id for b, inserting it (with an owned copy) if new. The caller may
// reuse b after Intern returns.
func (t *Table) Intern(b []byte) ID {
	if id, ok := t.ids.Get(b); ok {
		return ID(id)
	}

	return t.add(b)
}

// Lookup returns the id for b and whether it is interned (without inserting).
func (t *Table) Lookup(b []byte) (ID, bool) {
	id, ok := t.ids.Get(b)

	return ID(id), ok
}

// Get returns the bytes for id (aliasing the table) and whether id is valid.
func (t *Table) Get(id ID) ([]byte, bool) {
	if int(id) >= len(t.syms) {
		return nil, false
	}

	return t.syms[id], true
}

// Len returns the number of interned symbols.
func (t *Table) Len() int { return len(t.syms) }

// Release returns the underlying map to the shared pool. After Release the table must
// not be used.
func (t *Table) Release() {
	if t.ids != nil {
		t.ids.PutBack()
		t.ids = nil
	}

	t.syms = nil
}

// Encode appends the serialized symbol table to dst. Layout:
//
//	[u32 magic][uvarint version][uvarint count]
//	  per symbol in id order: [uvarint len][bytes]
//	[u32 CRC32C]
func (t *Table) Encode(dst []byte) []byte {
	start := len(dst)

	w := bitstream.NewWriter(dst)
	binary.BigEndian.PutUint32(w.AppendBytes(4), symbolsMagic)
	w.WriteUvarint(uint64(symbolsVersion))
	w.WriteUvarint(uint64(len(t.syms)))

	for _, s := range t.syms {
		w.WriteUvarint(uint64(len(s)))
		w.WriteBytes(s)
	}

	w.PadToByte()
	out := w.Bytes()
	crc := crc32.Checksum(out[start:], castagnoli)

	return binary.BigEndian.AppendUint32(out, crc)
}

// Decode parses a serialized symbol table. It verifies the CRC and bounds-checks every
// field, returning an [ErrCorrupt]-wrapping error on malformed input; it never panics.
func Decode(src []byte) (*Table, error) {
	if len(src) < 4 {
		return nil, errors.Wrap(ErrCorrupt, "too short for CRC")
	}

	body := src[:len(src)-4]
	if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(src[len(src)-4:]) {
		return nil, errors.Wrap(ErrCorrupt, "CRC mismatch")
	}

	r := bitstream.NewReader(body)

	magic, err := readU32(r)
	if err != nil || magic != symbolsMagic {
		return nil, errors.Wrap(ErrCorrupt, "bad magic")
	}

	version, err := r.ReadUvarint()
	if err != nil || version != uint64(symbolsVersion) {
		return nil, errors.Wrap(ErrCorrupt, "version")
	}

	count, err := r.ReadUvarint()
	if err != nil {
		return nil, errors.Wrap(ErrCorrupt, "count")
	}

	if count > uint64(len(body)) {
		return nil, errors.Wrapf(ErrCorrupt, "count %d exceeds body", count)
	}

	t := New()
	for i := range count {
		ln, err := r.ReadUvarint()
		if err != nil {
			return nil, errors.Wrapf(ErrCorrupt, "symbol %d length", i)
		}

		view, err := r.ReadBytesView(int(ln))
		if err != nil {
			return nil, errors.Wrapf(ErrCorrupt, "symbol %d bytes", i)
		}

		t.add(view)
	}

	return t, nil
}

// add stores an owned copy of b and returns its new id.
func (t *Table) add(b []byte) ID {
	stable := slices.Clone(b)
	id := ID(len(t.syms))
	t.syms = append(t.syms, stable)
	t.ids.Put(stable, int(id))

	return id
}

func readU32(r *bitstream.Reader) (uint32, error) {
	b, err := r.ReadBytesView(4)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(b), nil
}
