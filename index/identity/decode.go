package identity

import (
	"encoding/binary"
	"hash/crc32"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/signal"
)

// Decode calls fn for every identity in data, in ascending id order, stopping at the first error fn
// returns.
//
// The identities it yields **alias data**: their label bytes point into the object's symbol table,
// so a caller that keeps one past the call must intern or clone it (the series index does, on Add).
// That is what makes loading a part's identities allocation-light on the recovery path.
func Decode(data []byte, fn func(id signal.SeriesID, s signal.Series) error) error {
	toc, err := readTOC(data)
	if err != nil {
		return err
	}

	symData, ok := toc.section(sectionSymbols)
	if !ok {
		return errors.Wrap(ErrCorrupt, "no symbol section")
	}

	tab, err := symbols.Decode(symData)
	if err != nil {
		return errors.Wrap(err, "decode symbols")
	}

	body, ok := toc.section(sectionSeries)
	if !ok {
		return errors.Wrap(ErrCorrupt, "no series section")
	}

	return decodeSeries(body, tab, fn)
}

// Count returns the number of identities the object holds, without decoding them.
func Count(data []byte) (int, error) {
	toc, err := readTOC(data)
	if err != nil {
		return 0, err
	}

	body, ok := toc.section(sectionSeries)
	if !ok {
		return 0, errors.Wrap(ErrCorrupt, "no series section")
	}

	n, k := binary.Uvarint(body)
	if k <= 0 {
		return 0, errors.Wrap(ErrCorrupt, "series count")
	}

	return int(n), nil
}

// toc is the parsed table of contents: each section's bytes, validated against its checksum on
// access.
type toc struct {
	data     []byte
	sections []tocEntry
}

type tocEntry struct {
	kind   byte
	offset uint64
	length uint64
	crc    uint32
}

// section returns the bytes of the first section of the given kind, checksum-verified. An unknown
// kind — or one a later writer added and this reader does not know — is simply not found.
func (t toc) section(kind byte) ([]byte, bool) {
	for _, e := range t.sections {
		if e.kind != kind {
			continue
		}

		end := e.offset + e.length
		if end > uint64(len(t.data)) || e.offset > end {
			return nil, false
		}

		b := t.data[e.offset:end]
		if crc32.Checksum(b, castagnoli) != e.crc {
			return nil, false
		}

		return b, true
	}

	return nil, false
}

// readTOC locates and parses the table of contents from the object's tail. It validates every
// bound, so a truncated or corrupt object yields an error rather than a panic.
func readTOC(data []byte) (toc, error) {
	if len(data) < headerLen+tocTrailer {
		return toc{}, errors.Wrap(ErrCorrupt, "short object")
	}

	if binary.BigEndian.Uint32(data) != magic {
		return toc{}, errors.Wrap(ErrCorrupt, "bad magic")
	}

	if data[4] != version {
		return toc{}, errors.Wrapf(ErrCorrupt, "version %d", data[4])
	}

	trailer := data[len(data)-tocTrailer:]
	tocOff := binary.BigEndian.Uint32(trailer)
	want := binary.BigEndian.Uint32(trailer[4:])

	if uint64(tocOff) < headerLen || uint64(tocOff) > uint64(len(data)-tocTrailer) {
		return toc{}, errors.Wrap(ErrCorrupt, "toc offset")
	}

	body := data[tocOff : len(data)-tocTrailer]
	if crc32.Checksum(body, castagnoli) != want {
		return toc{}, errors.Wrap(ErrCorrupt, "toc checksum")
	}

	count, k := binary.Uvarint(body)
	if k <= 0 || count > uint64(len(body)) {
		return toc{}, errors.Wrap(ErrCorrupt, "toc count")
	}

	body = body[k:]

	out := toc{data: data, sections: make([]tocEntry, 0, count)}

	for range count {
		if len(body) < 1 {
			return toc{}, errors.Wrap(ErrCorrupt, "toc entry")
		}

		e := tocEntry{kind: body[0]}
		body = body[1:]

		var n int

		if e.offset, n = binary.Uvarint(body); n <= 0 {
			return toc{}, errors.Wrap(ErrCorrupt, "section offset")
		}

		body = body[n:]

		if e.length, n = binary.Uvarint(body); n <= 0 {
			return toc{}, errors.Wrap(ErrCorrupt, "section length")
		}

		body = body[n:]

		if len(body) < 4 {
			return toc{}, errors.Wrap(ErrCorrupt, "section checksum")
		}

		e.crc = binary.BigEndian.Uint32(body)
		body = body[4:]

		out.sections = append(out.sections, e)
	}

	return out, nil
}

// decodeSeries walks the series section, resolving every symbol reference against tab.
func decodeSeries(body []byte, tab *symbols.Table, fn func(signal.SeriesID, signal.Series) error) error {
	count, k := binary.Uvarint(body)
	if k <= 0 {
		return errors.Wrap(ErrCorrupt, "series count")
	}

	d := decoder{rest: body[k:], tab: tab}

	for range count {
		id, s, err := d.series()
		if err != nil {
			return err
		}

		if err := fn(id, s); err != nil {
			return err
		}
	}

	return nil
}

// decoder walks the series section. Every read is bounds-checked against the remaining bytes.
type decoder struct {
	rest []byte
	tab  *symbols.Table
}

func (d *decoder) series() (signal.SeriesID, signal.Series, error) {
	var (
		id signal.SeriesID
		s  signal.Series
	)

	if len(d.rest) < 16 {
		return id, s, errors.Wrap(ErrCorrupt, "series id")
	}

	id = signal.SeriesID{
		Hi: binary.BigEndian.Uint64(d.rest),
		Lo: binary.BigEndian.Uint64(d.rest[8:]),
	}
	d.rest = d.rest[16:]

	var err error

	if s.Resource.SchemaURL, err = d.sym(); err != nil {
		return id, s, err
	}

	if s.Resource.Attributes, err = d.attrs(); err != nil {
		return id, s, err
	}

	if s.Scope.Name, err = d.sym(); err != nil {
		return id, s, err
	}

	if s.Scope.Version, err = d.sym(); err != nil {
		return id, s, err
	}

	if s.Scope.SchemaURL, err = d.sym(); err != nil {
		return id, s, err
	}

	if s.Scope.Attributes, err = d.attrs(); err != nil {
		return id, s, err
	}

	s.Attributes, err = d.attrs()

	return id, s, err
}

// sym reads one symbol reference and returns the table's bytes for it (aliasing the object).
func (d *decoder) sym() ([]byte, error) {
	v, n := binary.Uvarint(d.rest)
	if n <= 0 {
		return nil, errors.Wrap(ErrCorrupt, "symbol reference")
	}

	d.rest = d.rest[n:]

	// Checked before the narrowing conversion: symbols.ID is 32-bit, so a ref of 2^32+k would
	// otherwise decode as symbol k — a different identity, with no error.
	if v > math.MaxUint32 {
		return nil, errors.Wrapf(ErrCorrupt, "symbol %d out of range", v)
	}

	b, ok := d.tab.Get(symbols.ID(v))
	if !ok {
		return nil, errors.Wrapf(ErrCorrupt, "symbol %d out of range", v)
	}

	return b, nil
}

func (d *decoder) attrs() (signal.Attributes, error) {
	count, n := binary.Uvarint(d.rest)
	if n <= 0 {
		return nil, errors.Wrap(ErrCorrupt, "attribute count")
	}

	d.rest = d.rest[n:]

	// Two symbol references per attribute is the floor, so a count larger than the remaining bytes
	// is malformed — checked before allocating, so a corrupt length cannot request a huge slice.
	if count > uint64(len(d.rest)) {
		return nil, errors.Wrapf(ErrCorrupt, "attribute count %d exceeds remaining bytes", count)
	}

	if count == 0 {
		return nil, nil
	}

	out := make(signal.Attributes, count)

	for i := range out {
		key, err := d.sym()
		if err != nil {
			return nil, err
		}

		raw, err := d.sym()
		if err != nil {
			return nil, err
		}

		v, _, err := signal.DecodeValue(raw)
		if err != nil {
			return nil, errors.Wrap(err, "decode attribute value")
		}

		out[i] = signal.KeyValue{Key: key, Value: v}
	}

	return out, nil
}
