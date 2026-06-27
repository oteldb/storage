package block

import (
	"encoding/binary"
	"hash/crc32"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/bitstream"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// Kind is the physical type of a column's values (DESIGN.md §6). It selects which
// typed slice a [Column] carries and which codec family applies. Values are
// persisted in the manifest; never reorder.
type Kind uint8

const (
	// KindInt64 is an int64 column (timestamps, counters, series ids).
	KindInt64 Kind = iota
	// KindFloat64 is a float64 column (gauge/sum values).
	KindFloat64
	// KindBytes is a []byte column (low-cardinality attributes, strings).
	KindBytes
	// KindInt128 is a 128-bit id column ([chunk.U128]), e.g. the SeriesID sort key of a
	// metric part. RLE-coded; carries no min/max or constant value in the manifest.
	KindInt128
)

// String returns a stable lower-case kind name.
func (k Kind) String() string {
	switch k {
	case KindInt64:
		return "int64"
	case KindFloat64:
		return "float64"
	case KindBytes:
		return "bytes"
	case KindInt128:
		return "int128"
	default:
		return "unknown"
	}
}

func (k Kind) valid() bool { return k <= KindInt128 }

// manifest framing constants. The manifest is the last object written for a part
// (the commit point) and is the entry point a reader parses first.
const (
	manifestMagic   uint32 = 0x4F54504D // "OTPM" (OTel Part Manifest)
	manifestVersion uint32 = 1

	// flagConst marks a constant-collapsed column (single value, no data object).
	flagConst byte = 1 << 0
	// flagLossy marks a float column carrying a non-zero lossy precision budget; when set, one
	// byte (FloatPrecisionBits) follows the flags byte. Absent on lossless columns, so existing
	// parts and the common path keep their exact layout.
	flagLossy byte = 1 << 1
)

// ErrCorrupt is returned when a manifest (or any part metadata) fails to parse:
// bad magic, CRC mismatch, truncation, or an out-of-range field.
var ErrCorrupt = errors.New("block: corrupt metadata")

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ColumnDesc describes one column in a part: its identity, codecs, constant value (if
// collapsed), and numeric min/max stats. Data offsets are absent — each column is its
// own backend object keyed by ordinal (DESIGN.md §14 M1 multi-key layout).
type ColumnDesc struct {
	Name     string
	Kind     Kind
	Codec    chunk.Codec
	Compress compress.Algorithm
	Const    bool

	// Constant value, set iff Const, by Kind.
	ConstInt64   int64
	ConstFloat64 float64
	ConstBytes   []byte

	// Numeric min/max (KindInt64/KindFloat64). Unused for KindBytes.
	MinInt64, MaxInt64     int64
	MinFloat64, MaxFloat64 float64

	// FloatPrecisionBits is the lossy precision budget a float column was encoded under (the
	// significant mantissa bits retained): 0 ⇒ lossless. Persisted only when non-zero (a
	// flag-gated byte), so lossless parts keep their byte-for-byte layout. The merge engine reads
	// it as the fixed point for age-tiered precision — it never re-coarsens a part already at or
	// below the target budget.
	FloatPrecisionBits uint8
}

// Manifest is the part descriptor: format version, row count, time range, granule size,
// and the per-column descriptors. It is serialized to the `{prefix}/manifest` object,
// CRC32C-checked, and written last to commit the part.
type Manifest struct {
	Version     uint32
	RowCount    int
	MinTime     int64
	MaxTime     int64
	GranuleSize int
	Columns     []ColumnDesc
}

// Encode appends the binary manifest to dst and returns the extended slice. Layout:
//
//	[u32 magic][uvarint version][uvarint rowCount][varint minTime][varint maxTime]
//	[uvarint granuleSize][uvarint colCount]
//	  per column: [uvarint nameLen][name][byte kind][byte codec][byte compress][byte flags]
//	              [byte precisionBits if flagLossy][numeric min/max per kind][const value per kind if flagConst]
//	[u32 CRC32C over all the above]
func (m Manifest) Encode(dst []byte) []byte {
	start := len(dst)

	w := bitstream.NewWriter(dst)
	putU32(w, manifestMagic)
	w.WriteUvarint(uint64(m.Version))
	w.WriteUvarint(uint64(m.RowCount))
	w.WriteVarint(m.MinTime)
	w.WriteVarint(m.MaxTime)
	w.WriteUvarint(uint64(m.GranuleSize))
	w.WriteUvarint(uint64(len(m.Columns)))

	for i := range m.Columns {
		c := &m.Columns[i]
		w.WriteUvarint(uint64(len(c.Name)))
		w.AppendString(c.Name)
		_ = w.WriteByte(byte(c.Kind))
		_ = w.WriteByte(byte(c.Codec))
		_ = w.WriteByte(byte(c.Compress))

		var flags byte
		if c.Const {
			flags |= flagConst
		}

		if c.FloatPrecisionBits != 0 {
			flags |= flagLossy
		}

		_ = w.WriteByte(flags)

		if flags&flagLossy != 0 {
			_ = w.WriteByte(c.FloatPrecisionBits)
		}

		switch c.Kind {
		case KindInt64:
			w.WriteVarint(c.MinInt64)
			w.WriteVarint(c.MaxInt64)

			if c.Const {
				w.WriteVarint(c.ConstInt64)
			}
		case KindFloat64:
			putU64(w, math.Float64bits(c.MinFloat64))
			putU64(w, math.Float64bits(c.MaxFloat64))

			if c.Const {
				putU64(w, math.Float64bits(c.ConstFloat64))
			}
		case KindBytes:
			if c.Const {
				w.WriteUvarint(uint64(len(c.ConstBytes)))
				w.WriteBytes(c.ConstBytes)
			}
		case KindInt128:
			// No min/max or constant value: id columns are never constant-collapsed.
		}
	}

	w.PadToByte()
	out := w.Bytes()
	crc := crc32.Checksum(out[start:], castagnoli)

	return binary.BigEndian.AppendUint32(out, crc)
}

// DecodeManifest parses a manifest object. It verifies the CRC and bounds-checks every
// field, returning an [ErrCorrupt]-wrapping error on any malformed input — it never
// panics, so it is safe to fuzz on arbitrary bytes.
func DecodeManifest(src []byte) (Manifest, error) {
	if len(src) < 4 {
		return Manifest{}, errors.Wrap(ErrCorrupt, "too short for CRC")
	}

	body := src[:len(src)-4]
	want := binary.BigEndian.Uint32(src[len(src)-4:])

	if crc32.Checksum(body, castagnoli) != want {
		return Manifest{}, errors.Wrap(ErrCorrupt, "CRC mismatch")
	}

	r := bitstream.NewReader(body)

	magic, err := getU32(r)
	if err != nil || magic != manifestMagic {
		return Manifest{}, errors.Wrap(ErrCorrupt, "bad magic")
	}

	var m Manifest

	version, err := r.ReadUvarint()
	if err != nil {
		return Manifest{}, errors.Wrap(ErrCorrupt, "version")
	}

	m.Version = uint32(version)
	if m.Version != manifestVersion {
		return Manifest{}, errors.Wrapf(ErrCorrupt, "unsupported version %d", m.Version)
	}

	rowCount, err := r.ReadUvarint()
	if err != nil {
		return Manifest{}, errors.Wrap(ErrCorrupt, "rowCount")
	}

	m.RowCount = int(rowCount)

	if m.MinTime, err = r.ReadVarint(); err != nil {
		return Manifest{}, errors.Wrap(ErrCorrupt, "minTime")
	}

	if m.MaxTime, err = r.ReadVarint(); err != nil {
		return Manifest{}, errors.Wrap(ErrCorrupt, "maxTime")
	}

	granuleSize, err := r.ReadUvarint()
	if err != nil {
		return Manifest{}, errors.Wrap(ErrCorrupt, "granuleSize")
	}

	m.GranuleSize = int(granuleSize)

	colCount, err := r.ReadUvarint()
	if err != nil {
		return Manifest{}, errors.Wrap(ErrCorrupt, "colCount")
	}
	// Guard against a huge count driving an OOM allocation: each column needs several
	// bytes, so it cannot exceed the body length.
	if colCount > uint64(len(body)) {
		return Manifest{}, errors.Wrapf(ErrCorrupt, "colCount %d exceeds body", colCount)
	}

	m.Columns = make([]ColumnDesc, 0, colCount)
	for i := range colCount {
		c, err := decodeColumnDesc(r)
		if err != nil {
			return Manifest{}, errors.Wrapf(err, "column %d", i)
		}

		m.Columns = append(m.Columns, c)
	}

	return m, nil
}

func decodeColumnDesc(r *bitstream.Reader) (ColumnDesc, error) {
	var c ColumnDesc

	nameLen, err := r.ReadUvarint()
	if err != nil {
		return c, errors.Wrap(ErrCorrupt, "nameLen")
	}

	name, err := r.ReadBytesView(int(nameLen))
	if err != nil {
		return c, errors.Wrap(ErrCorrupt, "name")
	}

	c.Name = string(name)

	kind, err := r.ReadByte()
	if err != nil {
		return c, errors.Wrap(ErrCorrupt, "kind")
	}

	c.Kind = Kind(kind)
	if !c.Kind.valid() {
		return c, errors.Wrapf(ErrCorrupt, "bad kind %d", kind)
	}

	codec, err := r.ReadByte()
	if err != nil {
		return c, errors.Wrap(ErrCorrupt, "codec")
	}

	c.Codec = chunk.Codec(codec)

	comp, err := r.ReadByte()
	if err != nil {
		return c, errors.Wrap(ErrCorrupt, "compress")
	}

	c.Compress = compress.Algorithm(comp)

	flags, err := r.ReadByte()
	if err != nil {
		return c, errors.Wrap(ErrCorrupt, "flags")
	}

	c.Const = flags&flagConst != 0

	if flags&flagLossy != 0 {
		bits, err := r.ReadByte()
		if err != nil {
			return c, errors.Wrap(ErrCorrupt, "precisionBits")
		}

		c.FloatPrecisionBits = bits
	}

	switch c.Kind {
	case KindInt64:
		err = decodeInt64Col(r, &c)
	case KindFloat64:
		err = decodeFloat64Col(r, &c)
	case KindBytes:
		err = decodeBytesCol(r, &c)
	case KindInt128:
		// No per-column metadata: id columns carry no min/max or constant value.
	}

	if err != nil {
		return c, err
	}

	return c, nil
}

func decodeInt64Col(r *bitstream.Reader, c *ColumnDesc) error {
	var err error
	if c.MinInt64, err = r.ReadVarint(); err != nil {
		return errors.Wrap(ErrCorrupt, "minInt64")
	}

	if c.MaxInt64, err = r.ReadVarint(); err != nil {
		return errors.Wrap(ErrCorrupt, "maxInt64")
	}

	if c.Const {
		if c.ConstInt64, err = r.ReadVarint(); err != nil {
			return errors.Wrap(ErrCorrupt, "constInt64")
		}
	}

	return nil
}

func decodeFloat64Col(r *bitstream.Reader, c *ColumnDesc) error {
	minBits, err := getU64(r)
	if err != nil {
		return errors.Wrap(ErrCorrupt, "minFloat64")
	}

	maxBits, err := getU64(r)
	if err != nil {
		return errors.Wrap(ErrCorrupt, "maxFloat64")
	}

	c.MinFloat64 = math.Float64frombits(minBits)
	c.MaxFloat64 = math.Float64frombits(maxBits)

	if c.Const {
		cb, err := getU64(r)
		if err != nil {
			return errors.Wrap(ErrCorrupt, "constFloat64")
		}

		c.ConstFloat64 = math.Float64frombits(cb)
	}

	return nil
}

func decodeBytesCol(r *bitstream.Reader, c *ColumnDesc) error {
	if !c.Const {
		return nil
	}

	n, err := r.ReadUvarint()
	if err != nil {
		return errors.Wrap(ErrCorrupt, "constBytesLen")
	}

	view, err := r.ReadBytesView(int(n))
	if err != nil {
		return errors.Wrap(ErrCorrupt, "constBytes")
	}

	c.ConstBytes = append([]byte(nil), view...)

	return nil
}

// putU32 / getU32 / putU64 / getU64 read and write big-endian fixed-width integers in
// the byte-aligned bitstream (used for magic and float bit patterns).
func putU32(w *bitstream.Writer, v uint32) {
	binary.BigEndian.PutUint32(w.AppendBytes(4), v)
}

func getU32(r *bitstream.Reader) (uint32, error) {
	b, err := r.ReadBytesView(4)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(b), nil
}

func putU64(w *bitstream.Writer, v uint64) {
	binary.BigEndian.PutUint64(w.AppendBytes(8), v)
}

func getU64(r *bitstream.Reader) (uint64, error) {
	b, err := r.ReadBytesView(8)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(b), nil
}
