// Package ec erasure-codes flushed-part backend objects for shared-nothing durability at
// sub-replica storage cost: an object split into Data shards plus Parity parity shards
// (systematic Reed-Solomon) survives the loss of any Parity shards while storing only
// (Data+Parity)/Data of the logical bytes — e.g. {4,2} tolerates 2 losses at 1.5x versus 3x for
// RF=3 full copies. Each of Data+Parity cluster nodes holds one shard slot; any Data shards
// reconstruct the object.
//
// This package is the codec and framing layer only (issue 108, phase 2 milestone 1): shard
// encode/reconstruct/join over github.com/klauspost/reedsolomon (kept behind this package's
// small surface so the codec is swappable), plus the per-part Meta sidecar that records the
// scheme, each object's size, and per-shard checksums for fetch-time verification. Placement,
// the reconstructing read path, and repair live in later milestones.
package ec

import (
	"encoding/binary"

	"github.com/go-faster/errors"
	"github.com/klauspost/reedsolomon"
	"github.com/zeebo/xxh3"
)

// Scheme is a Reed-Solomon layout: Data data shards + Parity parity shards.
type Scheme struct {
	Data   int
	Parity int
}

// Validate reports whether the scheme is usable.
func (s Scheme) Validate() error {
	if s.Data < 1 || s.Parity < 1 {
		return errors.Errorf("ec: invalid scheme {%d,%d}: Data and Parity must be >= 1", s.Data, s.Parity)
	}

	if s.Data+s.Parity > 256 {
		return errors.Errorf("ec: invalid scheme {%d,%d}: at most 256 total shards", s.Data, s.Parity)
	}

	return nil
}

// Shards is the total shard count.
func (s Scheme) Shards() int { return s.Data + s.Parity }

// MinZones is the number of distinct failure domains (racks/zones) needed to place the shards
// so that losing any one zone costs at most Parity shards — i.e. the placement stays
// recoverable through a whole-rack failure. It is `ceil(Shards / Parity)`: with that many
// zones a balanced placement holds at most Parity shards per zone. Fewer zones cannot be made
// rack-safe for this scheme (a zone must then hold more than Parity shards).
func (s Scheme) MinZones() int {
	if s.Parity < 1 {
		return s.Shards()
	}

	return (s.Shards() + s.Parity - 1) / s.Parity
}

// encoderFor returns a reedsolomon encoder for the scheme. Encoders are stateless and cached
// internally by the library per (data,parity) via New's option defaults; construction is cheap
// relative to encoding, so no pool is kept here.
func encoderFor(s Scheme) (reedsolomon.Encoder, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	enc, err := reedsolomon.New(s.Data, s.Parity)
	if err != nil {
		return nil, errors.Wrap(err, "reedsolomon encoder")
	}

	return enc, nil
}

// Encode splits data into scheme.Data equal-size shards (zero-padded systematically: the
// original bytes are the concatenation of the data shards, truncated to len(data)) and computes
// scheme.Parity parity shards. The returned slice has scheme.Shards() entries. Empty data is
// valid (all shards empty).
func Encode(s Scheme, data []byte) ([][]byte, error) {
	enc, err := encoderFor(s)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		// reedsolomon rejects an empty split; an object of zero bytes still needs a well-formed
		// shard set so the framing stays uniform.
		return make([][]byte, s.Shards()), nil
	}

	shards, err := enc.Split(data)
	if err != nil {
		return nil, errors.Wrap(err, "split")
	}

	if err := enc.Encode(shards); err != nil {
		return nil, errors.Wrap(err, "encode parity")
	}

	return shards, nil
}

// Reconstruct fills the missing (nil) entries of shards in place. At least scheme.Data entries
// must be present; shards must have exactly scheme.Shards() entries in slot order.
func Reconstruct(s Scheme, shards [][]byte) error {
	if len(shards) != s.Shards() {
		return errors.Errorf("ec: got %d shards, scheme has %d slots", len(shards), s.Shards())
	}

	if allEmpty(shards) {
		return nil // the zero-byte object: nothing to rebuild
	}

	enc, err := encoderFor(s)
	if err != nil {
		return err
	}

	if err := enc.Reconstruct(shards); err != nil {
		return errors.Wrap(err, "reconstruct")
	}

	return nil
}

// Join reassembles the original object of size bytes from a complete shard set (all data slots
// present — run [Reconstruct] first if any are missing).
func Join(s Scheme, shards [][]byte, size int64) ([]byte, error) {
	if len(shards) != s.Shards() {
		return nil, errors.Errorf("ec: got %d shards, scheme has %d slots", len(shards), s.Shards())
	}

	if size == 0 {
		return []byte{}, nil
	}

	for i := range s.Data {
		if shards[i] == nil {
			return nil, errors.Errorf("ec: data shard %d missing (reconstruct first)", i)
		}
	}

	out := make([]byte, 0, size)
	for i := range s.Data {
		out = append(out, shards[i]...)
	}

	if int64(len(out)) < size {
		return nil, errors.Errorf("ec: shards hold %d bytes, object is %d", len(out), size)
	}

	return out[:size], nil
}

// allEmpty reports whether every shard is nil or zero-length.
func allEmpty(shards [][]byte) bool {
	for _, sh := range shards {
		if len(sh) > 0 {
			return false
		}
	}

	return true
}

// ObjectMeta describes one erasure-coded backend object within a part: its original size (Join
// needs it — shards are zero-padded) and the xxh3 checksum of each shard for fetch-time
// verification (a corrupt shard is detected before it poisons a reconstruction).
type ObjectMeta struct {
	// Name is the object's key relative to the part prefix (e.g. "c/0", "manifest").
	Name string
	// Size is the original object's byte size.
	Size int64
	// Checksums holds one xxh3 hash per shard slot.
	Checksums []uint64
}

// Meta is the per-part erasure-coding sidecar: the scheme and every coded object. It is
// written (like a manifest) after the part's shards, and read before any reconstruction.
type Meta struct {
	Scheme  Scheme
	Objects []ObjectMeta
}

// metaVersion is the Meta framing version byte.
const metaVersion = 1

// ChecksumShard returns the xxh3 checksum of one shard as stored in [ObjectMeta.Checksums].
func ChecksumShard(shard []byte) uint64 { return xxh3.Hash(shard) }

// AppendBinary appends the framed sidecar to dst and returns the extended slice.
func (m *Meta) AppendBinary(dst []byte) []byte {
	dst = append(dst, metaVersion)
	dst = binary.AppendUvarint(dst, uint64(m.Scheme.Data))
	dst = binary.AppendUvarint(dst, uint64(m.Scheme.Parity))
	dst = binary.AppendUvarint(dst, uint64(len(m.Objects)))

	for _, o := range m.Objects {
		dst = binary.AppendUvarint(dst, uint64(len(o.Name)))
		dst = append(dst, o.Name...)
		dst = binary.AppendUvarint(dst, uint64(o.Size))
		dst = binary.AppendUvarint(dst, uint64(len(o.Checksums)))

		for _, c := range o.Checksums {
			dst = binary.LittleEndian.AppendUint64(dst, c)
		}
	}

	sum := xxh3.Hash(dst)

	return binary.LittleEndian.AppendUint64(dst, sum)
}

// ErrCorrupt reports a Meta payload that fails structural or checksum validation.
var ErrCorrupt = errors.New("ec: corrupt meta")

// DecodeMeta parses an AppendBinary payload, defensively against truncation and corruption
// (trailing whole-payload checksum, bounded lengths).
func DecodeMeta(data []byte) (*Meta, error) {
	if len(data) < 8+1 {
		return nil, errors.Wrap(ErrCorrupt, "short payload")
	}

	body, tail := data[:len(data)-8], data[len(data)-8:]
	if xxh3.Hash(body) != binary.LittleEndian.Uint64(tail) {
		return nil, errors.Wrap(ErrCorrupt, "checksum mismatch")
	}

	if body[0] != metaVersion {
		return nil, errors.Wrapf(ErrCorrupt, "unknown version %d", body[0])
	}
	body = body[1:]

	readUvarint := func() (uint64, bool) {
		v, n := binary.Uvarint(body)
		if n <= 0 {
			return 0, false
		}
		body = body[n:]

		return v, true
	}

	var m Meta

	d, ok := readUvarint()
	if !ok {
		return nil, errors.Wrap(ErrCorrupt, "data shards")
	}

	p, ok := readUvarint()
	if !ok {
		return nil, errors.Wrap(ErrCorrupt, "parity shards")
	}

	m.Scheme = Scheme{Data: int(d), Parity: int(p)}
	if err := m.Scheme.Validate(); err != nil {
		return nil, errors.Wrap(ErrCorrupt, err.Error())
	}

	count, ok := readUvarint()
	if !ok || count > uint64(len(body)) { // each object needs >= 1 byte
		return nil, errors.Wrap(ErrCorrupt, "object count")
	}

	m.Objects = make([]ObjectMeta, 0, count)

	for range count {
		nameLen, ok := readUvarint()
		if !ok || nameLen > uint64(len(body)) {
			return nil, errors.Wrap(ErrCorrupt, "name length")
		}

		o := ObjectMeta{Name: string(body[:nameLen])}
		body = body[nameLen:]

		size, ok := readUvarint()
		if !ok {
			return nil, errors.Wrap(ErrCorrupt, "object size")
		}
		o.Size = int64(size)

		sums, ok := readUvarint()
		if !ok || sums > uint64(len(body)/8) || sums != uint64(m.Scheme.Shards()) {
			return nil, errors.Wrap(ErrCorrupt, "checksum count")
		}

		o.Checksums = make([]uint64, sums)
		for i := range o.Checksums {
			o.Checksums[i] = binary.LittleEndian.Uint64(body)
			body = body[8:]
		}

		m.Objects = append(m.Objects, o)
	}

	if len(body) != 0 {
		return nil, errors.Wrap(ErrCorrupt, "trailing bytes")
	}

	return &m, nil
}
