package block

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/bitstream"
)

// Granule is one entry of the sparse mark index: the first row of a fixed-size run of
// rows, plus the min/max of the part's sort-key column over that run. The min/max let a
// query prune whole granules whose key range cannot intersect its window
// (_ref/docs/storage-engine.md §2, ClickHouse marks / Parquet row-group stats).
type Granule struct {
	FirstRow int
	MinKey   int64
	MaxKey   int64
}

// Marks is the sparse granule index for a part: granules of GranuleSize rows over the
// sort-key column (timestamp for the metrics vertical). It is serialized to the
// `{prefix}/marks` object.
type Marks struct {
	GranuleSize int
	Granules    []Granule
}

const (
	marksMagic   uint32 = 0x4F544D4B // "OTMK" (OTel part MarKs)
	marksVersion uint32 = 1
)

// BuildMarks computes the sparse index over a sort-key column, chunked into granules of
// granuleSize rows. The min/max are computed by scanning each granule, so correctness
// does not depend on sortKey being sorted (though for the metrics vertical it is).
// granuleSize must be > 0.
func BuildMarks(sortKey []int64, granuleSize int) Marks {
	if granuleSize <= 0 {
		panic("block: granuleSize must be > 0")
	}

	m := Marks{GranuleSize: granuleSize}
	for start := 0; start < len(sortKey); start += granuleSize {
		end := min(start+granuleSize, len(sortKey))

		lo, hi := sortKey[start], sortKey[start]
		for _, v := range sortKey[start+1 : end] {
			if v < lo {
				lo = v
			}

			if v > hi {
				hi = v
			}
		}

		m.Granules = append(m.Granules, Granule{FirstRow: start, MinKey: lo, MaxKey: hi})
	}

	return m
}

// Overlapping returns the granules whose [MinKey, MaxKey] range intersects the inclusive
// window [lo, hi]. It is the pruning primitive the fetcher (M3) uses to skip granules.
func (m Marks) Overlapping(lo, hi int64) []Granule {
	var out []Granule

	for _, g := range m.Granules {
		if g.MaxKey >= lo && g.MinKey <= hi {
			out = append(out, g)
		}
	}

	return out
}

// Encode appends the binary marks index to dst. Layout:
//
//	[u32 magic][uvarint version][uvarint granuleSize][uvarint count]
//	  per granule: [uvarint firstRow][varint minKey-prevMinKey][varint maxKey-minKey]
//	[u32 CRC32C]
//
// Keys are delta-encoded across granules (sort key is non-decreasing) for compactness.
func (m Marks) Encode(dst []byte) []byte {
	start := len(dst)

	w := bitstream.NewWriter(dst)
	putU32(w, marksMagic)
	w.WriteUvarint(uint64(marksVersion))
	w.WriteUvarint(uint64(m.GranuleSize))
	w.WriteUvarint(uint64(len(m.Granules)))

	var prevMin int64
	for _, g := range m.Granules {
		w.WriteUvarint(uint64(g.FirstRow))
		w.WriteVarint(g.MinKey - prevMin)
		w.WriteVarint(g.MaxKey - g.MinKey)
		prevMin = g.MinKey
	}

	w.PadToByte()
	out := w.Bytes()
	crc := crc32.Checksum(out[start:], castagnoli)

	return binary.BigEndian.AppendUint32(out, crc)
}

// DecodeMarks parses a marks object. It verifies the CRC and bounds-checks every field,
// returning an [ErrCorrupt]-wrapping error on malformed input; it never panics.
func DecodeMarks(src []byte) (Marks, error) {
	if len(src) < 4 {
		return Marks{}, errors.Wrap(ErrCorrupt, "marks too short for CRC")
	}

	body := src[:len(src)-4]
	if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(src[len(src)-4:]) {
		return Marks{}, errors.Wrap(ErrCorrupt, "marks CRC mismatch")
	}

	r := bitstream.NewReader(body)

	magic, err := getU32(r)
	if err != nil || magic != marksMagic {
		return Marks{}, errors.Wrap(ErrCorrupt, "marks bad magic")
	}

	version, err := r.ReadUvarint()
	if err != nil || version != uint64(marksVersion) {
		return Marks{}, errors.Wrap(ErrCorrupt, "marks version")
	}

	granuleSize, err := r.ReadUvarint()
	if err != nil {
		return Marks{}, errors.Wrap(ErrCorrupt, "marks granuleSize")
	}

	count, err := r.ReadUvarint()
	if err != nil {
		return Marks{}, errors.Wrap(ErrCorrupt, "marks count")
	}

	if count > uint64(len(body)) {
		return Marks{}, errors.Wrapf(ErrCorrupt, "marks count %d exceeds body", count)
	}

	m := Marks{GranuleSize: int(granuleSize), Granules: make([]Granule, 0, count)}

	var prevMin int64

	for i := range count {
		firstRow, err := r.ReadUvarint()
		if err != nil {
			return Marks{}, errors.Wrapf(ErrCorrupt, "granule %d firstRow", i)
		}

		minDelta, err := r.ReadVarint()
		if err != nil {
			return Marks{}, errors.Wrapf(ErrCorrupt, "granule %d minKey", i)
		}

		span, err := r.ReadVarint()
		if err != nil {
			return Marks{}, errors.Wrapf(ErrCorrupt, "granule %d maxKey", i)
		}

		minKey := prevMin + minDelta
		m.Granules = append(m.Granules, Granule{
			FirstRow: int(firstRow),
			MinKey:   minKey,
			MaxKey:   minKey + span,
		})
		prevMin = minKey
	}

	return m, nil
}
