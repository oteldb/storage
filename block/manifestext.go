package block

import (
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/bitstream"
	"github.com/oteldb/storage/encoding/chunk"
)

// sharedEntriesCeiling is the format bound on a shared dictionary's entry count: what a 2-byte id
// addresses.
const sharedEntriesCeiling = 1 << 16

// decodeColumnExt reads and bounds the fields the xflags byte x gates. Every value is checked as a
// uint64, by comparison and subtraction only, before it is converted: a CRC-valid manifest reaches
// here with any bytes a writer never produced.
func decodeColumnExt(r *bitstream.Reader, c *ColumnDesc, x byte, version uint32, rows uint64) error {
	if x&xTrailerDict != 0 {
		var v [4]uint64

		for i := range v {
			var err error
			if v[i], err = r.ReadUvarint(); err != nil {
				return errors.Wrap(ErrCorrupt, "trailer dictionary field")
			}
		}

		if err := c.setTrailerDict(v[0], v[1], v[2], v[3]); err != nil {
			return errors.Wrapf(err, "column %q", c.Name)
		}
	}

	if x&xSizing != 0 {
		var v [7]uint64

		for i := range v {
			var err error
			if v[i], err = r.ReadUvarint(); err != nil {
				return errors.Wrap(ErrCorrupt, "sizing field")
			}
		}

		if err := c.setSizing(v, rows); err != nil {
			return errors.Wrapf(err, "column %q", c.Name)
		}
	}

	// Only a trailer column ever pairs a shared dictionary with a footer directory.
	if version >= manifestVersionExt && c.SharedDict && c.Footer && !c.TrailerDict {
		return errors.Wrapf(ErrCorrupt, "column %q: a footer shared dictionary without a trailer", c.Name)
	}

	return nil
}

func (c *ColumnDesc) setTrailerDict(off, dictLen, raw, entries uint64) error {
	if c.Kind != KindBytes || c.Codec != chunk.CodecDict || !c.Blocked || !c.Framed || !c.Footer ||
		!c.SharedDict || c.Bytes <= 0 || c.Const {
		return errors.Wrap(ErrCorrupt, "trailer dictionary on a column that cannot carry one")
	}

	size := uint64(c.Bytes)

	switch {
	case entries > sharedEntriesCeiling:
		return errors.Wrapf(ErrCorrupt, "trailer dictionary of %d entries", entries)
	case raw > maxSharedDictRaw:
		return errors.Wrapf(ErrCorrupt, "trailer dictionary of %d bytes", raw)
	case entries > raw:
		return errors.Wrapf(ErrCorrupt, "%d entries in %d dictionary bytes", entries, raw)
	case off > size, dictLen > size-off, size-off-dictLen < footerLenBytes:
		return errors.Wrapf(ErrCorrupt, "trailer dictionary [%d,+%d) past object %d", off, dictLen, size)
	case dictLen < minDictRegion:
		return errors.Wrapf(ErrCorrupt, "trailer dictionary region of %d bytes", dictLen)
	case dictLen > maxDictRegion(entries, raw):
		return errors.Wrapf(ErrCorrupt, "trailer dictionary region of %d bytes for %d raw", dictLen, raw)
	}

	c.TrailerDict = true
	c.DictOff, c.DictLen, c.DictRaw, c.DictEntries = int64(off), int64(dictLen), int64(raw), int64(entries)

	return nil
}

// minDictRegion is the smallest dictionary region: one byte per uvarint, the compressor's flag byte
// alone (an empty dictionary), and the checksum.
const minDictRegion = 1 + 1 + 1 + objectCRCBytes

// maxDictRegion is the largest region a writer produces for raw dictionary bytes: compression
// never grows its input by more than the flag byte.
func maxDictRegion(entries, raw uint64) uint64 {
	return uint64(varintLen(entries)+varintLen(raw+1)) + raw + 1 + objectCRCBytes
}

func (c *ColumnDesc) setSizing(v [7]uint64, rows uint64) error {
	if c.Const || c.Bytes <= 0 {
		return errors.Wrap(ErrCorrupt, "sizing on a column without an object")
	}

	for _, f := range v {
		if f > math.MaxInt64 {
			return errors.Wrapf(ErrCorrupt, "sizing field %d exceeds MaxInt64", f)
		}
	}

	var s ColumnSizing
	for i, p := range s.fields() {
		*p = int64(v[i])
	}

	if err := checkSizing(c, s, rows); err != nil {
		return err
	}

	c.HasSizing, c.Sizing = true, s

	return nil
}

func checkSizing(c *ColumnDesc, s ColumnSizing, rows uint64) error {
	if !c.Blocked {
		if s != (ColumnSizing{StreamRaw: s.StreamRaw}) {
			return errors.Wrap(ErrCorrupt, "directory sizing on an unframed column")
		}

		return nil
	}

	if !c.Framed {
		return errors.Wrap(ErrCorrupt, "sizing on the legacy blocked layout")
	}

	dirMax := c.Bytes
	if c.Footer {
		dirMax -= footerLenBytes
	}

	switch {
	case c.TrailerDict && s.DirLen != c.Bytes-c.DictOff-c.DictLen-footerLenBytes:
		return errors.Wrapf(ErrCorrupt, "directory of %d bytes, the trailer leaves %d",
			s.DirLen, c.Bytes-c.DictOff-c.DictLen-footerLenBytes)
	case s.DirLen > dirMax:
		return errors.Wrapf(ErrCorrupt, "directory of %d bytes in an object of %d", s.DirLen, c.Bytes)
	case s.NumFrames > s.NumGranules, uint64(s.NumGranules) > rows, s.NumGranules > s.DirLen:
		return errors.Wrapf(ErrCorrupt, "%d frames, %d granules for %d rows in a %d-byte directory",
			s.NumFrames, s.NumGranules, rows, s.DirLen)
	case s.MaxFrameBytes > c.Bytes:
		return errors.Wrapf(ErrCorrupt, "frame of %d bytes in an object of %d", s.MaxFrameBytes, c.Bytes)
	case s.MaxGranuleRaw > s.MaxFrameRaw, s.MaxFrameRaw > math.MaxInt32:
		return errors.Wrapf(ErrCorrupt, "granule of %d bytes, frame of %d", s.MaxGranuleRaw, s.MaxFrameRaw)
	}

	return nil
}
