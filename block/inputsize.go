package block

import (
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/compress"
)

// ColumnInputSize is what reading one column of a part holds, derived from the manifest alone so a
// merge can be sized before it reads anything. Fields saturate at MaxInt64; a part whose manifest
// cannot bound its columns reports a saturated Resident and so never fits a budget.
type ColumnInputSize struct {
	// Resident is held for the whole walk: a shared dictionary, or the column decoded when it is
	// read whole.
	Resident      int64
	DictEntries   int64
	MaxFrameBytes int64
	MaxFrameRaw   int64
	MaxGranuleRaw int64
	// DirBytes is the parsed directory.
	DirBytes int64
	// OpenBytes is the encoded bytes held while the column opens.
	OpenBytes int64
	// WholeResident is the decoded footprint of the column read whole.
	WholeResident int64
	// WholeRead reports that the column can only be read whole: it is unframed, or its manifest
	// records no sizing.
	WholeRead bool
	// SourceWide reports that Resident and WholeResident charge the whole part rather than this
	// column, so a caller charges them once per part: a part without sizing is bounded only by its
	// RawBytes.
	SourceWide bool
}

// Footprints of a decoded column beyond its stream: an int64/float64 row with its stream slack; a
// parsed dictionary entry's header; and a whole-read bytes column's merged entry header, up to one
// per row and per dictionary entry and doubled by append growth, plus a row's 2-byte id.
const (
	decodedRowBytes    = 16
	decodedEntryHeader = 24
	mergedEntryHeader  = 2 * decodedEntryHeader
	decodedBytesRow    = mergedEntryHeader + 2
)

// ColumnInputSize sizes the named column from the manifest. It reads nothing.
func (r *PartReader) ColumnInputSize(name string) (ColumnInputSize, error) {
	i, ok := r.byName[name]
	if !ok {
		return ColumnInputSize{}, errors.Errorf("block: no column %q", name)
	}

	desc := r.manifest.Columns[i]

	switch {
	case desc.Const:
		return ColumnInputSize{}, nil
	case desc.HasSizing:
		return sizedInput(desc, int64(r.manifest.RowCount)), nil
	default:
		return r.sourceWideInput(desc), nil
	}
}

func sizedInput(desc ColumnDesc, rows int64) ColumnInputSize {
	s := desc.Sizing
	slack := int64(compress.OutputSlack(desc.Compress))

	in := ColumnInputSize{DictEntries: desc.DictEntries}

	var dict int64
	if desc.TrailerDict {
		dict = satAdd(desc.DictRaw, satMul(decodedEntryHeader, desc.DictEntries))
	}

	in.WholeResident = satAdd(s.StreamRaw, slack)
	if desc.Kind == KindBytes {
		in.WholeResident = satAdd(satAdd(in.WholeResident, dict), satMul(mergedEntryHeader, desc.DictEntries))
		in.WholeResident = satAdd(in.WholeResident, satMul(decodedBytesRow, rows))
	} else {
		in.WholeResident = satAdd(in.WholeResident, satMul(decodedRowBytes, rows))
	}

	if !desc.Blocked {
		in.WholeRead, in.Resident, in.OpenBytes = true, in.WholeResident, desc.Bytes

		return in
	}

	in.Resident = dict
	in.MaxFrameBytes, in.MaxFrameRaw, in.MaxGranuleRaw = s.MaxFrameBytes, s.MaxFrameRaw, s.MaxGranuleRaw

	// frameOff, frameRaw and (checked) frameCRC per frame, gFrame/gOff/gLen per granule: as
	// [blockDir.residentBytes] counts them.
	perFrame := int64(8)
	if desc.Checked {
		perFrame += 4
	}

	in.DirBytes = satAdd(satAdd(4, satMul(perFrame, s.NumFrames)), satMul(12, s.NumGranules))

	switch {
	case desc.TrailerDict:
		in.OpenBytes = desc.Bytes - desc.DictOff
	case desc.Footer:
		in.OpenBytes = s.DirLen + footerLenBytes
	default:
		in.OpenBytes = s.DirLen
	}

	return in
}

// sourceWideInput charges a column without sizing as a whole read of its whole part. RawBytes counts
// twice: once for the decoded values, once for the decompressed stream of the column being decoded,
// which for a numeric column is a second copy. Each column adds its stream's per-row and
// per-granule slack and, for bytes, its dictionary and merged entry headers at one per row. A part
// that records no RawBytes, or no object size for some column, cannot be bounded.
func (r *PartReader) sourceWideInput(desc ColumnDesc) ColumnInputSize {
	in := ColumnInputSize{WholeRead: true, SourceWide: true, OpenBytes: desc.Bytes}

	rows := int64(r.manifest.RowCount)
	granules := int64(1)

	if g := int64(r.manifest.GranuleSize); g > 0 {
		granules += (rows + g - 1) / g
	}

	charge := satMul(2, r.manifest.RawBytes)
	unbounded := charge <= 0 || desc.Bytes <= 0

	for i := range r.manifest.Columns {
		c := &r.manifest.Columns[i]
		if c.Const {
			continue
		}

		if c.Bytes <= 0 {
			unbounded = true
		}

		perRow := int64(streamRowSlack)
		if c.Kind == KindBytes {
			perRow += decodedEntryHeader + decodedBytesRow
		}

		// Every stream's fixed overhead, and the output slack a whole-column bytes walk keeps.
		overhead := satAdd(satMul(streamSlack, granules), int64(compress.OutputSlack(c.Compress)))
		charge = satAdd(charge, satAdd(satMul(perRow, rows), overhead))
	}

	if unbounded {
		charge = math.MaxInt64
	}

	in.Resident, in.WholeResident = charge, charge

	return in
}
