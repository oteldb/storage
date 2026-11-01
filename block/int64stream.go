package block

import (
	"context"
	"encoding/binary"
	"io"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

// Int64ScanBound bounds retained and transient memory for [Decoder.Int64Cursor], without reads.
func (r *PartReader) Int64ScanBound(ctx context.Context, name string) (retained, transient int64, ok bool) {
	i, ok := r.byName[name]
	if !ok {
		return 0, 0, false
	}
	desc := r.manifest.Columns[i]
	if desc.Kind != KindInt64 || desc.Const || !desc.Blocked || !desc.Framed || !desc.HasSizing || desc.SharedDict {
		return 0, 0, false
	}
	switch desc.Codec {
	case chunk.CodecDoD, chunk.CodecDoDScaled, chunk.CodecT64:
	default:
		return 0, 0, false
	}
	in, err := r.ColumnInputSize(name)
	if err != nil || in.WholeRead {
		return 0, 0, false
	}
	retained = satAdd(in.Resident, in.DirBytes)
	if backend.RangesNatively(ctx, r.b, columnKey(r.prefix, i)) {
		retained = satAdd(retained, in.MaxFrameBytes)
	} else {
		retained = satAdd(retained, desc.Bytes)
	}
	slack := int64(compress.OutputSlack(desc.Compress))
	retained = satAdd(retained, satMul(2, satAdd(in.MaxFrameRaw, slack)))
	rows := int64(r.manifest.RowCount)
	if granules := desc.Sizing.NumGranules; granules > 1 {
		rows = satAdd(rows/(granules-1), 1)
	}
	// T64's reused destination can double while it grows; an old cursor may retain its predecessor.
	retained = satAdd(retained, satMul(rows, 4*8))
	// Decoder, frame source and fixed-size numeric cursor state.
	retained = satAdd(retained, 4<<10)
	transient = int64(compress.DecodeWorkspace)
	if slack > 0 {
		scratch := satAdd(in.MaxFrameRaw, slack)
		switch {
		case scratch <= maxScratchBuf:
			scratch = maxScratchBuf
		case scratch <= maxLargeScratchBuf:
			scratch = maxLargeScratchBuf
		}
		transient = satAdd(transient, scratch)
	}
	transient = max(transient, satAdd(in.OpenBytes, dirProbeBytes+footerLenBytes))
	return retained, transient, true
}

// Int64StreamBound recovers [PartReader.Int64ScanBound] for ranged decoders; whole-object decoders are unsupported.
func (d *Decoder) Int64StreamBound() (retained, transient int64, ok bool) {
	if d.kind != KindInt64 || d.shared.on || d.streams.dir.src == nil || d.streams.dir.frameRaw == nil || len(d.streams.dir.frameOff) == 0 {
		return 0, 0, false
	}
	switch d.codec {
	case chunk.CodecDoD, chunk.CodecDoDScaled, chunk.CodecT64:
	default:
		return 0, 0, false
	}
	retained = satAdd(d.ResidentBytes(), satMul(int64(min(d.BlockRows(), d.rows)), 3*8))
	retained = satAdd(retained, 4<<10)
	transient = int64(compress.DecodeWorkspace)
	if slack := int64(d.streams.comp.OutputSlack()); slack > 0 {
		var raw int64
		for _, n := range d.streams.dir.frameRaw {
			raw = max(raw, int64(n))
		}
		scratch := satAdd(raw, slack)
		switch {
		case scratch <= maxScratchBuf:
			scratch = maxScratchBuf
		case scratch <= maxLargeScratchBuf:
			scratch = maxLargeScratchBuf
		}
		transient = satAdd(transient, scratch)
	}
	dir := d.streams.dir
	encodedDir := satAdd(dirProbeBytes+footerLenBytes, satMul(int64(len(dir.frameOff)-1), 2*binary.MaxVarintLen64+objectCRCBytes))
	encodedDir = satAdd(encodedDir, satMul(int64(dir.granules), binary.MaxVarintLen64))
	transient = max(transient, encodedDir)
	if src := dir.src; src != nil && !backend.RangesNatively(src.ctx, src.b, src.key) {
		whole := satAdd(src.base, int64(dir.frameOff[len(dir.frameOff)-1]))
		whole = satAdd(whole, encodedDir)
		transient = max(transient, satAdd(satMul(whole, 2), int64(compress.DecodeWorkspace)))
	}
	return retained, transient, true
}

// Int64Cursor reads one granule; its bytes remain valid until the decoder opens another granule.
func (d *Decoder) Int64Cursor(blk int) (chunk.TsCursor, error) {
	if d.kind != KindInt64 {
		return nil, errors.Errorf("block: column is %s, not int64", d.kind)
	}
	if blk < 0 || blk >= d.NumBlocks() {
		return nil, errors.Wrapf(ErrCorrupt, "block %d outside [0,%d)", blk, d.NumBlocks())
	}
	stream, err := d.streams.granule(blk)
	if err != nil {
		return nil, err
	}
	rows := blockRowsAt(d.streams.dir, d.rows, blk)
	if d.codec == chunk.CodecT64 {
		d.ints, _, err = chunk.DecodeIntsT64(d.ints[:0], stream, rows)
		if err != nil {
			return nil, errors.Wrap(ErrCorrupt, err.Error())
		}
		return &int64SliceCursor{values: d.ints}, nil
	}
	cur, err := chunk.NewTsCursor(d.codec, stream)
	if err != nil {
		return nil, errors.Wrap(ErrCorrupt, err.Error())
	}
	if cur.Len() != rows {
		return nil, errors.Wrapf(ErrCorrupt, "block %d decoded rows %d, want %d", blk, cur.Len(), rows)
	}
	return cur, nil
}

type int64SliceCursor struct {
	values []int64
	pos    int
}

func (c *int64SliceCursor) Len() int { return len(c.values) }
func (c *int64SliceCursor) Pos() int { return c.pos }
func (c *int64SliceCursor) Next() (int64, error) {
	if c.pos >= len(c.values) {
		return 0, io.EOF
	}
	v := c.values[c.pos]
	c.pos++
	return v, nil
}
