package block

import (
	"context"
	"unsafe"
)

// Clone shares immutable metadata and creates independent decode state. Frame reads use ctx.
func (d *Decoder) Clone(ctx context.Context) *Decoder {
	dir := d.streams.dir
	if src := dir.src; src != nil {
		dir.src = &frameSource{
			ctx: ctx, b: src.b, key: src.key, base: src.base,
			window: src.window, frameOff: src.frameOff,
		}
	}
	return &Decoder{
		rows: d.rows, kind: d.kind, codec: d.codec, i64: d.i64, f64: d.f64,
		shared: d.shared, granules: dictOwner{framed: d.granules.framed},
		streams: newBlockStreams(dir, d.streams.comp),
	}
}

// MetadataBytes estimates retained directory and dictionary bytes, excluding decode scratch.
func (d *Decoder) MetadataBytes() int64 {
	dir := &d.streams.dir
	n := int64(unsafe.Sizeof(*d)) + dir.residentBytes() + d.shared.residentBytes() + int64(len(dir.data))
	n += int64(len(dir.col))
	if src := dir.src; src != nil {
		n += int64(unsafe.Sizeof(*src)) + int64(len(src.key))
	}
	return n
}
