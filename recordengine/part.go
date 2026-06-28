package recordengine

import (
	"context"
	"sync/atomic"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/signal"
)

func idToU128(id signal.SeriesID) chunk.U128 { return chunk.U128{Hi: id.Hi, Lo: id.Lo} }
func u128ToID(u chunk.U128) signal.SeriesID  { return signal.SeriesID{Hi: u.Hi, Lo: u.Lo} }

// rowRange is the half-open row span [start, end) a stream occupies in a part.
type rowRange struct{ start, end int }

// part is a flushed, immutable part: the lazy on-backend [block.PartReader], an in-memory
// StreamID → row-range index (rows are sorted by (stream, ts), so each stream is one contiguous
// run), and the per-column blooms for predicate pruning.
type part struct {
	schema *Schema
	reader *block.PartReader
	prefix string
	ranges map[signal.SeriesID]rowRange
	blooms map[string]*bloom.Filter // column name → its bloom (FullText/Attrs/Equality); absent ⇒ scan

	// recordKeys is the part's distinct per-record attribute keys (the "keys.bin" footer), for
	// [Engine.Keys] enumeration. nil when the part has no record attributes (or predates the footer).
	recordKeys [][]byte

	// minTime, maxTime are the inclusive unix-ns record bounds of the part (from the columns when
	// written, from the bucket index when reconstructed), for time pruning.
	minTime, maxTime int64

	// refs counts in-flight fetches reading this part lock-free. A fetch acquires (under the engine
	// lock, while the part is still live) the parts it will read, releases them when done, and reads
	// the backend objects between. A retired part (removed from the live set by flush/merge) is not
	// deleted from the backend until its refs reach zero, so a lock-free reader never races a delete.
	refs atomic.Int32
}

func (p *part) acquire() { p.refs.Add(1) }
func (p *part) release() { p.refs.Add(-1) }

// deletePart removes every backend object of the part at prefix.
func deletePart(ctx context.Context, b backend.Backend, prefix string) error {
	keys, err := b.List(ctx, prefix)
	if err != nil {
		return err
	}

	for _, k := range keys {
		if err := b.Delete(ctx, k); err != nil {
			return err
		}
	}

	return nil
}

// openPart opens the part at prefix and builds its StreamID → row-range index and bloom set.
func openPart(ctx context.Context, b backend.Backend, schema *Schema, prefix string) (*part, error) {
	r, err := block.OpenPart(ctx, b, prefix)
	if err != nil {
		return nil, err
	}

	col, err := r.Column(ctx, colStream)
	if err != nil {
		return nil, err
	}

	ids, err := col.ID128(nil)
	if err != nil {
		return nil, err
	}

	ranges := make(map[signal.SeriesID]rowRange)

	for i := 0; i < len(ids); {
		j := i + 1
		for j < len(ids) && ids[j] == ids[i] {
			j++
		}

		ranges[u128ToID(ids[i])] = rowRange{start: i, end: j}
		i = j
	}

	blooms, err := loadBlooms(ctx, b, schema, prefix)
	if err != nil {
		return nil, err
	}

	recordKeys, err := loadRecordKeys(ctx, b, prefix)
	if err != nil {
		return nil, err
	}

	return &part{schema: schema, reader: r, prefix: prefix, ranges: ranges, blooms: blooms, recordKeys: recordKeys}, nil
}

// holdsAny reports whether the part carries any of the requested streams.
func (p *part) holdsAny(ids []signal.SeriesID) bool {
	for _, id := range ids {
		if _, ok := p.ranges[id]; ok {
			return true
		}
	}

	return false
}

// appendWindow appends stream id's records whose timestamp is in [start, end] to acc, decoding the
// full column set (used by merge, which rewrites every column). No-op if the part lacks the stream.
func (p *part) appendWindow(ctx context.Context, id signal.SeriesID, acc *recordCols, start, end int64) error {
	rng, ok := p.ranges[id]
	if !ok {
		return nil
	}

	cols, err := p.readCols(ctx, fullSel(p.schema), nil)
	if err != nil {
		return err
	}

	for i := rng.start; i < rng.end; i++ {
		if cols.ts[i] >= start && cols.ts[i] <= end {
			acc.appendRow(cols, i)
		}
	}

	return nil
}

// readCols decodes the part's timestamp column plus the schema columns selected by sel (unselected
// stay nil — lazy decode). Returned byte slices are freshly decoded (owned by the caller). getI64,
// when non-nil, supplies reusable int-column scratch from a pool (the fetch path, whose decoded int
// columns are copied out and then recycled by [Engine.recycleDecodeInts]); pass nil to decode into
// fresh slices (the merge path, which has no recycle point).
func (p *part) readCols(ctx context.Context, sel colSel, getI64 func() []int64) (*recordCols, error) {
	c := &recordCols{schema: p.schema, sel: sel, ints: make([][]int64, p.schema.numInts()), bytes: make([]byteCol, p.schema.numBytes())}

	dst := func() []int64 {
		if getI64 != nil {
			return getI64()
		}

		return nil
	}

	var err error
	if c.ts, err = p.readInt64(ctx, colTs, dst()); err != nil {
		return nil, err
	}

	for k := range c.ints {
		if sel.ints[k] {
			if c.ints[k], err = p.readInt64(ctx, p.schema.intColumn(k).Name, dst()); err != nil {
				return nil, err
			}
		}
	}

	for k := range c.bytes {
		if sel.bytes[k] {
			if c.bytes[k], err = p.readBytes(ctx, p.schema.byteColumn(k).Name); err != nil {
				return nil, err
			}
		}
	}

	return c, nil
}

func (p *part) readInt64(ctx context.Context, name string, dst []int64) ([]int64, error) {
	col, err := p.reader.Column(ctx, name)
	if err != nil {
		return nil, err
	}

	return col.Int64(dst)
}

// readBytes decodes the named byte column into the contiguous offsets+blob [byteCol] layout,
// concatenating the per-row cells (which the dictionary decoder returns as views into its shared
// entries) into one owned blob so the fetch/scan path reads cells with locality and the GC scans two
// slice headers per column instead of one per row.
func (p *part) readBytes(ctx context.Context, name string) (byteCol, error) {
	col, err := p.reader.Column(ctx, name)
	if err != nil {
		return byteCol{}, err
	}

	dc, err := col.Bytes()
	if err != nil {
		return byteCol{}, err
	}

	n := dc.Len()
	out := byteCol{offsets: make([]int32, 1, n+1)}
	for i := range n {
		out.appendCell(dc.At(i))
	}

	return out, nil
}
