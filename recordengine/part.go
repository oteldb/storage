package recordengine

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/index/bloom"
	"github.com/oteldb/storage/signal"
)

func idToU128(id signal.SeriesID) chunk.U128 { return chunk.U128{Hi: id.Hi, Lo: id.Lo} }
func u128ToID(u chunk.U128) signal.SeriesID  { return signal.SeriesID{Hi: u.Hi, Lo: u.Lo} }

// rowRange is the half-open row span [start, end) a stream occupies in a part.
type rowRange struct{ start, end int }

// streamRange binds a stream id to the row span it occupies in a part.
type streamRange struct {
	rowRange

	id signal.SeriesID
}

// part is a flushed, immutable part: the lazy on-backend [block.PartReader], an in-memory
// StreamID → row-range index (rows are sorted by (stream, ts), so each stream is one contiguous
// run), and the per-column blooms for predicate pruning.
type part struct {
	schema *Schema
	reader *block.PartReader
	prefix string

	// ranges is the stream → row-span index, sorted by stream id, so a query resolves its streams by
	// a merge-join ([part.heldStreams]) rather than one hash probe per (part, stream) pair.
	ranges []streamRange
	blooms map[string]*bloom.Filter // column name → its bloom (FullText/Attrs/Equality); absent ⇒ scan

	// recordKeys is the part's distinct per-record attribute keys (the "keys.bin" footer), for
	// [Engine.Keys] enumeration. nil when the part has no record attributes (or predates the footer).
	recordKeys [][]byte

	// marksOnce lazily loads the sparse granule index on the first fetch that can prune with it;
	// granules is nil when the object is absent/corrupt or its granularity does not line up with the
	// part's framing, which leaves every granule a candidate. See granule.go.
	marksOnce sync.Once
	granules  []block.Granule

	// minTime, maxTime are the inclusive unix-ns record bounds of the part (from the columns when
	// written, from the bucket index when reconstructed), for time pruning.
	minTime, maxTime int64

	// blocks and level are the part's identity in block-number space, carried through the bucket
	// index. They are unset for a part written before that identity existed, which makes it neither
	// contain nor be contained by anything — see [bucketindex.Interval].
	blocks bucketindex.Interval
	level  uint32

	// pending is the identity a part this engine just wrote is waiting to be assigned; nil for one
	// opened from an index, whose identity is whatever that index recorded — including the unset
	// one of a part written before format v5. See blockid.go.
	pending *blockPlan

	// rawBytes is the part's decoded footprint per its manifest, 0 for a part written before the
	// manifest carried one. See [part.sizeBytes].
	rawBytes int64

	// refs counts in-flight fetches reading this part lock-free. A fetch acquires (under the engine
	// lock, while the part is still live) the parts it will read, releases them when done, and reads
	// the backend objects between. A retired part (removed from the live set by flush/merge) is not
	// deleted from the backend until its refs reach zero, so a lock-free reader never races a delete.
	refs atomic.Int32

	// streamMaxMu guards streamMax, the per-stream newest timestamps of [part.streamMaxTimes]. A
	// mutex rather than a sync.Once so a failed decode (a transient backend error) is retried
	// instead of being cached as "this part has no times".
	streamMaxMu sync.Mutex
	streamMax   []int64
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

	ranges := buildRanges(ids)

	blooms, err := loadBlooms(ctx, b, schema, prefix)
	if err != nil {
		return nil, err
	}

	recordKeys, err := loadRecordKeys(ctx, b, prefix)
	if err != nil {
		return nil, err
	}

	return &part{
		schema: schema, reader: r, prefix: prefix, ranges: ranges,
		blooms: blooms, recordKeys: recordKeys, rawBytes: r.Manifest().RawBytes,
	}, nil
}

// streamMaxTimes returns, aligned with p.ranges, the newest timestamp each stream has in this part:
// the per-stream durability watermark the replica refresh trims against. Decoded from the timestamp
// column once per part (parts are immutable) and kept as one int64 per stream, since the alternative
// — re-decoding on every refresh — repeats a whole-column read per part per tick.
func (p *part) streamMaxTimes(ctx context.Context) ([]int64, error) {
	p.streamMaxMu.Lock()
	defer p.streamMaxMu.Unlock()

	if p.streamMax != nil {
		return p.streamMax, nil
	}

	ts, err := p.readInt64(ctx, colTs, nil, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "read timestamps of part %q", p.prefix)
	}

	out := make([]int64, len(p.ranges))

	for i, sr := range p.ranges {
		newest := minInt64

		// Scanning the run rather than reading its last row: rows are (stream, ts)-ordered as
		// written, but the watermark decides what a replica may delete, so it does not rest on
		// an ordering the part itself does not enforce.
		for _, t := range ts[min(sr.start, len(ts)):min(sr.end, len(ts))] {
			newest = max(newest, t)
		}

		out[i] = newest
	}

	p.streamMax = out

	return out, nil
}

// buildRanges turns a part's stream column into the sorted stream → row-span index. A part written
// by flush or merge is already (stream, ts)-ordered, so the runs come out ascending; a part whose
// stream column is ordered otherwise is sorted here, once, rather than at every query.
func buildRanges(ids []chunk.U128) []streamRange {
	ranges := make([]streamRange, 0, 64)

	for i := 0; i < len(ids); {
		j := i + 1
		for j < len(ids) && ids[j] == ids[i] {
			j++
		}

		ranges = append(ranges, streamRange{id: u128ToID(ids[i]), rowRange: rowRange{start: i, end: j}})
		i = j
	}

	if !slices.IsSortedFunc(ranges, func(a, b streamRange) int { return a.id.Compare(b.id) }) {
		slices.SortFunc(ranges, func(a, b streamRange) int { return a.id.Compare(b.id) })
	}

	return ranges
}

// disjointIDs reports whether a sorted id set and a part's sorted stream list cannot overlap, so a
// fragmented store's many irrelevant parts are rejected in two comparisons instead of a walk. Both
// slices must be non-empty and ascending.
func disjointIDs(ids []signal.SeriesID, rs []streamRange) bool {
	wantFirst, wantLast := ids[0], ids[len(ids)-1]
	heldFirst, heldLast := rs[0].id, rs[len(rs)-1].id

	return wantLast.Compare(heldFirst) < 0 || wantFirst.Compare(heldLast) > 0
}

// lookup returns the row span stream id occupies in the part.
func (p *part) lookup(id signal.SeriesID) (rowRange, bool) {
	i, ok := slices.BinarySearchFunc(p.ranges, id, func(sr streamRange, target signal.SeriesID) int {
		return sr.id.Compare(target)
	})
	if !ok {
		return rowRange{}, false
	}

	return p.ranges[i].rowRange, true
}

// heldStreams appends to dst the entries of the requested streams the part holds. ids must be sorted
// ascending (duplicates allowed); the walk is a merge-join against the part's own sorted stream list,
// so it costs O(len(ids) + len(p.ranges)) comparisons and no hashing.
func (p *part) heldStreams(dst []streamRange, ids []signal.SeriesID) []streamRange {
	rs := p.ranges
	if len(rs) == 0 || len(ids) == 0 {
		return dst
	}

	if disjointIDs(ids, rs) {
		return dst
	}

	j := 0

	for _, id := range ids {
		for j < len(rs) && rs[j].id.Compare(id) < 0 {
			j++
		}

		if j == len(rs) {
			break
		}

		if rs[j].id == id {
			dst = append(dst, rs[j])
		}
	}

	return dst
}

// readCols decodes the part's timestamp column plus the schema columns selected by sel (unselected
// stay nil — lazy decode). Returned byte slices are freshly decoded (owned by the caller). getI64,
// when non-nil, supplies reusable int-column scratch from a pool (the fetch path, whose decoded int
// columns are copied out and then recycled by [Engine.recycleDecodeInts]); pass nil to decode into
// fresh slices (the merge path, which has no recycle point). blocks selects the granules to decode
// (nil ⇒ all), so a windowed fetch reads only the granules its rows occupy.
func (p *part) readCols(ctx context.Context, sel colSel, getI64 func() []int64, blocks []int) (*recordCols, error) {
	c := &recordCols{schema: p.schema, sel: sel, ints: make([][]int64, p.schema.numInts()), bytes: make([]byteCol, p.schema.numBytes())}

	dst := func() []int64 {
		if getI64 != nil {
			return getI64()
		}

		return nil
	}

	// The timestamp column is decoded whole, never pruned, however narrow the window.
	//
	// Pruning leaves the rows of unselected granules *unspecified* — that is the blocked decoder's
	// contract, and it is what makes pruning cheap. Row selection reads the timestamps directly
	// (binary search over each stream's ts-ascending range, see [tsWindow]), so unspecified
	// timestamps would break the search's precondition and let it return rows the window never
	// covered — with the value columns of those rows equally unspecified.
	//
	// Decoding it whole restores the invariant the rest of the pruning rests on: selection is driven
	// by real timestamps, and every row that selection can reach lies in a granule overlapping the
	// window, which is a granule pruning always keeps. The column is int64 delta-of-delta and tiny
	// next to the bodies and attributes the pruning is actually there to skip.
	var err error
	if c.ts, err = p.readInt64(ctx, colTs, dst(), nil); err != nil {
		return nil, err
	}

	for k := range c.ints {
		if sel.ints[k] {
			if c.ints[k], err = p.readInt64(ctx, p.schema.intColumn(k).Name, dst(), blocks); err != nil {
				return nil, err
			}
		}
	}

	for k := range c.bytes {
		if sel.bytes[k] {
			if c.bytes[k], err = p.readBytes(ctx, p.schema.byteColumn(k).Name, blocks); err != nil {
				return nil, err
			}
		}
	}

	return c, nil
}

// readInt64 decodes the named int column. blocks selects the granules to decode (nil ⇒ all); an
// unblocked column ignores it and decodes whole, so a part written before framing still reads.
// Decoded rows land at their part row offsets either way, so a caller's row indices stay valid.
func (p *part) readInt64(ctx context.Context, name string, dst []int64, blocks []int) ([]int64, error) {
	col, err := p.reader.Column(ctx, name)
	if err != nil {
		return nil, err
	}

	if blocks == nil {
		return col.Int64(dst)
	}

	return col.DecodeBlocksInt64(dst, blocks)
}

// decodeDict decodes a byte column, restricted to the given granules when the caller pruned (nil ⇒
// the whole column). An unblocked column decodes whole either way, so a part written before framing
// still reads.
func decodeDict(col *block.ColumnReader, blocks []int) (*chunk.DictColumn, error) {
	if blocks == nil || !col.Blocked() {
		return col.Bytes()
	}

	return col.DecodeBlocksBytesIntoColumn(blocks)
}

// readBytes decodes the named byte column into the contiguous offsets+blob [byteCol] layout,
// concatenating the per-row cells (which the dictionary decoder returns as views into its shared
// entries) into one owned blob so the fetch/scan path reads cells with locality and the GC scans two
// slice headers per column instead of one per row.
func (p *part) readBytes(ctx context.Context, name string, blocks []int) (byteCol, error) {
	col, err := p.reader.Column(ctx, name)
	if err != nil {
		return byteCol{}, err
	}

	dc, err := decodeDict(col, blocks)
	if err != nil {
		return byteCol{}, err
	}

	n := dc.Len()
	out := byteCol{offsets: make([]int32, 1, n+1)}

	if blocks == nil {
		for i := range n {
			out.appendCell(dc.At(i))
		}

		return out, nil
	}

	// Only the selected granules hold values this query will read. Unselected rows still get a cell
	// — the blob is indexed by part row, and shifting it would invalidate the row ranges the fetch
	// already resolved — but an empty one, so the copy costs what the selection covers rather than
	// what the part holds. Without this the decode prunes and the materialization undoes it.
	size := p.reader.Manifest().GranuleSize
	if size <= 0 {
		for i := range n {
			out.appendCell(dc.At(i))
		}

		return out, nil
	}

	keep := make([]bool, (n+size-1)/size)
	for _, g := range blocks {
		if g >= 0 && g < len(keep) {
			keep[g] = true
		}
	}

	for i := range n {
		if keep[i/size] {
			out.appendCell(dc.At(i))
		} else {
			out.appendCell(nil)
		}
	}

	return out, nil
}
