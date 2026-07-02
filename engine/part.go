package engine

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/signal"
)

// A flushed metric part is the series id sort key, the sample timestamp, the sample value, and —
// only when lossy sampling occurred — a scale-factor column, one row per sample, sorted by
// (series, ts).
const (
	colSeries = "series"
	colTs     = "ts"
	colValue  = "value"
	colSF     = "sf" // lossy-sampling weight; absent when no sampling occurred (reader defaults to 1)
)

func idToU128(id signal.SeriesID) chunk.U128 { return chunk.U128{Hi: id.Hi, Lo: id.Lo} }
func u128ToID(u chunk.U128) signal.SeriesID  { return signal.SeriesID{Hi: u.Hi, Lo: u.Lo} }

// rowRange is the half-open row span [start, end) a series occupies in a part.
type rowRange struct{ start, end int }

// partIndex is a part's SeriesID → row-range index: a sorted id slice plus a row-start offsets
// slice, so a lookup is a binary search (O(log series)) and the layout carries no per-entry
// overhead — vs a resident map's bucket structure (~3× the payload) which dominated live heap
// under continuous ingestion. Because rows are sorted by (series, ts), every series occupies one
// contiguous run and the runs partition [0, rows): series k's range is
// [starts[k], starts[k+1]), so no per-entry end (nor a separate total) is stored. That makes the
// resident cost 20 bytes/series (16 id + 4 offset) — the compact form of the "stop pinning the
// full SeriesID→rowRange map" work; a further sparse/paged level is deliberately not layered on,
// as lookup sits on the per-series hot path of every fetch/count/aggregate.
//
// int32 offsets cap a part at ~2.1 G rows; openPart materializes the whole series column in RAM
// (16 B/row) to build this index, so a part anywhere near that bound (32 GiB of ids) is
// unrepresentable long before the offset overflows.
type partIndex struct {
	ids    []signal.SeriesID
	starts []int32 // len == len(ids)+1; starts[0] == 0, starts[len(ids)] == total rows
}

// lookup returns id's row range and whether the part holds it.
func (idx partIndex) lookup(id signal.SeriesID) (rowRange, bool) {
	i, ok := slices.BinarySearchFunc(idx.ids, id, signal.SeriesID.Compare)
	if !ok {
		return rowRange{}, false
	}

	return rowRange{start: int(idx.starts[i]), end: int(idx.starts[i+1])}, true
}

// has reports whether the part holds id.
func (idx partIndex) has(id signal.SeriesID) bool {
	_, ok := slices.BinarySearchFunc(idx.ids, id, signal.SeriesID.Compare)

	return ok
}

// rows returns the part's total sample count (the series runs partition [0, rows)).
func (idx partIndex) rows() int {
	if len(idx.starts) == 0 {
		return 0
	}

	return int(idx.starts[len(idx.starts)-1])
}

// buildPartIndex scans a sorted series column (each distinct id repeated for its contiguous run) and
// records one entry per distinct id — the inverse of the on-disk layout. It is what openPart pays
// once per part open. It counts distinct ids in a first pass so the two output slices are sized
// exactly (no over-allocation and no regrow), keeping the resident footprint to 20 bytes/series.
func buildPartIndex(ids []chunk.U128) partIndex {
	var idx partIndex
	if len(ids) == 0 {
		return idx
	}

	distinct := 1
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1] {
			distinct++
		}
	}

	idx.ids = make([]signal.SeriesID, distinct)
	idx.starts = make([]int32, distinct+1)

	k := 0
	for i := 0; i < len(ids); {
		j := i + 1
		for j < len(ids) && ids[j] == ids[i] {
			j++
		}

		idx.ids[k] = u128ToID(ids[i])
		idx.starts[k] = int32(i)
		k++
		i = j
	}

	idx.starts[distinct] = int32(len(ids))

	return idx
}

// part is a flushed, immutable metric part: the lazy on-backend [block.PartReader] plus an in-memory
// SeriesID → row-range index ([partIndex]).
type part struct {
	reader *block.PartReader
	be     backend.Backend // for lazily loading the aggregate-pushdown stats sidecar
	prefix string
	index  partIndex
	hasSF  bool // the part carries a scale-factor column (sampling occurred); else every weight is 1

	// statsOnce lazily loads the per-series aggregate sidecar (statsKey) on first aggregate query;
	// stats is nil when the sidecar is absent/corrupt or the part is sampled, signaling the
	// aggregate path to fall back to decoding this part.
	statsOnce sync.Once
	stats     map[signal.SeriesID]SeriesAgg

	// minTime, maxTime are the inclusive unix-ns sample bounds of the part, recorded in the
	// bucket index for time pruning. Set from the flush/merge columns when written and from
	// the index entry when reconstructed (see engine/index.go).
	minTime, maxTime int64

	// refs counts in-flight fetches reading this part lock-free. A fetch acquires (under the engine
	// lock, while the part is still live) the parts it will read and releases them when done; a retired
	// part is not deleted from the backend until its refs reach zero, so a lock-free read never races a
	// delete.
	refs atomic.Int32
}

func (p *part) acquire() { p.refs.Add(1) }
func (p *part) release() { p.refs.Add(-1) }

// deletePart removes every backend object of the part at prefix (manifest, marks, and
// column objects), found by listing the prefix.
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

// openPart opens the part at prefix and builds its SeriesID → row-range index.
func openPart(ctx context.Context, b backend.Backend, prefix string) (*part, error) {
	r, err := block.OpenPart(ctx, b, prefix)
	if err != nil {
		return nil, err
	}

	col, err := r.Column(ctx, colSeries)
	if err != nil {
		return nil, err
	}

	ids, err := col.ID128(nil)
	if err != nil {
		return nil, err
	}

	return &part{
		reader: r,
		be:     b,
		prefix: prefix,
		index:  buildPartIndex(ids),
		hasSF:  slices.Contains(r.ColumnNames(), colSF),
	}, nil
}

// seriesStat returns id's precomputed aggregate from the part's stats sidecar, lazily loading it.
// ok is false when the sidecar is absent, corrupt, or the part is sampled — the caller then decodes.
func (p *part) seriesStat(ctx context.Context, id signal.SeriesID) (SeriesAgg, bool) {
	p.statsOnce.Do(func() {
		if p.be == nil || p.hasSF {
			return
		}

		data, err := p.be.Read(ctx, statsKey(p.prefix))
		if err != nil {
			return // absent ⇒ fall back to decode
		}

		if m, err := decodeSeriesStats(data); err == nil {
			p.stats = m
		}
	})

	if p.stats == nil {
		return SeriesAgg{}, false
	}

	a, ok := p.stats[id]

	return a, ok
}

// compressedWith returns the block-compression algorithm the part's value column was written with
// (representative of the part — all columns share the writer's default algorithm). It is the basis
// for the recompression fixed point: a part already at the cold algorithm is not rewritten again.
func (p *part) compressedWith() compress.Algorithm {
	for _, c := range p.reader.Manifest().Columns {
		if c.Name == colValue {
			return c.Compress
		}
	}

	return compress.AlgorithmNone
}

// rows returns the part's total sample count (its series ranges partition [0, rows)).
func (p *part) rows() int {
	return p.index.rows()
}

// colNeed selects which of a part's columns a decode materializes beyond the always-decoded
// timestamp column. A query reading only sample existence/time (count, count_over_time) needs no
// values; one reading samples (raw selection, rate, aggregates) needs the value column — and the
// scale-factor column when the part carries one. The zero value decodes timestamps only, skipping
// the Gorilla-XOR value decode that such queries never read.
type colNeed struct {
	values bool
}

// decodedPart is a part's columns decoded once: the timestamp column plus the value column (and the
// scale factors when present) when the decode requested them, indexed by the part's per-series row
// ranges. One decode is shared across every series a fetch or merge reads from the part — decoding
// the whole column is O(rows), so doing it per series would be O(series × rows), the dominant fetch
// allocation.
type decodedPart struct {
	ts   []int64
	vals []float64
	sf   []float64 // nil when the part has no scale-factor column (every weight is 1)
	// haveValues reports whether the value (and scale-factor) columns are decoded. A ts-only decode
	// (colNeed{}) leaves them unpopulated and haveValues false; the cross-fetch decode cache reads
	// it to tell a ts-only entry from a full one, so a later value-needing query never slices an
	// absent value column out of a ts-only cache hit.
	haveValues bool
	// pooled marks a decodedPart whose slices came from the engine's decode-buffer pool (the
	// no-cross-fetch-cache path). The fetch returns it to the pool on releaseParts; safe because the
	// merge copies values out, so no result batch aliases these slices.
	pooled bool
}

// decodeFunc decodes a part's columns — either plainly ([decodePart]) or via the engine's
// cross-fetch decode cache ([Engine.decodeOf]).
type decodeFunc func(context.Context, *part) (*decodedPart, error)

// decodePart decodes p (all columns) with no caching — used by the merge path, whose source parts
// are about to be retired and so must not populate the decode cache.
func decodePart(ctx context.Context, p *part) (*decodedPart, error) {
	return p.decode(ctx, colNeed{values: true})
}

// decode reads and decodes the part's timestamp column (and, when need.values, the value/sf columns)
// once.
func (p *part) decode(ctx context.Context, need colNeed) (*decodedPart, error) {
	return p.decodeInto(ctx, nil, need)
}

// decodeInto decodes the part's columns per need, reusing reuse's slices as decode destinations when
// reuse is non-nil (so a pooled buffer of sufficient capacity is filled without allocating). reuse ==
// nil allocates fresh. A ts-only decode (need.values false) skips the value/sf columns entirely and,
// on the reuse path, leaves reuse's value/sf buffers intact (unread, since haveValues is false) so
// their capacity survives for the next value-needing decode that reuses the slot. Returns reuse
// (mutated) or a new decodedPart.
func (p *part) decodeInto(ctx context.Context, reuse *decodedPart, need colNeed) (*decodedPart, error) {
	var tsDst []int64

	var valDst, sfDst []float64

	if reuse != nil {
		tsDst = reuse.ts[:0]

		if need.values {
			valDst, sfDst = reuse.vals[:0], reuse.sf[:0]
		}
	}

	tsCol, err := p.reader.Column(ctx, colTs)
	if err != nil {
		return nil, err
	}

	ts, err := tsCol.Int64(tsDst)
	if err != nil {
		return nil, err
	}

	var vals, sf []float64

	if need.values {
		if vals, sf, err = p.decodeValueCols(ctx, valDst, sfDst); err != nil {
			return nil, err
		}
	}

	if reuse == nil {
		return &decodedPart{ts: ts, vals: vals, sf: sf, haveValues: need.values}, nil
	}

	reuse.ts, reuse.haveValues = ts, need.values

	if need.values {
		reuse.vals, reuse.sf = vals, sf
	}

	return reuse, nil
}

// decodeRangesInto decodes only the column blocks spanning ranges (the matched series' row runs) into
// reuse — the series-skip path: a query touching a fraction of a part's series decodes a fraction of
// its column blocks instead of the whole column. The returned columns are full-length with only the
// spanned blocks populated; the caller reads only the matched series' rows, which lie in those blocks.
// A nil ranges, or a part whose ts column is not blocked (a pre-block part, or a tiny constant column),
// falls back to the whole-column decode.
func (p *part) decodeRangesInto(ctx context.Context, reuse *decodedPart, need colNeed, ranges []rowRange) (*decodedPart, error) {
	if ranges == nil {
		return p.decodeInto(ctx, reuse, need)
	}

	tsCol, err := p.reader.Column(ctx, colTs)
	if err != nil {
		return nil, err
	}

	if !tsCol.Blocked() {
		return p.decodeInto(ctx, reuse, need)
	}

	blockRows, err := tsCol.BlockRows()
	if err != nil {
		return nil, err
	}

	blocks := neededBlocks(ranges, blockRows, p.rows())

	var tsDst []int64

	var valDst, sfDst []float64

	if reuse != nil {
		tsDst, valDst, sfDst = reuse.ts, reuse.vals, reuse.sf
	}

	ts, err := tsCol.DecodeBlocksInt64(tsDst, blocks)
	if err != nil {
		return nil, err
	}

	var vals, sf []float64

	if need.values {
		if vals, sf, err = p.decodeValueBlocks(ctx, valDst, sfDst, blocks); err != nil {
			return nil, err
		}
	}

	if reuse == nil {
		return &decodedPart{ts: ts, vals: vals, sf: sf, haveValues: need.values}, nil
	}

	reuse.ts, reuse.haveValues = ts, need.values

	if need.values {
		reuse.vals, reuse.sf = vals, sf
	}

	return reuse, nil
}

// decodeValueBlocks decodes the given blocks of the value column (and the sf column when present)
// into the reusable destinations, the series-skip analog of [part.decodeValueCols].
func (p *part) decodeValueBlocks(ctx context.Context, valDst, sfDst []float64, blocks []int) (vals, sf []float64, err error) {
	valCol, err := p.reader.Column(ctx, colValue)
	if err != nil {
		return nil, nil, err
	}

	if vals, err = valCol.DecodeBlocksFloat64(valDst, blocks); err != nil {
		return nil, nil, err
	}

	if !p.hasSF {
		return vals, nil, nil
	}

	sfCol, err := p.reader.Column(ctx, colSF)
	if err != nil {
		return nil, nil, err
	}

	if sf, err = sfCol.DecodeBlocksFloat64(sfDst, blocks); err != nil {
		return nil, nil, err
	}

	return vals, sf, nil
}

// neededBlocks returns the sorted block indices that ranges span, for a column of totalRows rows
// blocked at blockRows. It is the set of blocks the series-skip decode must materialize.
func neededBlocks(ranges []rowRange, blockRows, totalRows int) []int {
	if blockRows <= 0 || totalRows == 0 {
		return nil
	}

	nBlocks := (totalRows + blockRows - 1) / blockRows
	seen := make([]bool, nBlocks)

	for _, rng := range ranges {
		if rng.start >= rng.end {
			continue
		}

		for b := rng.start / blockRows; b <= (rng.end-1)/blockRows; b++ {
			seen[b] = true
		}
	}

	out := make([]int, 0, nBlocks)

	for b, s := range seen {
		if s {
			out = append(out, b)
		}
	}

	return out
}

// decodeValueCols decodes the value column — and the scale-factor column when the part carries one —
// into the given reusable destinations (empty-but-capacity slices on the pooled path, nil for a fresh
// decode), returning the decoded slices.
func (p *part) decodeValueCols(ctx context.Context, valDst, sfDst []float64) (vals, sf []float64, err error) {
	valCol, err := p.reader.Column(ctx, colValue)
	if err != nil {
		return nil, nil, err
	}

	if vals, err = valCol.Float64(valDst); err != nil {
		return nil, nil, err
	}

	if !p.hasSF {
		return vals, nil, nil
	}

	sfCol, err := p.reader.Column(ctx, colSF)
	if err != nil {
		return nil, nil, err
	}

	if sf, err = sfCol.Float64(sfDst); err != nil {
		return nil, nil, err
	}

	return vals, sf, nil
}

// mergeSeriesInto adds series row-range rng's samples within [start, end] to m, slicing the
// already-decoded columns (no per-series decode or allocation).
func (d *decodedPart) mergeSeriesInto(rng rowRange, m *sampleMerge, start, end int64) {
	var sf []float64
	if d.sf != nil {
		sf = d.sf[rng.start:rng.end]
	}

	m.add(d.ts[rng.start:rng.end], d.vals[rng.start:rng.end], sf, start, end)
}

// partDecodeCache memoizes one [decodedPart] per part for the lifetime of a single fetch or
// merge, so a part is read from the backend and decoded exactly once however many series read
// it. It is not safe for concurrent use; each fetch/merge owns its own cache.
type partDecodeCache map[*part]*decodedPart

// get returns p's decoded columns, decoding (via decode) and memoizing them on first use within
// the operation. decode is [decodePart] for a merge or [Engine.decodeOf] for a fetch (the latter
// consults the cross-fetch decode cache).
func (c partDecodeCache) get(ctx context.Context, p *part, decode decodeFunc) (*decodedPart, error) {
	if d, ok := c[p]; ok {
		return d, nil
	}

	d, err := decode(ctx, p)
	if err != nil {
		return nil, err
	}

	c[p] = d

	return d, nil
}
