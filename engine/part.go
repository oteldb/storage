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

// part is a flushed, immutable metric part: the lazy on-backend [block.PartReader] plus an
// in-memory SeriesID → row-range index. Because rows are sorted by (series, ts), every
// series occupies one contiguous run, so the index is one entry per series, built by
// scanning the series column once on open.
type part struct {
	reader *block.PartReader
	be     backend.Backend // for lazily loading the aggregate-pushdown stats sidecar
	prefix string
	ranges map[signal.SeriesID]rowRange
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

	ranges := make(map[signal.SeriesID]rowRange)

	for i := 0; i < len(ids); {
		j := i + 1
		for j < len(ids) && ids[j] == ids[i] {
			j++
		}

		ranges[u128ToID(ids[i])] = rowRange{start: i, end: j}
		i = j
	}

	return &part{reader: r, be: b, prefix: prefix, ranges: ranges, hasSF: slices.Contains(r.ColumnNames(), colSF)}, nil
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
	n := 0
	for _, r := range p.ranges {
		n += r.end - r.start
	}

	return n
}

// decodedPart is a part's columns decoded once: the full ts and value columns (and the scale
// factors when present), indexed by the part's per-series row ranges. One decode is shared
// across every series a fetch or merge reads from the part — decoding the whole column is
// O(rows), so doing it per series would be O(series × rows), the dominant fetch allocation.
type decodedPart struct {
	ts   []int64
	vals []float64
	sf   []float64 // nil when the part has no scale-factor column (every weight is 1)
	// pooled marks a decodedPart whose slices came from the engine's decode-buffer pool (the
	// no-cross-fetch-cache path). The fetch returns it to the pool on releaseParts; safe because the
	// merge copies values out, so no result batch aliases these slices.
	pooled bool
}

// bytes is the decoded footprint (for the decode cache's budget): 8 bytes per ts/value/sf element.
func (d *decodedPart) bytes() int64 {
	return int64(len(d.ts))*8 + int64(len(d.vals))*8 + int64(len(d.sf))*8
}

// decodeFunc decodes a part's columns — either plainly ([decodePart]) or via the engine's
// cross-fetch decode cache ([Engine.decodeOf]).
type decodeFunc func(context.Context, *part) (*decodedPart, error)

// decodePart decodes p with no caching — used by the merge path, whose source parts are about to be
// retired and so must not populate the decode cache.
func decodePart(ctx context.Context, p *part) (*decodedPart, error) { return p.decode(ctx) }

// decode reads and decodes the part's ts / value (/ sf) columns once.
func (p *part) decode(ctx context.Context) (*decodedPart, error) {
	return p.decodeInto(ctx, nil)
}

// decodeInto decodes the part's columns, reusing reuse's slices as decode destinations when reuse is
// non-nil (so a pooled buffer of sufficient capacity is filled without allocating). reuse == nil
// allocates fresh. Returns reuse (mutated) or a new decodedPart.
func (p *part) decodeInto(ctx context.Context, reuse *decodedPart) (*decodedPart, error) {
	var tsDst []int64

	var valDst, sfDst []float64

	if reuse != nil {
		tsDst, valDst, sfDst = reuse.ts[:0], reuse.vals[:0], reuse.sf[:0]
	}

	tsCol, err := p.reader.Column(ctx, colTs)
	if err != nil {
		return nil, err
	}

	ts, err := tsCol.Int64(tsDst)
	if err != nil {
		return nil, err
	}

	valCol, err := p.reader.Column(ctx, colValue)
	if err != nil {
		return nil, err
	}

	vals, err := valCol.Float64(valDst)
	if err != nil {
		return nil, err
	}

	var sf []float64

	if p.hasSF {
		sfCol, err := p.reader.Column(ctx, colSF)
		if err != nil {
			return nil, err
		}

		if sf, err = sfCol.Float64(sfDst); err != nil {
			return nil, err
		}
	}

	if reuse == nil {
		return &decodedPart{ts: ts, vals: vals, sf: sf}, nil
	}

	reuse.ts, reuse.vals, reuse.sf = ts, vals, sf

	return reuse, nil
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
