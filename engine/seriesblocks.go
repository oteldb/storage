package engine

import (
	"context"

	"github.com/oteldb/storage/block"
)

// seriesBlockReader streams a single part's matched-series samples into a merge by slicing the
// part's decoded column blocks directly — never materializing the whole column into a per-fetch
// decodedPart (the growLen RSS cliff). A series' rows lie in one or a few granule-sized blocks; each
// block is taken from the cross-fetch block cache (or decoded once into it on a miss), and the
// series' sub-range is added to the merge as a *view* into that cached block. The cached block is
// immutable and stays reachable through the merge run until collect copies the samples out, so no
// copy and no full-length buffer are needed on the fetch path.
//
// It is used only when the block cache is enabled and the part's ts/value(/sf) columns are all
// block-framed; a constant or legacy-unblocked column falls back to the whole-part decode path.
type seriesBlockReader struct {
	engine    *Engine
	part      *part
	blockRows int

	// Per-column block decoders, built lazily on the first cache miss (so a fully-warm part reads
	// the column object not at all). One reader serves one part for one fetch and is not used
	// concurrently (prefetch warms each part on its own goroutine, the scan reads them serially).
	tsDec, valDec, sfDec *block.Decoder

	// Last block held per column (index + data), so consecutive matched series that fall in the same
	// block reuse it instead of re-locking the cache for every series. Series are visited in row
	// order, so a part's accesses are non-decreasing block indices and this one-entry memo hits the
	// common run. -1 ⇒ none held. tsEnt/valEnt/sfEnt are the cache entries backing the held views,
	// so the per-series pin release can keep exactly those pinned.
	tsB, valB, sfB       int
	tsHeld               []int64
	valHeld, sfHeld      []float64
	tsEnt, valEnt, sfEnt *blockEntry

	// pins holds every cache entry this reader has taken a reference on. The reader adds the touched
	// blocks to the merge as *views*, so the entries must stay unrecycled until the fetch's collect
	// has copied the samples out — the fetch releases them per series (releaseSeriesPins) and sweeps
	// the rest at teardown (releasePins). One entry may appear more than once (a block revisited
	// after the memo moved on); each pin has one release.
	pins []*blockEntry
}

// newSeriesBlockReader returns a reader for part if it is block-sliceable (ts/value, and sf when
// present, are all block-framed) and the engine has a block cache; otherwise nil (the caller uses the
// whole-part decode path).
func (e *Engine) newSeriesBlockReader(p *part) *seriesBlockReader {
	if e.blockCache == nil || !p.blockSliceable() {
		return nil
	}

	return &seriesBlockReader{
		engine: e, part: p, blockRows: p.reader.Manifest().GranuleSize,
		tsB: -1, valB: -1, sfB: -1,
	}
}

// blockSliceable reports whether the part's ts and value columns — and the scale-factor column when
// present — are all block-framed (so the series-block reader can slice them). A constant or
// legacy-unblocked column is not sliceable and routes through the whole-part decode path.
func (p *part) blockSliceable() bool {
	if d, ok := p.reader.ColumnDescByName(colTs); !ok || !d.Blocked {
		return false
	}

	if d, ok := p.reader.ColumnDescByName(colValue); !ok || !d.Blocked {
		return false
	}

	if p.hasSF {
		if d, ok := p.reader.ColumnDescByName(colSF); !ok || !d.Blocked {
			return false
		}
	}

	return true
}

// addRange adds series row-range rng's in-window samples to m, slicing each spanning block straight
// from the block cache (decoding+caching a miss). The added slices are views into the immutable
// cached blocks — valid until the caller's collect copies them out.
func (r *seriesBlockReader) addRange(ctx context.Context, rng rowRange, m *sampleMerge, start, end int64) error {
	first := rng.start / r.blockRows
	last := (rng.end - 1) / r.blockRows

	for b := first; b <= last; b++ {
		tsBlk, err := r.tsBlock(ctx, b)
		if err != nil {
			return err
		}

		valBlk, err := r.valBlock(ctx, b)
		if err != nil {
			return err
		}

		blockStart := b * r.blockRows

		lo := max(rng.start, blockStart) - blockStart
		hi := min(min(rng.end, blockStart+r.blockRows)-blockStart, len(tsBlk))

		if lo >= hi {
			continue
		}

		var sf []float64

		if r.part.hasSF {
			sfBlk, err := r.sfBlock(ctx, b)
			if err != nil {
				return err
			}

			sf = sfBlk[lo:hi]
		}

		m.add(tsBlk[lo:hi], valBlk[lo:hi], sf, start, end)
	}

	return nil
}

// warm decodes (and caches) the blocks spanning ranges without slicing — the block-cache analog of
// the old whole-part prefetch, run concurrently per part so its reads/decodes overlap before the scan.
func (r *seriesBlockReader) warm(ctx context.Context, ranges []rowRange) error {
	for _, b := range neededBlocks(ranges, r.blockRows, r.part.rows()) {
		if _, err := r.tsBlock(ctx, b); err != nil {
			return err
		}

		if _, err := r.valBlock(ctx, b); err != nil {
			return err
		}

		if r.part.hasSF {
			if _, err := r.sfBlock(ctx, b); err != nil {
				return err
			}
		}
	}

	return nil
}

// tsBlock returns block b of the timestamp column, from the held memo, the cache, or a fresh decode
// (cached). Consecutive series in the same block reuse the memo without re-locking the cache.
func (r *seriesBlockReader) tsBlock(ctx context.Context, b int) ([]int64, error) {
	if b == r.tsB {
		return r.tsHeld, nil
	}

	key := blockKey{prefix: r.part.prefix, col: colTsID, blk: b}
	if ent, ok := r.engine.blockCache.get(key); ok {
		r.pins = append(r.pins, ent)
		r.tsB, r.tsHeld, r.tsEnt = b, ent.i64, ent

		return ent.i64, nil
	}

	if r.tsDec == nil {
		d, err := r.partDecoder(ctx, colTs)
		if err != nil {
			return nil, err
		}

		r.tsDec = d
	}

	blk, err := r.tsDec.DecodeInt64Into(b, r.engine.blockCache.getI64Buf(r.blockRows))
	if err != nil {
		return nil, err
	}

	// Read (and memo) the canonical buffer insert returns — a concurrent decode may have won the key,
	// in which case ours was recycled and the winner's slice is the one that stays valid.
	ent := r.engine.blockCache.insert(&blockEntry{key: key, i64: blk, bytes: int64(len(blk)) * 8})
	r.pins = append(r.pins, ent)
	r.tsB, r.tsHeld, r.tsEnt = b, ent.i64, ent

	return ent.i64, nil
}

// valBlock returns block b of the value column (memoized like tsBlock).
func (r *seriesBlockReader) valBlock(ctx context.Context, b int) ([]float64, error) {
	if b == r.valB {
		return r.valHeld, nil
	}

	blk, ent, err := r.floatBlock(ctx, &r.valDec, colValue, colValID, b)
	if err != nil {
		return nil, err
	}

	r.valB, r.valHeld, r.valEnt = b, blk, ent

	return blk, nil
}

// sfBlock returns block b of the scale-factor column (memoized like tsBlock).
func (r *seriesBlockReader) sfBlock(ctx context.Context, b int) ([]float64, error) {
	if b == r.sfB {
		return r.sfHeld, nil
	}

	blk, ent, err := r.floatBlock(ctx, &r.sfDec, colSF, colSFID, b)
	if err != nil {
		return nil, err
	}

	r.sfB, r.sfHeld, r.sfEnt = b, blk, ent

	return blk, nil
}

// floatBlock fetches block b of a float column from the cache or a fresh decode (cached), returning
// the pinned cache entry alongside the data so the caller can memoize it.
func (r *seriesBlockReader) floatBlock(
	ctx context.Context, dec **block.Decoder, name string, cid colID, b int,
) ([]float64, *blockEntry, error) {
	key := blockKey{prefix: r.part.prefix, col: cid, blk: b}
	if ent, ok := r.engine.blockCache.get(key); ok {
		r.pins = append(r.pins, ent)

		return ent.f64, ent, nil
	}

	if *dec == nil {
		d, err := r.partDecoder(ctx, name)
		if err != nil {
			return nil, nil, err
		}

		*dec = d
	}

	blk, err := (*dec).DecodeFloat64Into(b, r.engine.blockCache.getF64Buf(r.blockRows))
	if err != nil {
		return nil, nil, err
	}

	ent := r.engine.blockCache.insert(&blockEntry{key: key, f64: blk, bytes: int64(len(blk)) * 8})
	r.pins = append(r.pins, ent)

	return ent.f64, ent, nil
}

// releaseSeriesPins drops the pins taken while merging one series, keeping exactly one pin per
// currently-memoized block (its held view must stay valid for the next series). The fetch calls it
// right after collect has copied the series' samples out of the merge, so the released views are
// dead. Releasing per series — rather than only at fetch teardown — is what keeps the decode
// freelist fed: a block the byte budget evicted mid-fetch returns its buffer while the fetch is
// still running instead of holding it hostage until teardown.
func (r *seriesBlockReader) releaseSeriesPins() {
	keepTs, keepVal, keepSf := r.tsEnt, r.valEnt, r.sfEnt

	kept := r.pins[:0]

	for _, e := range r.pins {
		// Retain a single pin per live memo slot; duplicates of a memo entry (a revisited block)
		// release like any other pin, keeping the refcount balanced.
		switch e {
		case keepTs:
			keepTs = nil
			kept = append(kept, e)
		case keepVal:
			keepVal = nil
			kept = append(kept, e)
		case keepSf:
			keepSf = nil
			kept = append(kept, e)
		default:
			r.engine.blockCache.release(e)
		}
	}

	clear(r.pins[len(kept):])
	r.pins = kept
}

// releasePins drops the reader's references on every cache block it still holds (the memoized
// blocks after per-series releases, or everything when the fetch never ran collect), letting an
// evicted block's buffer return to the pool. The fetch calls it at teardown, after any views into
// these blocks are dead.
func (r *seriesBlockReader) releasePins() {
	for _, e := range r.pins {
		r.engine.blockCache.release(e)
	}

	r.pins = r.pins[:0]
	r.tsB, r.valB, r.sfB = -1, -1, -1
	r.tsHeld, r.valHeld, r.sfHeld = nil, nil, nil
	r.tsEnt, r.valEnt, r.sfEnt = nil, nil, nil
}

// partDecoder reads the named column's object and returns a per-block decoder over it.
func (r *seriesBlockReader) partDecoder(ctx context.Context, name string) (*block.Decoder, error) {
	col, err := r.part.reader.Column(ctx, name)
	if err != nil {
		return nil, err
	}

	return col.BlockDecoder()
}
