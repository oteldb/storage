package engine

import (
	"context"
	"slices"

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
	prefix string
	ranges map[signal.SeriesID]rowRange
	hasSF  bool // the part carries a scale-factor column (sampling occurred); else every weight is 1

	// minTime, maxTime are the inclusive unix-ns sample bounds of the part, recorded in the
	// bucket index for time pruning. Set from the flush/merge columns when written and from
	// the index entry when reconstructed (see engine/index.go).
	minTime, maxTime int64
}

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

	return &part{reader: r, prefix: prefix, ranges: ranges, hasSF: slices.Contains(r.ColumnNames(), colSF)}, nil
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

// mergeInto adds series id's samples within [start, end] to m. It is a no-op if the part
// does not hold the series.
func (p *part) mergeInto(ctx context.Context, id signal.SeriesID, m *sampleMerge, start, end int64) error {
	rng, ok := p.ranges[id]
	if !ok {
		return nil
	}

	tsCol, err := p.reader.Column(ctx, colTs)
	if err != nil {
		return err
	}

	ts, err := tsCol.Int64(nil)
	if err != nil {
		return err
	}

	valCol, err := p.reader.Column(ctx, colValue)
	if err != nil {
		return err
	}

	vals, err := valCol.Float64(nil)
	if err != nil {
		return err
	}

	// The scale-factor column is present only when sampling occurred; absent ⇒ every weight is 1.
	var sf []float64

	if p.hasSF {
		sfCol, err := p.reader.Column(ctx, colSF)
		if err != nil {
			return err
		}

		full, err := sfCol.Float64(nil)
		if err != nil {
			return err
		}

		sf = full[rng.start:rng.end]
	}

	m.add(ts[rng.start:rng.end], vals[rng.start:rng.end], sf, start, end)

	return nil
}
