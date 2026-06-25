package engine

import (
	"context"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// A flushed metric part is three flat columns — the series id sort key, the sample
// timestamp, and the sample value — one row per sample, sorted by (series, ts).
const (
	colSeries = "series"
	colTs     = "ts"
	colValue  = "value"
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

	return &part{reader: r, prefix: prefix, ranges: ranges}, nil
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

	m.add(ts[rng.start:rng.end], vals[rng.start:rng.end], start, end)

	return nil
}
