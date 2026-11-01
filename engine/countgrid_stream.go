package engine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
)

func (p *enginePlan) countStreamingEstimate() (int64, bool) {
	var retained, transient int64
	for _, pt := range p.liveParts {
		if !pt.countBounded {
			return 0, false
		}
		if p.blockReaders[pt] == nil {
			desc, ok := pt.reader.ColumnDescByName(colTs)
			if !ok || !desc.Const {
				return 0, false
			}
			continue
		}
		retained = buildbudget.Add(retained, buildbudget.Add(pt.countRetained, memsize.Of[seriesBlockReader]()))
		transient = max(transient, pt.countTransient)
	}
	for _, r := range p.blockReaders {
		r.countStreaming = true
	}
	retained = buildbudget.Add(retained, memsize.Map(p.blockReaders))
	return buildbudget.Add(retained, transient), true
}

func (r *seriesBlockReader) streamGridTimestamps(ctx context.Context, rng rowRange, start, end int64, presence *gridPresence) error {
	granules := r.granules(ctx)
	for b, last := rng.start/r.blockRows, (rng.end-1)/r.blockRows; b <= last; b++ {
		if presence.complete() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !blockInWindow(granules, b, start, end) {
			continue
		}
		base := b * r.blockRows
		lo, hi := max(rng.start, base)-base, min(rng.end-base, r.blockRows, r.part.rows()-base)
		cur, err := r.countBlockCursor(ctx, b, lo)
		if err != nil {
			return err
		}
		if err := addCountCursor(ctx, cur, lo, hi, start, end, presence); err != nil {
			return err
		}
	}
	return nil
}

func (r *seriesBlockReader) countBlockCursor(ctx context.Context, b, lo int) (chunk.TsCursor, error) {
	if r.tsDec == nil {
		d, err := r.partDecoder(ctx, colTs)
		if err != nil {
			return nil, err
		}
		if d.BlockRows() != r.blockRows {
			return nil, errors.New("count grid timestamp block size differs from manifest")
		}
		r.tsDec = d
	}
	if r.countCursor == nil || r.tsB != b || r.countCursor.Pos() > lo {
		cur, err := r.tsDec.Int64Cursor(b)
		if err != nil {
			return nil, err
		}
		r.countCursor, r.tsB = cur, b
	}
	return r.countCursor, nil
}

func addCountCursor(ctx context.Context, cur chunk.TsCursor, lo, hi int, start, end int64, presence *gridPresence) error {
	if lo < 0 || lo > hi || hi > cur.Len() {
		return errors.New("count grid cursor range outside timestamp block")
	}
	for cur.Pos() < hi {
		pos := cur.Pos()
		if pos%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		ts, err := cur.Next()
		if err != nil {
			return err
		}
		if pos >= lo && ts >= start && ts <= end {
			presence.sample(ts)
			if presence.complete() {
				break
			}
		}
	}
	return nil
}
