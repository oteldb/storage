package logengine

import (
	"context"

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

// part is a flushed, immutable log part: the lazy on-backend [block.PartReader] plus an in-memory
// StreamID → row-range index. Rows are sorted by (stream, ts), so every stream occupies one
// contiguous run; the index is one entry per stream, built by scanning the stream column once.
type part struct {
	reader    *block.PartReader
	prefix    string
	ranges    map[signal.SeriesID]rowRange
	bodyBloom *bloom.Filter // body token bloom for full-text pruning; nil ⇒ always scan
	attrBloom *bloom.Filter // per-record attribute key=value bloom for equality pruning; nil ⇒ scan

	// minTime, maxTime are the inclusive unix-ns record bounds of the part (from the flush/merge
	// columns when written, from the bucket index when reconstructed), for time pruning.
	minTime, maxTime int64
}

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

// openPart opens the part at prefix and builds its StreamID → row-range index.
func openPart(ctx context.Context, b backend.Backend, prefix string) (*part, error) {
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

	bf, err := loadBodyBloom(ctx, b, prefix)
	if err != nil {
		return nil, err
	}

	af, err := loadAttrBloom(ctx, b, prefix)
	if err != nil {
		return nil, err
	}

	return &part{reader: r, prefix: prefix, ranges: ranges, bodyBloom: bf, attrBloom: af}, nil
}

// appendWindow appends stream id's records whose timestamp is in [start, end] to acc. It is a
// no-op if the part does not hold the stream. M8a decodes the full column set; projection narrows
// this in M8b.
func (p *part) appendWindow(ctx context.Context, id signal.SeriesID, acc *recordCols, start, end int64) error {
	rng, ok := p.ranges[id]
	if !ok {
		return nil
	}

	cols, err := p.readCols(ctx)
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

// readCols decodes every column of the part into a recordCols (rows aligned across columns). The
// returned byte slices are freshly decoded (owned by the caller).
func (p *part) readCols(ctx context.Context) (*recordCols, error) {
	c := &recordCols{}

	var err error

	if c.ts, err = p.readInt64(ctx, colTs); err != nil {
		return nil, err
	}

	if c.observed, err = p.readInt64(ctx, colObserved); err != nil {
		return nil, err
	}

	if c.severity, err = p.readInt64(ctx, colSeverity); err != nil {
		return nil, err
	}

	if c.flags, err = p.readInt64(ctx, colFlags); err != nil {
		return nil, err
	}

	if c.dropped, err = p.readInt64(ctx, colDropped); err != nil {
		return nil, err
	}

	if c.sevText, err = p.readBytes(ctx, colSevText); err != nil {
		return nil, err
	}

	if c.body, err = p.readBytes(ctx, colBody); err != nil {
		return nil, err
	}

	if c.traceID, err = p.readBytes(ctx, colTraceID); err != nil {
		return nil, err
	}

	if c.spanID, err = p.readBytes(ctx, colSpanID); err != nil {
		return nil, err
	}

	if c.attrs, err = p.readBytes(ctx, colAttrs); err != nil {
		return nil, err
	}

	return c, nil
}

func (p *part) readInt64(ctx context.Context, name string) ([]int64, error) {
	col, err := p.reader.Column(ctx, name)
	if err != nil {
		return nil, err
	}

	return col.Int64(nil)
}

func (p *part) readBytes(ctx context.Context, name string) ([][]byte, error) {
	col, err := p.reader.Column(ctx, name)
	if err != nil {
		return nil, err
	}

	dc, err := col.Bytes()
	if err != nil {
		return nil, err
	}

	out := make([][]byte, dc.Len())
	for i := range out {
		out[i] = dc.At(i)
	}

	return out, nil
}
