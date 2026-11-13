package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/signal"
)

// streamSampleHead preserves one publication and WAL checkpoint for both identity populations.
func (e *Engine) streamSampleHead(ctx context.Context, samples map[signal.SeriesID]*sampleBuf) ([]*part, int, error) {
	e.mu.RLock()
	known := e.head.known
	if known == nil {
		e.mu.RUnlock()
		return e.streamHead(ctx, samples)
	}
	ids := make([]signal.SeriesID, 0, len(samples))
	unknown := make(map[signal.SeriesID]*sampleBuf)
	for id, buf := range samples {
		if known.has(id) {
			ids = append(ids, id)
		} else {
			unknown[id] = buf
		}
	}
	e.mu.RUnlock()
	var parts []*part //nolint:prealloc // The ordinary builder determines the unknown output count.
	var rows int
	var err error
	if len(unknown) > 0 {
		parts, rows, err = e.streamHead(ctx, unknown)
	}
	if err != nil {
		return nil, 0, err
	}
	next, n, err := e.streamKnownSamples(ctx, ids, samples)
	if err != nil {
		closePartReaders(parts)
		return nil, 0, err
	}
	return append(parts, next...), rows + n, nil
}

func (e *Engine) streamKnownSamples(
	ctx context.Context, ids []signal.SeriesID, samples map[signal.SeriesID]*sampleBuf,
) (_ []*part, _ int, err error) {
	if len(ids) == 0 {
		return nil, 0, nil
	}
	slices.SortFunc(ids, signal.SeriesID.Compare)
	// IDs and the partition maps are bounded by the same active/frozen head limit.
	var sorting int64
	for _, id := range ids {
		if buf := samples[id]; !ascendingInt64(buf.ts) {
			sorting = max(sorting, int64(len(buf.ts))*48)
		}
	}
	e.mu.RLock()
	source, current := e.head.known.snapshot, e.seriesTable
	current.acquire()
	metadataWorkspace := source.workspace(0) + current.workspace(0) + knownCatalogCompareScratch
	e.mu.RUnlock()
	defer current.release()
	workspace := int64(len(samples))*128 + sorting + metadataWorkspace + 8<<20
	target := max(e.flushMemoryBytes(), workspace+8<<20)
	release := func() {}
	if e.cfg.FlushAdmission != nil {
		release, err = e.cfg.FlushAdmission(ctx, target)
		if err != nil {
			return nil, 0, err
		}
	}
	defer release()
	if err := verifyKnownSnapshots(ctx, source, current, ids); err != nil {
		return nil, 0, err
	}

	b := knownPartBuilder{e: e, workspace: workspace, target: target}
	defer func() {
		if b.writer != nil {
			b.writer.abort()
		}
		if err != nil {
			closePartReaders(b.parts)
		}
	}()
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		if err := b.add(ctx, id, samples[id]); err != nil {
			return nil, 0, err
		}
	}
	if err := b.finish(ctx); err != nil {
		return nil, 0, err
	}
	return b.parts, b.rows, nil
}

type knownPartBuilder struct {
	e                              *Engine
	writer                         *partStreamWriter
	scratch                        block.StreamBuildScratch
	parts                          []*part
	workspace, target, closedBytes int64
	rows                           int
}

func (b *knownPartBuilder) finish(ctx context.Context) error {
	if b.writer == nil {
		return nil
	}
	p, err := b.writer.finish(ctx)
	if err != nil {
		return err
	}
	planFlushBlocks(p)
	b.parts = append(b.parts, p)
	b.closedBytes += p.idBloom.Load().sizeBytes() + 64<<10
	b.writer = nil
	if b.workspace+b.closedBytes >= b.target {
		return errors.New("engine: known output metadata exceeds flush workspace")
	}
	return nil
}

func (b *knownPartBuilder) add(ctx context.Context, id signal.SeriesID, buf *sampleBuf) error {
	ts, values, sf := buf.ts, buf.values, buf.sf
	if !ascendingInt64(ts) {
		ts, values, sf = sortedWindow(buf, minInt64, maxInt64, nil)
	}
	blockRows := b.e.cfg.MetricBlockRows
	if blockRows <= 0 {
		blockRows = DefaultMetricBlockRows
	}
	rowLimit := maxRowsPerPart(b.e.cfg.MaxPartBytes)
	for lo := 0; lo < len(ts); {
		if b.writer == nil {
			w, err := newPartStreamWriter(ctx, b.e, compressProfile{}, 0, true, b.e.cfg.AggregateStats, &block.Rollup{})
			if err != nil {
				return err
			}
			b.writer = w
			w.buildScratch = &b.scratch
			w.mergeFinish = &mergeFinishBudget{}
		}
		hi := min(lo+blockRows, len(ts))
		if rowLimit > 0 {
			hi = min(hi, lo+rowLimit-b.writer.rows)
		}
		var weights []float64
		if sf != nil {
			weights = sf[lo:hi]
		}
		if err := b.writer.appendSeries(idToU128(id), ts[lo:hi], values[lo:hi], weights, ts[hi-1]); err != nil {
			return err
		}
		b.rows += hi - lo
		lo = hi
		held := b.workspace + b.closedBytes + b.scratch.SizeBytes() + b.writer.mergeResidentBytes()
		if held >= b.target || rowLimit > 0 && b.writer.rows >= rowLimit {
			if err := b.finish(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
