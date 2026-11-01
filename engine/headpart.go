package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/signal"
)

const defaultFlushMemoryBytes = 64 << 20

type headPartBuilder struct {
	ctx     context.Context //nolint:containedctx // The builder and its streaming writes share one flush lifetime.
	e       *Engine
	samples map[signal.SeriesID]*sampleBuf
	view    series.View
	refs    []uint32

	identities   series.EncodingBudget
	encoded      int64
	materialized int64
	sorting      int64
	runRows      int64

	target, reserved int64
	release          func()
	writer           *partStreamWriter
	writerRows       int
	partRefs         []uint32
	parts            []*part
	rows             int
	orderWorkspace   int64
	direct           bool
}

func (e *Engine) flushMemoryBytes() int64 {
	if e.cfg.FlushMemoryBytes > 0 {
		return e.cfg.FlushMemoryBytes
	}
	return defaultFlushMemoryBytes
}

func (e *Engine) streamHead(ctx context.Context, samples map[signal.SeriesID]*sampleBuf) ([]*part, int, error) {
	b := headPartBuilder{ctx: ctx, e: e, samples: samples, target: e.flushMemoryBytes()}
	if err := b.reserve(b.target); err != nil {
		return nil, 0, err
	}
	// Only a bounded run is indexed, even if the frozen generation contains millions of series.
	b.refs = make([]uint32, 0, min(len(samples), int(b.target/1024)))
	defer func() {
		if b.writer != nil {
			b.writer.abort()
		}
		if b.release != nil {
			b.release()
		}
	}()

	expected := 0
	for _, buf := range samples {
		expected += len(buf.ts)
	}
	reserveOrder := func(bytes int64) error {
		if needed := max(b.target, buildbudget.Add(b.baseBytes(), bytes)); needed > b.reserved {
			return b.reserve(needed)
		}
		return nil
	}
	order, err := e.prepareDetachedOrder(ctx, samples, reserveOrder)
	if err == nil {
		b.orderWorkspace = order.workspace
		b.view = order.view
		err = walkIdentityOrder(ctx, order.view, order.runs, func(id signal.SeriesID, ref uint32) error {
			buf := samples[id]
			if buf == nil || len(buf.ts) == 0 {
				return nil
			}
			return b.add(ref, buf)
		})
	}
	if err == nil {
		err = b.flushRun()
	}
	if err != nil {
		return nil, 0, err
	}
	if b.rows != expected {
		return nil, 0, errors.Errorf("head has %d samples without identities", expected-b.rows)
	}
	return b.parts, b.rows, nil
}

func (b *headPartBuilder) baseBytes() int64 {
	blockRows := b.e.cfg.MetricBlockRows
	if blockRows <= 0 {
		blockRows = DefaultMetricBlockRows
	}
	n := buildbudget.Add(8<<20, buildbudget.Mul(int64(blockRows), 64))
	n = buildbudget.Add(n, b.orderWorkspace)
	return buildbudget.Add(n, buildbudget.Mul(int64(cap(b.refs)+cap(b.partRefs)), 4))
}

func (b *headPartBuilder) partRows() int {
	rows := maxRowsPerPart(b.e.cfg.MaxPartBytes)
	if backend.StreamsWrites(b.e.cfg.Backend) {
		return rows
	}
	// Whole-object backends retain encoded columns until finish; leave room for sidecars.
	buffered := max(int(max(b.target-b.baseBytes(), 0)/256), 1)
	if rows > 0 {
		return min(rows, buffered)
	}
	return buffered
}

func (b *headPartBuilder) reserve(bytes int64) error {
	if b.release != nil {
		b.release()
		b.release = nil
	}
	if b.e.cfg.FlushAdmission == nil {
		b.release = func() {}
	} else {
		var err error
		b.release, err = b.e.cfg.FlushAdmission(b.ctx, bytes)
		if err != nil {
			return err
		}
	}
	b.reserved = bytes
	return nil
}

func (b *headPartBuilder) add(ref uint32, buf *sampleBuf) error {
	var sorting int64
	if !ascendingInt64(buf.ts) {
		// sortedWindow retains its tuple scratch and up to three owned numeric columns.
		sorting = buildbudget.Mul(int64(len(buf.ts)), 48)
	}
	return b.addSeries(ref, len(buf.ts), sorting)
}

func (b *headPartBuilder) addSeries(ref uint32, rows int, sorting int64) error {
	single := buildbudget.Add(b.baseBytes(), b.view.IdentityWorkspace(int(ref)))
	single = buildbudget.Add(single, b.view.MaterializationBytes(int(ref)))
	single = buildbudget.Add(single, sorting)
	single = buildbudget.Add(single, buildbudget.Mul(b.view.EncodingBytes(int(ref)), 4))
	if single > b.target {
		if err := b.flushRun(); err != nil {
			return err
		}
		if err := b.reserve(single); err != nil {
			return err
		}
	}
	b.identities.Add(b.view, int(ref))
	estimate := b.estimate(ref, rows, sorting)
	maxRows := b.partRows()
	if len(b.refs) > 0 && (estimate > b.target || maxRows > 0 && b.runRows+int64(rows) > int64(maxRows)) {
		if err := b.flushRun(); err != nil {
			return err
		}
		b.identities.Add(b.view, int(ref))
		estimate = b.estimate(ref, rows, sorting)
	}
	if estimate > b.reserved {
		if err := b.reserve(estimate); err != nil {
			return err
		}
	}
	b.encoded = buildbudget.Add(b.encoded, b.view.EncodingBytes(int(ref)))
	b.materialized = max(b.materialized, b.view.MaterializationBytes(int(ref)))
	b.sorting = max(b.sorting, sorting)
	b.runRows = buildbudget.Add(b.runRows, int64(rows))
	b.refs = append(b.refs, ref)
	return nil
}

func (b *headPartBuilder) estimate(ref uint32, rows int, sorting int64) int64 {
	n := b.baseBytes()
	n = buildbudget.Add(n, b.identities.Bytes())
	n = buildbudget.Add(n, buildbudget.Mul(buildbudget.Add(b.encoded, b.view.EncodingBytes(int(ref))), 4))
	// Runs, sidecars, references, their serialized forms and slice growth are per identity.
	n = buildbudget.Add(n, buildbudget.Mul(int64(len(b.refs)+1), 512))
	n = buildbudget.Add(n, max(b.materialized, b.view.MaterializationBytes(int(ref))))
	n = buildbudget.Add(n, max(b.sorting, sorting))
	if !backend.StreamsWrites(b.e.cfg.Backend) {
		partRows := buildbudget.Add(b.runRows, int64(rows))
		if limit := b.partRows(); limit > 0 {
			partRows = min(partRows, int64(limit))
		}
		n = buildbudget.Add(n, buildbudget.Mul(partRows, 128))
	}
	return n
}

func (b *headPartBuilder) flushRun() error {
	if len(b.refs) == 0 {
		return nil
	}
	b.partRefs = slices.Grow(b.partRefs, max(len(b.refs)-len(b.partRefs), 0))
	for _, ref := range b.refs {
		if b.direct {
			break
		}
		if err := b.ctx.Err(); err != nil {
			return err
		}
		id := b.view.ID(int(ref))
		buf := b.samples[id]
		ts, values, sf := buf.ts, buf.values, buf.sf
		if !ascendingInt64(ts) {
			ts, values, sf = sortedWindow(buf, minInt64, maxInt64, nil)
		}
		if err := b.append(ref, ts, values, sf); err != nil {
			return err
		}
	}
	if err := b.finish(); err != nil {
		return err
	}
	b.refs = b.refs[:0]
	b.identities.Reset()
	b.encoded, b.materialized, b.sorting, b.runRows = 0, 0, 0, 0
	if b.reserved > b.target {
		return b.reserve(b.target)
	}
	return nil
}

func (b *headPartBuilder) append(ref uint32, ts []int64, values, sf []float64) error {
	chunkRows := b.e.cfg.MetricBlockRows
	if chunkRows <= 0 {
		chunkRows = DefaultMetricBlockRows
	}
	for lo := 0; lo < len(ts); {
		if err := b.ctx.Err(); err != nil {
			return err
		}
		if b.writer == nil {
			var err error
			b.writer, err = newPartStreamWriter(b.ctx, b.e, compressProfile{}, 0, true, b.e.cfg.AggregateStats, &block.Rollup{})
			if err != nil {
				return err
			}
			if err := b.writer.reserveSeries(b.seriesCapacity()); err != nil {
				return err
			}
			b.writerRows = b.partRows()
		}
		limit := b.writerRows
		hi := min(lo+chunkRows, len(ts))
		if limit > 0 {
			hi = min(hi, lo+limit-b.writer.rows)
		}
		var weights []float64
		if sf != nil {
			weights = sf[lo:hi]
		}
		id := idToU128(b.view.ID(int(ref)))
		if len(b.writer.runs) == 0 || b.writer.runs[len(b.writer.runs)-1].Value != id {
			b.partRefs = append(b.partRefs, ref)
		}
		if err := b.writer.appendSeries(id, ts[lo:hi], values[lo:hi], weights, ts[hi-1]); err != nil {
			return err
		}
		b.rows += hi - lo
		lo = hi
		if limit > 0 && b.writer.rows == limit || b.writer.residentBytes()+b.writer.w.FinishBytes() >= b.reserved {
			if err := b.finish(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *headPartBuilder) finish() error {
	if b.writer == nil {
		return nil
	}
	p, err := b.writer.finishWithIdentities(b.ctx, identitySnapshot{view: b.view, refs: b.partRefs})
	if err != nil {
		return err
	}
	planFlushBlocks(p)
	b.parts = append(b.parts, p)
	b.writer = nil
	b.partRefs = b.partRefs[:0]
	return nil
}

func (b *headPartBuilder) seriesCapacity() int {
	if b.direct {
		return cap(b.refs)
	}
	return len(b.refs)
}
