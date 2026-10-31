package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/signal"
)

// Column ordinals of a metric part, fixed by the order [newPartStreamWriter] declares them.
const (
	colSeriesIdx = iota
	colTsIdx
	colValueIdx
	colSFIdx
)

// partStreamWriter writes one output part of a merge a series at a time: a [block.StreamWriter] for
// the columns, plus the part's sidecars built from the same per-series calls, so they cost
// O(distinct series) rather than O(rows).
//
// Rows must arrive in the part's sort order; consecutive chunks of one series are coalesced.
type partStreamWriter struct {
	e      *Engine
	w      *block.StreamWriter
	prefix string

	// runs is the series column's run-length form: one entry per series, feeding both the id column
	// and the series-index sidecar.
	runs []chunk.U128Run

	statsIDs []chunk.U128
	stats    []SeriesAgg

	// wmarks is the part's per-series durability watermark sidecar, accumulated with the runs.
	wmarks []watermark.Entry

	withSF    bool
	withStats bool
	// sampled decides both whether the weight column survives (OmitConstColumn) and whether the
	// aggregate sidecar is written — it holds raw values, so a sampled part must fall back to the
	// weighted decode path.
	sampled bool
	ones    []float64 // unit weights an unsampled series contributes to a declared weight column

	rows       int
	minT, maxT int64
	haveTime   bool
	buffered   bool
}

// newPartStreamWriter starts an output part. comp and precisionBits are fixed for the whole part —
// chosen per merge, see mergeEncoding — because a streamed column cannot be re-encoded once the
// part is under way. withSF declares the weight column, dropped again at finish if every weight
// turned out to be 1. rollup is the downsampling layout the part records (nil: unknown).
//
// ctx spans the part, not just its construction: the writer hands each sealed compression frame to
// the backend as it is produced, so the part's size is bounded by the disk it lands on rather than
// by the memory this process can spare for it. A writer that will not reach finish must be released
// with abort.
func newPartStreamWriter(
	ctx context.Context, e *Engine, comp compressProfile, precisionBits uint8, withSF, withStats bool,
	rollup *block.Rollup,
) (*partStreamWriter, error) {
	blockRows := e.cfg.MetricBlockRows
	if blockRows <= 0 {
		blockRows = DefaultMetricBlockRows
	}

	opts := partOptions(blockRows, comp, rollup)
	prefix := e.newPartPrefix()
	w := block.NewStreamWriterTo(ctx, e.cfg.Backend, prefix, opts...)

	if err := w.AddColumn(block.Column{Name: colSeries, Kind: block.KindInt128}); err != nil {
		return nil, err
	}

	if err := w.AddColumn(block.Column{
		Name: colTs, Kind: block.KindInt64, Codec: e.tsCodec, Block: true,
	}); err != nil {
		return nil, err
	}

	if err := w.AddColumn(block.Column{
		Name: colValue, Kind: block.KindFloat64, AutoCodec: true, FloatPrecisionBits: precisionBits, Block: true,
	}); err != nil {
		return nil, err
	}

	if withSF {
		if err := w.AddColumn(block.Column{Name: colSF, Kind: block.KindFloat64, Block: true}); err != nil {
			return nil, err
		}

		if err := w.OmitConstColumn(1); err != nil {
			return nil, err
		}
	}

	return &partStreamWriter{
		e: e, w: w, prefix: prefix, withSF: withSF, withStats: withStats,
		buffered: !backend.StreamsWrites(e.cfg.Backend),
	}, nil
}

// appendSeries appends one series' samples; a nil sf means every weight is 1. covered is the newest
// timestamp the run covers, the watermark the replica refresh trims through; a rollup can cover past
// its newest sample.
func (p *partStreamWriter) appendSeries(id chunk.U128, ts []int64, values, sf []float64, covered int64) error {
	if len(ts) == 0 {
		return nil
	}

	if err := p.w.AppendU128Run(colSeriesIdx, id, len(ts)); err != nil {
		return err
	}

	if err := p.w.AppendInt64(colTsIdx, ts); err != nil {
		return err
	}

	if err := p.w.AppendFloat64(colValueIdx, values); err != nil {
		return err
	}

	if p.withSF {
		if err := p.w.AppendFloat64(colSFIdx, p.weights(sf, len(ts))); err != nil {
			return err
		}
	}

	sameSeries := len(p.runs) > 0 && p.runs[len(p.runs)-1].Value == id
	if sameSeries {
		p.runs[len(p.runs)-1].Count += len(ts)
		p.wmarks[len(p.wmarks)-1].Max = max(p.wmarks[len(p.wmarks)-1].Max, covered)
	} else {
		p.runs = append(p.runs, chunk.U128Run{Value: id, Count: len(ts)})
		p.wmarks = append(p.wmarks, watermark.Entry{ID: u128ToID(id), Max: covered})
		if p.withStats {
			p.statsIDs = append(p.statsIDs, id)
			p.stats = append(p.stats, SeriesAgg{})
		}
	}
	p.rows += len(ts)

	// Rows arrive series-major, so the part's bounds are the running extremes over every series
	// rather than the first and last row.
	lo, hi := ts[0], ts[len(ts)-1]
	if !p.haveTime {
		p.minT, p.maxT, p.haveTime = lo, hi, true
	} else {
		p.minT, p.maxT = min(p.minT, lo), max(p.maxT, hi)
	}

	if p.withStats {
		agg := &p.stats[len(p.stats)-1]
		for _, v := range values {
			agg.addSample(v, 1) // the sidecar is written only for an unsampled part; see encodeSeriesStats
		}
	}

	return nil
}

func (p *partStreamWriter) encodedBytes() int64 { return p.w.EncodedBytes() }

func (p *partStreamWriter) reserveSeries(n int) error {
	p.runs = slices.Grow(p.runs, max(n-len(p.runs), 0))
	p.wmarks = slices.Grow(p.wmarks, max(n-len(p.wmarks), 0))
	if p.withStats {
		p.statsIDs = slices.Grow(p.statsIDs, max(n-len(p.statsIDs), 0))
		p.stats = slices.Grow(p.stats, max(n-len(p.stats), 0))
	}
	return p.w.ReserveU128Runs(colSeriesIdx, n)
}

// residentBytes is what the part holds in RAM: the block writer's buffers plus the sidecars this
// type accumulates, which are O(distinct series) rather than O(rows). Once the column frames stream
// out, the sidecars are what is left — and a merge of very short series can hold more of them per
// encoded byte than a merge of long ones, so they are the term worth sealing on.
func (p *partStreamWriter) residentBytes() int64 {
	const (
		runBytes   = 24 // chunk.U128Run
		idBytes    = 16 // chunk.U128
		aggBytes   = 40 // SeriesAgg
		wmarkBytes = 24 // watermark.Entry
	)

	total := p.w.ResidentBytes() + int64(cap(p.runs))*runBytes
	total += int64(cap(p.statsIDs))*idBytes + int64(cap(p.stats))*aggBytes
	total += int64(cap(p.wmarks)) * wmarkBytes
	total += int64(cap(p.ones)) * 8

	if p.buffered {
		total += 2 * p.w.EncodedBytes()
	}
	return total
}

// abort releases the part's in-flight column objects, so an abandoned merge leaves no half-written
// object behind. Idempotent, and a no-op once the part has been written.
func (p *partStreamWriter) abort() { p.w.Abort() }

// weights materializes the unit vector for an unsampled series, so a declared weight column stays
// aligned with the other columns.
func (p *partStreamWriter) weights(sf []float64, n int) []float64 {
	if len(sf) != n {
		if cap(p.ones) < n {
			p.ones = make([]float64, n)
			for i := range p.ones {
				p.ones[i] = 1
			}
		}

		return p.ones[:n]
	}

	for _, w := range sf {
		if w != 1 {
			p.sampled = true

			break
		}
	}

	return sf
}

// finish writes the part with its sidecars, opens it and stamps its time bounds — the streaming
// counterpart of [Engine.writeMergedPart]. At least one series must have been appended: an empty
// part would mean a burnt part id and an unreadable prefix.
func (p *partStreamWriter) finish(ctx context.Context) (*part, error) {
	return p.finishWithIdentities(ctx, p.identityEntries())
}

func (p *partStreamWriter) finishWithIdentities(ctx context.Context, identities identitySnapshot) (*part, error) {
	if p.rows == 0 {
		return nil, errors.New("engine: finishing a merge output part with no rows")
	}

	prefix := p.prefix

	if err := block.WriteStreamPart(ctx, p.e.cfg.Backend, prefix, p.w); err != nil {
		return nil, errors.Wrapf(err, "write part %q", prefix)
	}

	if err := backend.WriteDeferred(ctx, p.e.cfg.Backend, sidxKey(prefix), encodeSeriesIndexRuns(p.runs)); err != nil {
		return nil, errors.Wrapf(err, "write series-index sidecar %q", prefix)
	}

	if err := writeIdentity(ctx, p.e.cfg.Backend, prefix, identities); err != nil {
		return nil, err
	}

	if err := backend.WriteDeferred(ctx, p.e.cfg.Backend, watermark.Key(prefix), watermark.Encode(nil, p.wmarks)); err != nil {
		return nil, errors.Wrapf(err, "write watermark sidecar %q", prefix)
	}

	if p.withStats && !p.sampled {
		if err := backend.WriteDeferred(ctx, p.e.cfg.Backend, statsKey(prefix), encodeSeriesStats(p.statsIDs, p.stats)); err != nil {
			return nil, errors.Wrapf(err, "write stats sidecar %q", prefix)
		}
	}

	if err := backend.SyncPrefix(ctx, p.e.cfg.Backend, prefix); err != nil {
		return nil, errors.Wrapf(err, "sync part %q", prefix)
	}

	part, err := openPart(ctx, p.e.cfg.Backend, prefix, p.e.cfg.Obs.Corruption, p.e.readCompressors)
	if err != nil {
		return nil, err
	}

	part.minTime, part.maxTime = p.minT, p.maxT

	return part, nil
}

// identityEntries resolves the part's identities in bounded lock sections. Caller holds flushMu.
func (p *partStreamWriter) identityEntries() identitySnapshot {
	out := identitySnapshot{refs: make([]uint32, 0, len(p.runs))}
	ids := func(yield func(signal.SeriesID) bool) {
		for _, r := range p.runs {
			if !yield(signal.SeriesID(r.Value)) {
				return
			}
		}
	}
	p.e.captureIdentities(ids, func(_ signal.SeriesID, view series.View, ref uint32) {
		out.view = view
		out.refs = append(out.refs, ref)
	})
	return out
}
