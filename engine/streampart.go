package engine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

// Column ordinals of a metric part, fixed by the order [newPartStreamWriter] declares them.
const (
	colSeriesIdx = iota
	colTsIdx
	colValueIdx
	colSFIdx
)

// partStreamWriter writes one output part of a merge a series at a time, so the merge never holds
// a whole part's uncompressed rows. It wraps a [block.StreamWriter] for the columns and builds the
// part's sidecars — series index, identities, aggregate stats — from the same per-series calls,
// each of which is run- or series-shaped and so costs O(distinct series) rather than O(rows).
//
// Rows must arrive in the part's sort order — series ascending, timestamps ascending within a
// series — one whole series per [partStreamWriter.appendSeries] call, which is what the streaming
// merge produces.
type partStreamWriter struct {
	e   *Engine
	w   *block.StreamWriter
	seq int

	// runs is the series column's run-length form: one entry per series, and the input to both the
	// id column and the series-index sidecar.
	runs []chunk.U128Run

	statsIDs []chunk.U128
	stats    []SeriesAgg

	withSF    bool
	withStats bool
	// sampled records that some weight was not 1, which is what decides both whether the weight
	// column survives ([block.StreamWriter.OmitConstColumn]) and whether the aggregate sidecar is
	// written (it holds raw values, so a sampled part falls back to the weighted decode path).
	sampled bool
	// ones backs the unit weights an unsampled series contributes to a declared weight column.
	ones []float64

	rows       int
	minT, maxT int64
	haveTime   bool
}

// newPartStreamWriter starts an output part. comp and precisionBits are fixed for the whole part
// (chosen per merge — see [Engine.compactStream]) because a streamed column is encoded as its rows
// arrive and cannot be re-encoded once the part is under way.
//
// withSF declares the lossy-sampling weight column. It is dropped again at finish if every weight
// turned out to be 1, so an unsampled part keeps the three-column layout it has today.
func newPartStreamWriter(
	e *Engine, seq int, comp compressProfile, precisionBits uint8, withSF, withStats bool,
) (*partStreamWriter, error) {
	blockRows := e.cfg.MetricBlockRows
	if blockRows <= 0 {
		blockRows = DefaultMetricBlockRows
	}

	opts := []block.PartOption{block.WithSortKey(colTs), block.WithGranuleSize(blockRows)}
	if comp.Algorithm != compress.AlgorithmNone {
		opts = append(opts, block.WithCompression(comp.Algorithm), block.WithCompressionLevel(comp.Level))
	}

	w := block.NewStreamWriter(opts...)

	if err := w.AddColumn(block.Column{Name: colSeries, Kind: block.KindInt128}); err != nil {
		return nil, err
	}

	if err := w.AddColumn(block.Column{
		Name: colTs, Kind: block.KindInt64, Codec: chunk.CodecDoD, Block: true,
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

	return &partStreamWriter{e: e, w: w, seq: seq, withSF: withSF, withStats: withStats}, nil
}

// appendSeries appends one series' samples. sf carries the per-sample weights and may be nil, in
// which case every weight is 1.
func (p *partStreamWriter) appendSeries(id chunk.U128, ts []int64, values, sf []float64) error {
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

	p.runs = append(p.runs, chunk.U128Run{Value: id, Count: len(ts)})
	p.rows += len(ts)

	// Rows arrive series-major, not time-major, so the part's time bounds are the running extremes
	// over every series rather than the first and last row.
	lo, hi := ts[0], ts[len(ts)-1]
	if !p.haveTime {
		p.minT, p.maxT, p.haveTime = lo, hi, true
	} else {
		p.minT, p.maxT = min(p.minT, lo), max(p.maxT, hi)
	}

	if p.withStats {
		agg := SeriesAgg{}
		for _, v := range values {
			agg.addSample(v)
		}

		p.statsIDs = append(p.statsIDs, id)
		p.stats = append(p.stats, agg)
	}

	return nil
}

// encodedBytes is the compressed size the part has accumulated so far, what the merge seals on.
func (p *partStreamWriter) encodedBytes() int64 { return p.w.EncodedBytes() }

// weights returns n per-sample weights, materializing the unit vector for an unsampled series so a
// declared weight column stays aligned with the other columns.
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

// finish serializes and writes the part with its sidecars, then opens it and stamps its time
// bounds — the streaming counterpart of [Engine.writeMergedPart]. At least one series must have
// been appended: the caller starts a writer only when it has a series to put in it, so an empty
// part would mean a burnt sequence number and an unreadable prefix.
func (p *partStreamWriter) finish(ctx context.Context) (*part, error) {
	if p.rows == 0 {
		return nil, errors.New("engine: finishing a merge output part with no rows")
	}

	prefix := p.e.partPrefix(p.seq)

	if err := block.WriteStreamPart(ctx, p.e.cfg.Backend, prefix, p.w); err != nil {
		return nil, errors.Wrapf(err, "write part %q", prefix)
	}

	// Series-index sidecar, from the runs the append calls already produced.
	if err := p.e.cfg.Backend.Write(ctx, sidxKey(prefix), encodeSeriesIndexRuns(p.runs)); err != nil {
		return nil, errors.Wrapf(err, "write series-index sidecar %q", prefix)
	}

	// Identity object: the identities of this part's series, snapshotted once for the whole part
	// rather than per series, so the merge takes the engine's read lock a single time.
	if err := writeIdentity(ctx, p.e.cfg.Backend, prefix, p.identityEntries()); err != nil {
		return nil, err
	}

	if p.withStats && !p.sampled {
		if err := p.e.cfg.Backend.Write(ctx, statsKey(prefix), encodeSeriesStats(p.statsIDs, p.stats)); err != nil {
			return nil, errors.Wrapf(err, "write stats sidecar %q", prefix)
		}
	}

	part, err := openPart(ctx, p.e.cfg.Backend, prefix)
	if err != nil {
		return nil, err
	}

	part.minTime, part.maxTime = p.minT, p.maxT

	return part, nil
}

// identityEntries resolves the part's distinct series to their identities under one read lock.
func (p *partStreamWriter) identityEntries() []series.Entry {
	p.e.mu.RLock()
	defer p.e.mu.RUnlock()

	out := make([]series.Entry, 0, len(p.runs))

	for _, r := range p.runs {
		id := signal.SeriesID(r.Value)
		if s, ok := p.e.head.series.Get(id); ok {
			out = append(out, series.Entry{ID: id, Series: s})
		}
	}

	return out
}
