package engine

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/timebucket"
)

type rowSkipper interface {
	SkipToRow(row int) error
}

type encodedMergeRun struct {
	source, first, end, at, valueFirst int
	minTime, maxTime                   int64
}

type encodedMergePlan struct {
	runs    []encodedMergeRun
	enabled bool
	maxRows int
}

func (e *Engine) newEncodedMergePlan(precision uint8, withSF bool, tiers []DownsampleTier) encodedMergePlan {
	maxRows := e.cfg.MetricBlockRows
	if maxRows <= 0 {
		maxRows = DefaultMetricBlockRows
	}
	return encodedMergePlan{
		enabled: e.cfg.MetricSeriesChunks && (precision == 0 || precision >= 64) && !withSF && len(tiers) == 0,
		maxRows: maxRows,
	}
}

func granulePlanBytes(inputs int) int64 {
	return buildbudget.Mul(int64(inputs), memsize.Of[encodedMergeRun]())
}

func (s *partStream) canCopyGranules(p *part) bool {
	if s.decoders[0] == nil || s.decoders[1] == nil {
		return false
	}
	_, _, ok := encodedColumnDescs(p)
	if !ok {
		return false
	}
	_, tsOK := s.ts.(rowSkipper)
	_, valuesOK := s.val.(rowSkipper)
	return tsOK && valuesOK
}

func encodedColumnDescs(p *part) (block.ColumnDesc, block.ColumnDesc, bool) {
	ts, tsOK := p.reader.ColumnDescByName(colTs)
	values, valuesOK := p.reader.ColumnDescByName(colValue)
	if p.hasSF || len(p.rollup) > 0 || !tsOK || !valuesOK || !ts.HasSizing || !values.HasSizing ||
		ts.MaxBlockRows == 0 || values.MaxBlockRows == 0 ||
		!ts.Checked || !values.Checked || !ts.TimeBounds || ts.Codec != chunk.CodecAdaptiveTimestamps ||
		values.Codec != chunk.CodecFloatGroups || values.FloatPrecisionBits > 0 && values.FloatPrecisionBits < 64 {
		return ts, values, false
	}
	return ts, values, true
}

func granuleReadBytes(p *part) int64 {
	ts, values, ok := encodedColumnDescs(p)
	if !ok {
		return 0
	}
	column := func(c *block.ColumnDesc) int64 {
		return buildbudget.Mul(2, buildbudget.Add(c.Sizing.MaxFrameRaw, int64(compress.OutputSlack(c.Compress))))
	}
	return buildbudget.Add(column(&ts), column(&values))
}

func (p *encodedMergePlan) copySeries(item *mergeIdentityItem, streams []*partStream,
	router *timebucket.Router[*partStreamWriter], start int64, reached func(*partStreamWriter) bool,
) (bool, error) {
	if !p.enabled || !item.found && len(item.positions) == 0 {
		return false, nil
	}
	ok, err := p.prepare(streams, item.ranges, start, p.maxRows)
	if err != nil || !ok {
		return false, err
	}
	return true, p.write(item, streams, router, start, reached)
}

func (p *encodedMergePlan) prepare(streams []*partStream, ranges []rowRange, start int64, maxRows int) (bool, error) {
	if cap(p.runs) < len(streams) {
		p.runs = make([]encodedMergeRun, 0, len(streams))
	} else {
		p.runs = p.runs[:0]
	}
	for source, r := range ranges {
		if r.start == r.end {
			continue
		}
		run, ok, err := encodedSourceRun(streams[source], r, start, maxRows)
		if err != nil || !ok {
			return false, err
		}
		run.source = source
		p.runs = append(p.runs, run)
	}
	p.reset(streams)
	var previous int64
	havePrevious := false
	for i := p.next(); i >= 0; i = p.next() {
		r := &p.runs[i]
		g, err := streams[r.source].decoders[0].EncodedGranule(r.at)
		if err != nil {
			return false, err
		}
		if !g.Ordered || havePrevious && g.MinTime <= previous {
			return false, nil
		}
		previous, havePrevious = g.MaxTime, true
		p.advance(i, streams)
	}
	p.reset(streams)
	return true, nil
}

func encodedSourceRun(s *partStream, r rowRange, start int64, maxRows int) (encodedMergeRun, bool, error) {
	var run encodedMergeRun
	if !s.copyable {
		return run, false, nil
	}
	if s.pos != r.start {
		return run, false, errNonContiguousDecode
	}
	ts, values := s.decoders[0], s.decoders[1]
	first, err := ts.BlockForRow(r.start)
	if err != nil {
		return run, false, err
	}
	last, err := ts.BlockForRow(r.end - 1)
	if err != nil {
		return run, false, err
	}
	lo, _ := ts.BlockSpan(first)
	_, hi := ts.BlockSpan(last)
	if lo != r.start || hi != r.end {
		return run, false, nil
	}
	valueFirst, err := values.BlockForRow(r.start)
	if err != nil {
		return run, false, err
	}
	var previous int64
	for at := first; at <= last; at++ {
		lo, hi := ts.BlockSpan(at)
		vLo, vHi := values.BlockSpan(valueFirst + at - first)
		minT, maxT, ok := ts.BlockTimeBounds(at)
		if !ok || lo != vLo || hi != vHi || hi-lo > maxRows || !timebucket.Fits(minT, maxT, timebucket.Top()) ||
			minT < start && maxT >= start || at > first && minT <= previous {
			return run, false, nil
		}
		previous = maxT
	}
	return encodedMergeRun{first: first, end: last + 1, valueFirst: valueFirst}, true, nil
}

func (p *encodedMergePlan) reset(streams []*partStream) {
	for i := range p.runs {
		p.runs[i].at = p.runs[i].first
		p.bounds(i, streams)
	}
}

func (p *encodedMergePlan) bounds(i int, streams []*partStream) {
	r := &p.runs[i]
	if r.at < r.end {
		r.minTime, r.maxTime, _ = streams[r.source].decoders[0].BlockTimeBounds(r.at)
	}
}

func (p *encodedMergePlan) advance(i int, streams []*partStream) {
	p.runs[i].at++
	p.bounds(i, streams)
}

func (p *encodedMergePlan) next() int {
	selected := -1
	for i, r := range p.runs {
		if r.at < r.end && (selected < 0 || r.minTime < p.runs[selected].minTime) {
			selected = i
		}
	}
	return selected
}

func (s *partStream) skipGranule(row int) error {
	if err := s.ts.(rowSkipper).SkipToRow(row); err != nil {
		return err
	}
	if err := s.val.(rowSkipper).SkipToRow(row); err != nil {
		return err
	}
	s.pos = row
	return nil
}

func (p *encodedMergePlan) write(item *mergeIdentityItem, streams []*partStream,
	router *timebucket.Router[*partStreamWriter], start int64, reached func(*partStreamWriter) bool,
) error {
	for i := p.next(); i >= 0; i = p.next() {
		r := p.runs[i]
		s := streams[r.source]
		_, rowEnd := s.decoders[0].BlockSpan(r.at)
		p.advance(i, streams)
		if r.maxTime < start {
			if _, err := s.decoders[1].EncodedGranule(r.valueFirst + r.at - r.first); err != nil {
				return err
			}
			if err := s.skipGranule(rowEnd); err != nil {
				return err
			}
			continue
		}
		g, err := s.decoders[0].EncodedGranule(r.at)
		if err != nil {
			return err
		}
		covered := g.MaxTime
		if p.next() < 0 {
			covered = max(covered, item.covered)
		}
		if err := router.Append(g.MinTime, func(w *partStreamWriter) (bool, error) {
			if err := w.appendEncodedGranule(item, g, s.decoders[1], r.valueFirst+r.at-r.first, covered); err != nil {
				return false, err
			}
			return reached(w), nil
		}); err != nil {
			return err
		}
		if err := s.skipGranule(rowEnd); err != nil {
			return err
		}
	}
	return nil
}

func (p *partStreamWriter) appendEncodedGranule(item *mergeIdentityItem, ts block.EncodedGranule,
	values *block.Decoder, valueBlock int, covered int64,
) error {
	sameSeries := len(p.runs) > 0 && p.runs[len(p.runs)-1].Value == idToU128(item.id)
	if !sameSeries {
		if err := p.beginEncodedSeries(item); err != nil {
			return err
		}
	}
	var agg SeriesAgg
	if sameSeries && p.withStats {
		agg = p.stats[len(p.stats)-1]
	}
	seen := 0
	v, err := values.VisitEncodedGranule(valueBlock, func(value float64) {
		seen++
		if p.withStats {
			agg.addSample(value, 1)
		}
	})
	if err != nil {
		return err
	}
	if v.Rows != ts.Rows || seen != ts.Rows {
		return errors.Wrap(block.ErrCorrupt, "copied granule row mismatch")
	}
	p.runs, err = p.w.AppendU128RunView(colSeriesIdx, idToU128(item.id), ts.Rows)
	if err != nil {
		return err
	}
	if err := p.w.AppendEncodedRun(colTsIdx, colValueIdx, ts, v, v.FloatStats()); err != nil {
		return err
	}
	if sameSeries {
		p.covered[len(p.covered)-1] = max(p.covered[len(p.covered)-1], covered)
	} else {
		p.covered = append(p.covered, covered)
		if p.withStats {
			p.stats = append(p.stats, agg)
		}
	}
	if p.withStats {
		p.stats[len(p.stats)-1] = agg
	}
	p.rows += ts.Rows
	if !p.haveTime {
		p.minT, p.maxT, p.haveTime = ts.MinTime, ts.MaxTime, true
	} else {
		p.minT, p.maxT = min(p.minT, ts.MinTime), max(p.maxT, ts.MaxTime)
	}
	p.updateDiskSeriesMetadata()
	return nil
}

func (p *partStreamWriter) beginEncodedSeries(item *mergeIdentityItem) error {
	if err := p.w.EndGroup(); err != nil {
		return err
	}
	if p.e.cfg.DiskSeries {
		return p.addMergedDiskIdentity(item.id, item.identity, item.positions)
	}
	if item.found {
		p.mergeFinish.add(item.view, item.ref)
	}
	return nil
}
