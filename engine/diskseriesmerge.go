package engine

import (
	"bytes"
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/signal"
)

type diskMergeSource struct {
	cursor   *seriesindex.SeriesCursor
	legacy   *series.Index
	metadata []seriesindex.Metadata
	attrs    signal.Attributes
}

func (e *Engine) openMetricMergeStreams(ctx context.Context, src []*part) ([]*partStream, []*diskMergeSource, error) {
	streams := make([]*partStream, len(src))
	var metadata []*diskMergeSource
	if e.cfg.DiskSeries {
		metadata = make([]*diskMergeSource, len(src))
	}
	for i, p := range src {
		stream, err := newPartStream(ctx, p, e.mergeReadWindow)
		if err == nil && e.cfg.DiskSeries {
			metadata[i], err = newDiskMergeSource(ctx, p)
		}
		if err != nil {
			stream.close()
			for _, opened := range streams {
				opened.close()
			}
			return nil, nil, err
		}
		streams[i] = stream
	}
	return streams, metadata, nil
}

type mergeIdentityItem struct {
	id       signal.SeriesID
	view     series.View
	ref      uint32
	found    bool
	ranges   []rowRange
	identity signal.Series
	covered  int64
}

func (item *mergeIdentityItem) next(ctx context.Context, e *Engine, keys *seriesMerge,
	legacy *mergeIdentityBatch, parts []*part, sources []*diskMergeSource,
) (bool, error) {
	if e.cfg.DiskSeries {
		if !keys.Next() {
			return false, nil
		}
		item.id, item.ranges = keys.Key(), keys.ranges
		var err error
		item.identity, item.covered, err = mergeDiskIdentity(item.id, keys.positions, sources)
		return err == nil, err
	}
	if !legacy.next(e, keys) {
		return false, nil
	}
	i := legacy.at - 1
	item.id, item.view = legacy.ids[i], legacy.view
	item.ref, item.found = legacy.refs[i], legacy.found[i]
	item.ranges = legacy.ranges[i*len(parts) : (i+1)*len(parts)]
	var err error
	item.covered, err = sourceCovered(ctx, parts, item.id)
	return err == nil, err
}

func (item *mergeIdentityItem) appendTo(w *partStreamWriter, ts []int64, values, sf []float64, covered int64) error {
	if w.e.cfg.DiskSeries {
		return w.appendDiskMergeSeries(item.id, item.identity, ts, values, sf, covered)
	}
	return w.appendMergeSeries(item.view, item.ref, item.found, item.id, ts, values, sf, covered)
}

func newDiskMergeSource(ctx context.Context, p *part) (*diskMergeSource, error) {
	idx, err := p.seriesIndex(ctx)
	if err != nil {
		return nil, err
	}
	if idx != nil {
		return &diskMergeSource{cursor: idx.SeriesCursor(ctx)}, nil
	}
	out := &diskMergeSource{legacy: series.New()}
	err = p.visitLegacyDiskSeries(ctx, nil, func(id signal.SeriesID, raw []byte, meta seriesindex.Metadata) error {
		s, n, err := signal.DecodeSeries(raw)
		if err != nil || n != len(raw) {
			return errors.Wrap(seriesindex.ErrCorrupt, "legacy merge identity")
		}
		if out.legacy.Add(s) != id {
			return errors.Wrap(seriesindex.ErrCorrupt, "legacy merge hash")
		}
		out.metadata = append(out.metadata, meta)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *diskMergeSource) at(id signal.SeriesID, pos int) ([]byte, seriesindex.Metadata, error) {
	if s.cursor != nil {
		got, raw, meta, err := s.cursor.At(uint32(pos))
		if err != nil {
			return nil, seriesindex.Metadata{}, err
		}
		if got != id {
			return nil, seriesindex.Metadata{}, errors.Wrap(seriesindex.ErrCorrupt, "merge descriptor ID")
		}
		if len(raw) == 0 {
			return nil, seriesindex.Metadata{}, errors.Wrap(seriesindex.ErrCorrupt, "merge descriptor")
		}
		return raw, meta, nil
	}
	ref, ok := s.legacy.Ref(id)
	if !ok || int(ref) >= len(s.metadata) {
		return nil, seriesindex.Metadata{}, errors.Wrap(seriesindex.ErrCorrupt, "legacy merge reference")
	}
	return nil, s.metadata[ref], nil
}

func mergeDiskIdentity(id signal.SeriesID, positions []mergestream.Position,
	sources []*diskMergeSource,
) (signal.Series, int64, error) {
	var selected []byte
	var legacy *diskMergeSource
	covered := minInt64
	for _, pos := range positions {
		source := sources[pos.Source]
		raw, meta, err := source.at(id, pos.Index)
		if err != nil {
			return signal.Series{}, 0, err
		}
		if raw != nil {
			if selected != nil && !bytes.Equal(selected, raw) {
				return signal.Series{}, 0, errors.Wrap(seriesindex.ErrCorrupt, "merge descriptors differ")
			}
			selected = raw
		} else {
			legacy = source
		}
		covered = max(covered, meta.Covered)
	}
	if selected != nil {
		identity, n, err := signal.DecodeSeries(selected)
		if err != nil || n != len(selected) {
			return signal.Series{}, 0, errors.Wrap(seriesindex.ErrCorrupt, "merge descriptor")
		}
		return identity, covered, nil
	}
	if legacy == nil {
		return signal.Series{}, 0, errors.Wrap(seriesindex.ErrCorrupt, "merge identity missing")
	}
	identity, ok := legacy.legacy.GetInto(id, legacy.attrs)
	if !ok {
		return signal.Series{}, 0, errors.Wrap(seriesindex.ErrCorrupt, "legacy merge identity missing")
	}
	legacy.attrs = identity.Attributes
	return identity, covered, nil
}

func (p *partStreamWriter) appendDiskMergeSeries(id signal.SeriesID, identity signal.Series,
	ts []int64, values, sf []float64, covered int64,
) error {
	if len(ts) > 0 && (len(p.runs) == 0 || u128ToID(p.runs[len(p.runs)-1].Value) != id) {
		if p.seriesBuilder == nil {
			p.seriesBuilder = &seriesindex.Builder{}
		}
		if err := p.seriesBuilder.Add(id, identity, seriesindex.Metadata{}); err != nil {
			return err
		}
	}
	return p.appendSeries(idToU128(id), ts, values, sf, covered)
}
