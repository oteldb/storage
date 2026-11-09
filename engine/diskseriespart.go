package engine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func seriesIndexKey(prefix string) string { return prefix + "/series-index" }

func (p *part) seriesIndex(ctx context.Context) (*seriesindex.Index, error) {
	p.metadataMu.Lock()
	defer p.metadataMu.Unlock()
	if p.metadataChecked {
		if p.metadata != nil && readbudget.From(ctx) != nil && !p.metadataBounded {
			return nil, errors.Wrap(readbudget.ErrExceeded, "part series index requires bounded ranged reads")
		}
		return p.metadata, nil
	}
	_, known, err := backend.NativeSizeOf(ctx, p.be, seriesIndexKey(p.prefix))
	if errors.Is(err, backend.ErrNotExist) {
		p.metadataChecked = true
		return p.metadata, nil
	}
	if err != nil {
		return nil, err
	}
	bounded := known && backend.RangesNatively(ctx, p.be, seriesIndexKey(p.prefix))
	if readbudget.From(ctx) != nil && !bounded {
		present, err := diskObjectPresent(ctx, p.be, seriesIndexKey(p.prefix))
		if err != nil {
			return nil, err
		}
		if !present {
			p.metadataChecked = true
			return p.metadata, nil
		}
		return nil, errors.Wrap(readbudget.ErrExceeded, "part series index requires bounded ranged reads")
	}
	r, err := seriesindex.Open(p.reader.ReaderContext(ctx), p.be, seriesIndexKey(p.prefix))
	if errors.Is(err, backend.ErrNotExist) {
		p.metadataChecked = true
		return p.metadata, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "open series index %q", p.prefix)
	}
	if uint64(r.Count()) != uint64(p.index.seriesCount()) {
		return nil, errors.Wrap(seriesindex.ErrCorrupt, "part/index series count mismatch")
	}
	if p.reader.Manifest().BindingNonce == ([16]byte{}) || r.ParentDigest() != p.reader.ManifestDigest() {
		return nil, errors.Wrap(seriesindex.ErrCorrupt, "series index belongs to a different data manifest")
	}
	p.metadata, p.metadataChecked, p.metadataBounded = r, true, bounded
	return r, nil
}

func (p *partStreamWriter) addDiskSeries(view series.View, ref uint32, id signal.SeriesID) error {
	if !p.e.cfg.DiskSeries {
		return nil
	}
	if len(p.runs) > 0 && u128ToID(p.runs[len(p.runs)-1].Value) == id {
		return nil
	}
	if p.seriesBuilder == nil {
		p.seriesBuilder = &seriesindex.Builder{}
	}
	s := view.At(int(ref), p.seriesAttrs)
	p.seriesAttrs = s.Attributes
	return p.seriesBuilder.Add(id, s, seriesindex.Metadata{})
}

func (p *partStreamWriter) updateDiskSeriesMetadata() {
	if p.seriesBuilder == nil || len(p.wmarks) == 0 {
		return
	}
	meta := seriesindex.Metadata{Covered: p.wmarks[len(p.wmarks)-1].Max}
	if p.withStats && !p.sampled {
		a := p.stats[len(p.stats)-1]
		meta.HasStats = true
		meta.Stats = seriesindex.Stats{Count: int64(a.Count), Sum: a.Sum, Min: a.Min, Max: a.Max}
	}
	p.seriesBuilder.UpdateMetadata(meta)
}

func (p *partStreamWriter) writeDiskSeries(ctx context.Context, entries identitySnapshot) error {
	if p.seriesBuilder == nil {
		p.seriesBuilder = &seriesindex.Builder{}
		for i, ref := range entries.refs {
			id := entries.view.ID(int(ref))
			if i >= len(p.wmarks) || p.wmarks[i].ID != id {
				return errors.New("engine: incomplete series-index identities")
			}
			ent := entries.view.At(int(ref), p.seriesAttrs)
			p.seriesAttrs = ent.Attributes
			if err := p.seriesBuilder.Add(id, ent, seriesindex.Metadata{Covered: p.wmarks[i].Max}); err != nil {
				return err
			}
		}
	}
	if p.seriesBuilder.Count() != len(p.runs) {
		return errors.New("engine: incomplete series index")
	}
	digest, err := block.BindManifest(ctx, p.e.cfg.Backend, p.prefix)
	if err != nil {
		return err
	}
	p.seriesBuilder.BindParent(digest)
	return p.seriesBuilder.WriteObject(ctx, p.e.cfg.Backend, seriesIndexKey(p.prefix))
}
