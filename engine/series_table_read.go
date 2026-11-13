package engine

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/internal/watermark"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

func (p *part) sharedSeriesWorkspace(ctx context.Context, matchers int) (int64, error) {
	snapshot := p.acquireSeriesSnapshot(ctx)
	defer snapshot.release()
	n := buildbudget.Add(snapshot.workspace(matchers), 64<<10)
	n = buildbudget.Add(n, buildbudget.Add(memsize.MapBase,
		buildbudget.Mul(int64(p.index.seriesCount()), 2*memsize.MapEntry[signal.SeriesID, struct{}]())))
	stats, err := p.sharedStatsWorkspace(ctx)
	if err != nil {
		return 0, err
	}
	return buildbudget.Add(n, stats), nil
}

func (p *part) sharedStatsWorkspace(ctx context.Context) (int64, error) {
	if p.hasSF {
		return 0, nil
	}
	size, known, err := backend.NativeSizeOf(ctx, p.be, statsKey(p.prefix))
	if errors.Is(err, backend.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !known || size < 0 {
		return 0, errors.Wrap(readbudget.ErrExceeded, "cannot bound numeric series statistics")
	}
	return buildbudget.Mul(size, 16), nil
}

// selectSharedDiskParts resolves global predicates once, retaining only identities in the read window.
func (p *enginePlan) selectSharedDiskParts(ctx context.Context, r fetch.Request,
	selected map[signal.SeriesID]diskSelection, withIdentity, withMetadata bool,
) error {
	snapshot, err := p.sharedQuerySnapshot()
	if err != nil {
		return err
	}
	defer snapshot.release()
	if len(snapshot.roots) == 0 {
		return nil
	}
	if err := p.selectionBudget.Reserve(64 << 10); err != nil {
		return err
	}
	defer p.selectionBudget.Release(64 << 10)
	// Every root is held by a pinned part for the whole plan lifetime.
	err = snapshot.visit(ctx, r.Matchers, func(id signal.SeriesID, raw []byte) error {
		if _, exists := selected[id]; exists {
			return nil
		}
		present, err := p.sharedNumericContains(ctx, id)
		if err != nil || !present {
			return err
		}
		return p.addDiskSelection(selected, id, raw, withIdentity)
	})
	if err != nil || !withMetadata {
		return err
	}
	for _, pt := range p.liveParts {
		if pt.sharedSeries != nil {
			if err := p.selectSharedNumericMetadata(ctx, pt, selected); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *enginePlan) sharedQuerySnapshot() (*seriesSnapshot, error) {
	snapshot := newSeriesSnapshot(nil)
	ok := false
	defer func() {
		if !ok {
			snapshot.release()
		}
	}()
	var roots map[bucketindex.SeriesRoot]struct{}
	for _, pt := range p.liveParts {
		if pt.sharedSeries == nil {
			continue
		}
		if roots == nil {
			if err := p.reserveSelection(2 * memsize.MapBase); err != nil {
				return nil, err
			}
			roots = make(map[bucketindex.SeriesRoot]struct{})
		}
		held := pt.sharedSeries.acquire()
		for _, root := range held.roots {
			if _, exists := roots[root.binding]; exists {
				continue
			}
			n := 2*memsize.MapEntry[bucketindex.SeriesRoot, struct{}]() + 2*memsize.Of[*seriesRootHandle]()
			if err := p.reserveSelection(n); err != nil {
				held.release()
				return nil, err
			}
			root.acquire()
			roots[root.binding] = struct{}{}
			snapshot.roots = append(snapshot.roots, root)
		}
		held.release()
	}
	ok = true
	return snapshot, nil
}

func (p *enginePlan) sharedNumericContains(ctx context.Context, id signal.SeriesID) (bool, error) {
	for _, pt := range p.liveParts {
		if pt.sharedSeries == nil {
			continue
		}
		present, err := pt.index.has(pt.reader.ReaderContext(ctx), id)
		if err != nil || present {
			return present, err
		}
	}
	return false, nil
}

func (p *enginePlan) selectSharedNumericMetadata(ctx context.Context, pt *part, selected map[signal.SeriesID]diskSelection) error {
	workspace, err := pt.sharedStatsWorkspace(ctx)
	if err != nil {
		return err
	}
	if err := p.selectionBudget.Reserve(workspace); err != nil {
		return err
	}
	defer p.selectionBudget.Release(workspace)
	stats, err := pt.sharedSeriesStats(ctx)
	if err != nil {
		return err
	}
	if err := p.reserveSelection(2 * memsize.MapBase); err != nil {
		return err
	}
	metadata := make(map[signal.SeriesID]seriesindex.Metadata)
	p.diskMetadata[pt] = metadata
	for id := range selected {
		meta, found, err := pt.sharedSeriesMetadata(ctx, id)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := p.reserveSelection(2 * memsize.MapEntry[signal.SeriesID, seriesindex.Metadata]()); err != nil {
			return err
		}
		if stat, ok := stats[id]; ok {
			meta.HasStats = true
			meta.Stats = seriesindex.Stats{Count: stat.Rows, Sum: stat.Sum, Min: stat.Min, Max: stat.Max}
		}
		metadata[id] = meta
	}
	return nil
}

func (p *part) visitSharedDiskSeries(ctx context.Context, matchers []fetch.Matcher,
	visit func(signal.SeriesID, []byte, seriesindex.Metadata) error,
) error {
	stats, err := p.sharedSeriesStats(ctx)
	if err != nil {
		return err
	}
	seen := make(map[signal.SeriesID]struct{})
	snapshot := p.acquireSeriesSnapshot(ctx)
	defer snapshot.release()
	return snapshot.visit(ctx, matchers, func(id signal.SeriesID, raw []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, duplicate := seen[id]; duplicate {
			return nil
		}
		meta, present, err := p.sharedSeriesMetadata(ctx, id)
		if err != nil || !present {
			return err
		}
		seen[id] = struct{}{}
		if stat, ok := stats[id]; ok {
			meta.HasStats = true
			meta.Stats = seriesindex.Stats{Count: stat.Rows, Sum: stat.Sum, Min: stat.Min, Max: stat.Max}
		}
		return visit(id, raw, meta)
	})
}

// diskSeriesLookupRaw borrows a checked descriptor only for the callback's lifetime.
func (p *part) diskSeriesLookupRaw(ctx context.Context, id signal.SeriesID, visit func([]byte) error) (bool, error) {
	present, err := p.index.has(p.reader.ReaderContext(ctx), id)
	if err != nil || !present {
		return false, err
	}
	if p.sharedSeries != nil {
		snapshot := p.acquireSeriesSnapshot(ctx)
		defer snapshot.release()
		found, err := snapshot.lookup(ctx, id, visit)
		if err == nil && !found {
			return false, errors.Wrap(seriesindex.ErrCorrupt, "shared descriptor missing numeric series")
		}
		return found, err
	}
	s, _, found, err := p.lookupDiskSeries(ctx, id)
	if err != nil || !found {
		return false, err
	}
	if err := visit(s.AppendHashInput(nil)); err != nil {
		return false, err
	}
	return true, nil
}

func (p *part) lookupSharedDiskSeries(ctx context.Context, id signal.SeriesID) (signal.Series, seriesindex.Metadata, bool, error) {
	meta, present, err := p.sharedSeriesMetadata(ctx, id)
	if err != nil || !present {
		return signal.Series{}, seriesindex.Metadata{}, false, err
	}
	var identity signal.Series
	found, err := p.diskSeriesLookupRaw(ctx, id, func(raw []byte) error {
		var err error
		var used int
		identity, used, err = signal.DecodeSeries(bytes.Clone(raw))
		if err != nil || used != len(raw) || identity.Hash() != id {
			return errors.Wrap(seriesindex.ErrCorrupt, "shared descriptor canonical ID")
		}
		return nil
	})
	if err != nil || !found {
		return signal.Series{}, seriesindex.Metadata{}, false, err
	}
	stats, err := p.sharedSeriesStats(ctx)
	if err != nil {
		return signal.Series{}, seriesindex.Metadata{}, false, err
	}
	if stat, ok := stats[id]; ok {
		meta.HasStats = true
		meta.Stats = seriesindex.Stats{Count: stat.Rows, Sum: stat.Sum, Min: stat.Min, Max: stat.Max}
	}
	return identity, meta, true, nil
}

func (p *part) sharedSeriesMetadata(ctx context.Context, id signal.SeriesID) (seriesindex.Metadata, bool, error) {
	ctx = p.reader.ReaderContext(ctx)
	present, err := p.index.has(ctx, id)
	if err != nil || !present {
		return seriesindex.Metadata{}, false, err
	}
	r, err := p.sharedWatermarkReader(ctx)
	if err != nil {
		return seriesindex.Metadata{}, false, err
	}

	covered, found, err := r.Lookup(ctx, id)
	if err != nil {
		return seriesindex.Metadata{}, false, err
	}
	if !found {
		return seriesindex.Metadata{}, false, errors.Wrap(seriesindex.ErrCorrupt, "numeric watermark missing series")
	}
	return seriesindex.Metadata{Covered: covered}, true, nil
}

func (p *part) sharedWatermarkReader(ctx context.Context) (*watermark.Reader, error) {
	var err error
	p.diskWatermarkMu.Lock()
	r := p.diskWatermark
	if r == nil {
		r, err = watermark.OpenReader(ctx, p.be, watermark.Key(p.prefix), p.index.seriesCount())
		if err == nil {
			p.diskWatermark = r
		}
	}
	p.diskWatermarkMu.Unlock()
	if err != nil {
		return nil, errors.Wrap(err, "open numeric series watermark")
	}
	return r, nil
}

//nolint:nilnil,nilerr // Missing or corrupt optional statistics fall back to sample decoding.
func (p *part) sharedSeriesStats(ctx context.Context) (map[signal.SeriesID]SeriesAgg, error) {
	if p.hasSF {
		return nil, nil
	}
	raw, err := backend.ReadUncached(p.reader.ReaderContext(ctx), p.be, statsKey(p.prefix))
	if errors.Is(err, backend.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) < 8 {
		return nil, nil
	}
	count, used := binary.Uvarint(raw[4 : len(raw)-4])
	if used <= 0 || count != uint64(p.index.seriesCount()) || count > uint64((len(raw)-8)/41) {
		return nil, nil
	}
	stats, err := decodeSeriesStats(raw)
	if err != nil {
		return nil, nil
	}
	return stats, nil
}

func (p *part) validateSharedSeriesIDs(ctx context.Context) error {
	if p.index.seriesCount() == 0 || p.sharedSeries == nil {
		return errors.Wrap(seriesindex.ErrCorrupt, "empty shared numeric part")
	}
	n := buildbudget.Add(memsize.MapBase,
		buildbudget.Mul(int64(p.index.seriesCount()), 2*memsize.MapEntry[signal.SeriesID, struct{}]()))
	budget := readbudget.From(ctx)
	if err := budget.Reserve(n); err != nil {
		return err
	}
	defer budget.Release(n)
	seen := make(map[signal.SeriesID]struct{})
	snapshot := p.acquireSeriesSnapshot(ctx)
	defer snapshot.release()
	for _, root := range snapshot.roots {
		r, err := root.run.NewReader(ctx)
		if err != nil {
			return err
		}
		err = func() error {
			ids := p.diskRowIDs(p.reader.ReaderContext(ctx))
			for ids.Next() {
				id := ids.At()
				if _, found := seen[id]; found {
					continue
				}
				if _, found, err := r.LookupRaw(id); err != nil {
					return err
				} else if !found {
					continue
				}
				if _, _, err := p.sharedSeriesMetadata(ctx, id); err != nil {
					return err
				}
				seen[id] = struct{}{}
			}
			return ids.Err()
		}()
		closeErr := r.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if len(seen) == p.index.seriesCount() {
			return nil
		}
	}
	return errors.Wrap(seriesindex.ErrCorrupt, "shared descriptors do not cover numeric part")
}
