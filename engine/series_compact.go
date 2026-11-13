package engine

import (
	"bytes"
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/signal"
)

const maxSeriesRetirements = 4096

// compactSeriesRuns shares the maintenance admission and serializes with ordinary compaction.
func (e *Engine) compactSeriesRuns(ctx context.Context, background bool) (bool, error) {
	if !e.cfg.SeparateSeries {
		return false, nil
	}
	unlock := e.lockMerge()
	defer unlock()
	e.mu.RLock()
	snapshot := e.seriesTable
	snapshot.acquire()
	remaining := maxSeriesRetirements - len(e.seriesRetired)
	e.mu.RUnlock()
	defer snapshot.release()
	if snapshot == nil || len(snapshot.roots) < 2 || remaining < 2 || background && len(snapshot.roots) < 4 {
		return false, nil
	}
	sources := slices.Clone(snapshot.roots)
	slices.SortFunc(sources, func(a, b *seriesRootHandle) int { return cmp.Compare(a.run.Count(), b.run.Count()) })
	sources = sources[:min(len(sources), remaining, 8)]
	allowance := e.mergeMemoryBudgetBytes()
	sources, scratch := boundedSeriesSources(sources, allowance)
	if len(sources) < 2 {
		return false, nil
	}

	release, admitted, err := e.admitMergeBytes(ctx, background, allowance)
	if err != nil || !admitted {
		return false, err
	}
	defer release()
	outputs, err := e.buildSeriesMerge(ctx, sources, allowance-scratch)
	if err != nil {
		return false, err
	}
	if len(outputs) >= len(sources) {
		e.discardSeriesOutputs(ctx, outputs)
		return false, nil
	}
	keep := false
	defer func() {
		if !keep {
			e.discardSeriesOutputs(ctx, outputs)
			return
		}
		for _, h := range outputs {
			h.release()
		}
	}()

	if err := ctx.Err(); err != nil {
		return false, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, source := range sources {
		if !slices.Contains(e.seriesTable.roots, source) {
			return false, nil
		}
	}
	retired := make([]bucketindex.SeriesRoot, 0, len(sources))
	for _, source := range sources {
		retired = append(retired, source.binding)
	}
	removed, err := bucketindex.MergeSeriesRoots(e.seriesRetired, retired)
	if err != nil {
		return false, err
	}
	roots := make([]*seriesRootHandle, 0, len(e.seriesTable.roots)-len(sources)+len(outputs))
	for _, h := range e.seriesTable.roots {
		if !slices.Contains(sources, h) {
			roots = append(roots, h)
		}
	}
	roots = append(roots, outputs...)
	next := newSeriesSnapshot(roots)
	next.retired = len(removed)
	old := e.seriesTable
	e.seriesTable, e.seriesRetired = next, removed
	keep = true
	old.release()
	// Like new identity deltas, synced replacement roots remain pending after an unsuccessful CAS.
	if err := e.updateIndexLocked(ctx); err != nil {
		return false, err
	}
	zctx.From(ctx).Debug("compacted series metadata runs",
		zap.Int("roots_in", len(sources)), zap.Int("roots_out", len(outputs)), zap.Int("roots", len(e.seriesTable.roots)))
	return true, nil
}

func (e *Engine) buildSeriesMerge(ctx context.Context, sources []*seriesRootHandle, allowance int64) (_ []*seriesRootHandle, err error) {
	iterators := make([]*seriestable.Iterator, len(sources))
	live := make([]bool, len(sources))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
	}()
	for i, source := range sources {
		iterators[i], err = source.run.Iterate(ctx)
		if err != nil {
			return nil, err
		}
		live[i] = iterators[i].Next()
	}

	output := seriesMergeOutput{engine: e, ctx: ctx, allowance: allowance, first: true}
	defer func() {
		if err != nil {
			e.discardSeriesOutputs(ctx, output.roots)
		}
	}()

	for {
		selected := nextSeriesSource(iterators, live)
		if selected < 0 {
			break
		}
		it := iterators[selected]
		if err := output.add(it.ID(), it.Raw()); err != nil {
			return nil, err
		}
		live[selected] = it.Next()
	}
	for _, it := range iterators {
		if err := it.Err(); err != nil {
			return nil, err
		}
	}
	if err := output.flush(); err != nil {
		return nil, err
	}
	return output.roots, nil
}

func nextSeriesSource(iterators []*seriestable.Iterator, live []bool) int {
	selected := -1
	for i, ok := range live {
		if ok && (selected < 0 || iterators[i].ID().Compare(iterators[selected].ID()) < 0) {
			selected = i
		}
	}
	return selected
}

type seriesMergeOutput struct {
	engine    *Engine
	ctx       context.Context //nolint:containedctx // One merge owns the output writer and context.
	allowance int64
	roots     []*seriesRootHandle
	builder   seriestable.Builder
	previous  signal.SeriesID
	canonical []byte
	identity  signal.Series
	first     bool
}

func (o *seriesMergeOutput) flush() error {
	if o.builder.Count() == 0 {
		return nil
	}
	prefix := o.engine.cfg.Prefix + "/s-" + partid.New().String()
	run, err := seriestable.WriteWithIDCache(o.ctx, o.engine.cfg.Backend, prefix, &o.builder, o.engine.seriesIDs)
	if err != nil {
		o.engine.deletePrivateSeriesRun(o.ctx, prefix)
		return err
	}
	h := &seriesRootHandle{binding: bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: run.Digest()}, run: run}
	h.acquire()
	o.roots = append(o.roots, h)
	o.builder = seriestable.Builder{}
	return nil
}

func (o *seriesMergeOutput) add(id signal.SeriesID, raw []byte) error {
	if !o.first && id == o.previous {
		if !bytes.Equal(o.canonical, raw) {
			return errors.New("engine: conflicting compacted series identity")
		}
		return nil
	}
	if err := o.ctx.Err(); err != nil {
		return err
	}
	single := buildbudget.Mul(int64(len(raw))+512, 128)
	prospective := buildbudget.Add(o.builder.Bytes(), buildbudget.Add(4*o.builder.EncodedBound(), single))
	if prospective > o.allowance {
		if err := o.flush(); err != nil {
			return err
		}
		if buildbudget.Add(o.builder.Bytes(), single) > o.allowance {
			return errors.New("engine: series identity exceeds merge allowance")
		}
	}
	used, err := signal.DecodeSeriesInto(&o.identity, raw)
	if err != nil || used != len(raw) {
		return errors.Wrap(seriestable.ErrCorrupt, "merge canonical identity")
	}
	if err := o.builder.Add(id, o.identity); err != nil {
		return err
	}
	o.canonical = append(o.canonical[:0], raw...)
	o.previous, o.first = id, false
	return nil
}

func boundedSeriesSources(sources []*seriesRootHandle, allowance int64) ([]*seriesRootHandle, int64) {
	for len(sources) >= 2 {
		scratch := int64(16 << 20)
		for _, source := range sources {
			scratch = buildbudget.Add(scratch, 8*source.run.MaxPageBytes()+int64(source.run.Count())/8+4096)
		}
		if scratch < allowance/2 {
			return sources, scratch
		}
		sources = sources[:len(sources)-1]
	}
	return nil, 0
}

func (e *Engine) discardSeriesOutputs(ctx context.Context, roots []*seriesRootHandle) {
	for _, h := range roots {
		h.release()
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for _, h := range roots {
		if cleanup.Err() != nil {
			return
		}
		e.deleteSeriesRun(cleanup, h.binding.Prefix)
	}
}

func (e *Engine) deletePrivateSeriesRun(ctx context.Context, prefix string) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	e.deleteSeriesRun(cleanup, prefix)
}

func (e *Engine) deleteSeriesRun(ctx context.Context, prefix string) {
	keys, err := e.cfg.Backend.List(ctx, prefix+"/")
	if err != nil {
		zctx.From(ctx).Warn("list private series output for cleanup", zap.String("prefix", prefix), zap.Error(err))
		return
	}
	for _, key := range keys {
		if err := e.cfg.Backend.Delete(ctx, key); err != nil {
			zctx.From(ctx).Warn("delete private series output", zap.String("key", key), zap.Error(err))
		}
	}
}
