package engine

import (
	"context"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/watermark"
)

type mergeWatermarkPreflight struct {
	entries  map[*part][]watermark.Entry
	fallback map[*part]bool
}

func (e *Engine) prepareMergeWatermarks(ctx context.Context, cohorts []mergeCohort, readable []*part,
	opts MergeOptions, capBytes int64, idle int, turn uint64, selection boundedMergeSelection,
) (boundedMergeSelection, error) {
	if selection.allowance == math.MaxInt64 {
		return selection, nil
	}
	local := mergeWatermarkPreflight{entries: make(map[*part][]watermark.Entry), fallback: make(map[*part]bool)}
	defer clear(local.entries)
	for {
		if err := ctx.Err(); err != nil {
			return boundedMergeSelection{}, err
		}
		local.retain(selection.parts)
		changed, err := local.read(ctx, selection.parts)
		if err != nil {
			return boundedMergeSelection{}, err
		}
		if changed {
			selection, err = e.selectMergeInputsWithWatermarks(ctx, cohorts, readable, opts, capBytes, idle, turn,
				selection.allowance, local.fallback)
			if err != nil {
				return boundedMergeSelection{}, err
			}
			continue
		}
		// Every selected sidecar is validated or its timestamp fallback has been priced.
		if err := ctx.Err(); err != nil {
			return boundedMergeSelection{}, err
		}
		if err := local.install(ctx, selection.parts); err != nil {
			return boundedMergeSelection{}, err
		}
		start := minInt64
		if opts.RetainFrom > 0 {
			start = opts.RetainFrom
		}
		bound, err := e.mergeInputBound(ctx, selection.parts, len(selection.parts) == 1 && !splitsOutput(selection.parts, start))
		if err != nil {
			return boundedMergeSelection{}, err
		}
		bound.resident = buildbudget.Add(bound.resident, mergeSeriesScratchBound(selection.parts, selection.tiers))
		selection.inputBytes = bound.bytes()
		return selection, nil
	}
}

func (m *mergeWatermarkPreflight) retain(src []*part) {
	selected := make(map[*part]bool, len(src))
	for _, p := range src {
		selected[p] = true
	}
	for p := range m.entries {
		if !selected[p] {
			delete(m.entries, p)
		}
	}
}

func (m *mergeWatermarkPreflight) read(ctx context.Context, src []*part) (changed bool, err error) {
	for _, p := range src {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		p.seriesMaxMu.Lock()
		cached := p.seriesMax != nil
		p.seriesMaxMu.Unlock()
		if cached || m.entries[p] != nil || m.fallback[p] {
			continue
		}
		_, _, sizeErr := backend.NativeSizeOf(ctx, p.be, watermark.Key(p.prefix))
		if errors.Is(sizeErr, backend.ErrNotExist) {
			m.fallback[p], changed = true, true
			continue
		}
		if sizeErr != nil {
			return false, sizeErr
		}
		data, readErr := p.be.Read(ctx, watermark.Key(p.prefix))
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var entries []watermark.Entry
		if readErr == nil {
			entries, readErr = watermark.Decode(data)
		}
		if readErr != nil || len(entries) != p.index.seriesCount() {
			m.fallback[p], changed = true, true
			continue
		}
		m.entries[p] = entries
	}
	return changed, nil
}

func (m *mergeWatermarkPreflight) install(ctx context.Context, src []*part) error {
	for _, p := range src {
		if err := m.installPart(ctx, p); err != nil {
			return err
		}
		delete(m.entries, p)
	}
	return nil
}

func (m *mergeWatermarkPreflight) installPart(ctx context.Context, p *part) error {
	p.seriesMaxMu.Lock()
	defer p.seriesMaxMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.seriesMax != nil {
		return nil
	}
	if entries, ok := m.entries[p]; ok {
		p.seriesMax = entries
		return nil
	}
	if !m.fallback[p] {
		return errors.New("merge watermark preflight has no result")
	}
	entries, err := p.seriesMaxTimes(ctx)
	if err != nil {
		return err
	}
	p.seriesMax = entries
	return nil
}
