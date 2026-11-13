package engine

import (
	"bytes"
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// captureKnownQuery pins identity ownership in the same lock interval as sample snapshots.
func (e *Engine) captureKnownQuery(p *enginePlan) {
	if e.head.known == nil {
		return
	}
	p.knownSnapshot = e.head.known.snapshot
	p.knownSnapshot.acquire()
	p.knownIDs = make(map[signal.SeriesID]bool, e.head.known.len())
	for id := range e.head.known.marks {
		p.knownIDs[id] = true
	}
}

func (p *enginePlan) resolveKnownQuery(ctx context.Context, r fetch.Request, withIdentity bool) error {
	if p.knownSnapshot == nil || p.knownResolved || !withIdentity && len(r.Matchers) == 0 {
		return nil
	}
	workspace := int64(len(p.ids)) * 32 // Root readers reserve their own page workspace.
	if err := p.selectionBudget.Reserve(workspace); err != nil {
		return err
	}
	defer p.selectionBudget.Release(workspace)
	ids := make([]signal.SeriesID, 0, len(p.knownIDs))
	for _, id := range p.ids {
		if p.knownIDs[id] {
			ids = append(ids, id)
		}
	}
	return p.knownSnapshot.lookupKnownBatch(ctx, ids, func(id signal.SeriesID, raw []byte) error {
		i, found := slices.BinarySearchFunc(p.ids, id, signal.SeriesID.Compare)
		if !found {
			return errors.New("engine: known query selection changed")
		}
		return p.resolveKnownIdentity(raw, r, i, id, withIdentity)
	})
}

func (p *enginePlan) resolveKnownIdentity(raw []byte, r fetch.Request, i int, id signal.SeriesID, withIdentity bool) error {
	n := buildbudget.Mul(int64(len(raw)), 128)
	if err := p.selectionBudget.Reserve(n); err != nil {
		return err
	}
	if withIdentity {
		p.selectionBytes += n
		raw = bytes.Clone(raw)
	} else {
		defer p.selectionBudget.Release(n)
	}
	s, used, err := signal.DecodeSeries(raw)
	if err != nil {
		return err
	}
	if used != len(raw) {
		return errors.New("engine: known identity trailing bytes")
	}
	if !p.knownFiltered {
		p.knownIDs[id] = seriesindex.MatchesSeries(s, r.Matchers)
	}
	if withIdentity {
		p.series[i] = s
	}
	return nil
}
