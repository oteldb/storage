package engine

import (
	"bytes"
	"context"
	"slices"

	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

type diskLabelCollector struct {
	plan     *enginePlan
	seen     map[string]struct{}
	name     []byte
	values   bool
	knownIDs []signal.SeriesID
}

// diskLabels reads covered part dictionaries, resolving only selected identities for filtered reads.
func (e *Engine) diskLabels(ctx context.Context, r fetch.Request, name []byte, values bool) ([]string, error) {
	ctx, budget := diskQueryBudget(ctx)
	c := &diskLabelCollector{
		plan: &enginePlan{engine: e, selectionBudget: budget}, seen: make(map[string]struct{}), name: name, values: values,
	}
	if len(r.Matchers) > 0 {
		p, err := e.planDiskQuery(ctx, r, true, true, false)
		if err != nil {
			return nil, err
		}
		c.plan = p
	}
	defer c.plan.releaseParts()
	var err error
	if len(r.Matchers) > 0 {
		err = c.collectMatched(ctx)
	} else {
		err = c.collectBroad(ctx, r)
	}
	if err != nil {
		return nil, err
	}

	if err := c.plan.reserveSelection(buildbudget.Mul(int64(len(c.seen)), memsize.Of[string]())); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(c.seen))
	for s := range c.seen {
		out = append(out, s)
	}
	slices.Sort(out)
	return out, nil
}

func (c *diskLabelCollector) add(raw []byte) error {
	if _, ok := c.seen[string(raw)]; ok {
		return nil
	}
	if err := c.plan.reserveSelection(int64(len(raw)) + 2*memsize.MapEntry[string, struct{}]()); err != nil {
		return err
	}
	c.seen[string(raw)] = struct{}{}
	return nil
}

func (c *diskLabelCollector) addValue(v signal.Value) error {
	workspace := buildbudget.Mul(buildbudget.Attributes(signal.Attributes{{Value: v}}), 4)
	if err := c.plan.selectionBudget.Reserve(workspace); err != nil {
		return err
	}
	defer c.plan.selectionBudget.Release(workspace)
	return c.add(v.AppendText(nil))
}

func (c *diskLabelCollector) project(s signal.Series) error {
	if c.values {
		if v, ok := seriesLabelValue(s, c.name); ok {
			return c.addValue(v)
		}
		return nil
	}
	var err error
	forEachLabelName(s, func(name []byte) {
		if err == nil {
			err = c.add(name)
		}
	})
	return err
}

// Broad dictionaries expose every OTel source's values, while filtered projections use precedence.
func (c *diskLabelCollector) projectBroad(s signal.Series) error {
	if !c.values {
		return c.project(s)
	}
	for _, attrs := range [3]signal.Attributes{s.Resource.Attributes, s.Scope.Attributes, s.Attributes} {
		for _, kv := range attrs {
			if bytes.Equal(kv.Key, c.name) {
				if err := c.addValue(kv.Value); err != nil {
					return err
				}
			}
		}
	}
	if bytes.Equal(c.name, labelScopeName) && len(s.Scope.Name) > 0 {
		if err := c.addValue(signal.StringValue(s.Scope.Name)); err != nil {
			return err
		}
	}
	if bytes.Equal(c.name, labelScopeVersion) && len(s.Scope.Version) > 0 {
		return c.addValue(signal.StringValue(s.Scope.Version))
	}
	return nil
}

func (c *diskLabelCollector) collectMatched(ctx context.Context) error {
	p := c.plan
	for _, pt := range p.liveParts {
		if err := pt.index.intersectMark(ctx, p.ids, p.memActive); err != nil {
			return err
		}
	}
	for i, active := range p.memActive {
		if active {
			if err := c.project(p.series[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *Engine) captureDiskLabelHead(ctx context.Context, r fetch.Request, c *diskLabelCollector) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.fenceLocked(); err != nil {
		return err
	}
	e.acquireWindowParts(c.plan, r.Start, r.End, false)
	if e.head.known != nil {
		c.plan.knownSnapshot = e.head.known.snapshot
		c.plan.knownSnapshot.acquire()
	}
	for _, buffers := range [2]map[signal.SeriesID]*sampleBuf{e.head.samples, e.flushing} {
		for id, buf := range buffers {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !bufHasInWindow(buf, r.Start, r.End) {
				continue
			}
			if e.head.known.has(id) {
				if err := c.plan.reserveSelection(32); err != nil {
					return err
				}
				c.knownIDs = append(c.knownIDs, id)
			}
			if s, ok := e.head.series.Get(id); ok {
				if err := c.projectBroad(s); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *diskLabelCollector) collectPart(ctx context.Context, r fetch.Request, pt *part) error {
	ctx, release := withPartSeriesSnapshot(ctx, pt)
	defer release()
	workspace, err := pt.diskSeriesWorkspace(ctx, 0)
	if err != nil {
		return err
	}
	if err := c.plan.selectionBudget.Reserve(workspace); err != nil {
		return err
	}
	defer c.plan.selectionBudget.Release(workspace)
	ix, err := pt.seriesIndex(ctx)
	if err != nil {
		return err
	}
	if ix != nil && pt.minTime >= r.Start && pt.maxTime <= r.End {
		if c.values {
			return ix.Values(ctx, c.name, c.addValue)
		}
		return ix.Names(ctx, c.add)
	}
	return pt.visitDiskSeries(ctx, nil, func(_ signal.SeriesID, raw []byte, _ seriesindex.Metadata) error {
		expanded := buildbudget.Mul(int64(len(raw)), 128)
		if err := c.plan.selectionBudget.Reserve(expanded); err != nil {
			return err
		}
		defer c.plan.selectionBudget.Release(expanded)
		s, _, err := signal.DecodeSeries(raw)
		if err != nil {
			return err
		}
		return c.projectBroad(s)
	})
}

func (c *diskLabelCollector) collectBroad(ctx context.Context, r fetch.Request) error {
	if err := c.plan.engine.captureDiskLabelHead(ctx, r, c); err != nil {
		return err
	}
	if c.plan.knownSnapshot != nil {
		workspace := int64(len(c.knownIDs)) * 16 // Root readers reserve their own page workspace.
		if err := c.plan.selectionBudget.Reserve(workspace); err != nil {
			return err
		}
		defer c.plan.selectionBudget.Release(workspace)
	}
	if err := c.plan.knownSnapshot.lookupKnownBatch(ctx, c.knownIDs, func(_ signal.SeriesID, raw []byte) error {
		n := buildbudget.Mul(int64(len(raw)), 128)
		if err := c.plan.selectionBudget.Reserve(n); err != nil {
			return err
		}
		defer c.plan.selectionBudget.Release(n)
		decoded, _, err := signal.DecodeSeries(raw)
		if err != nil {
			return err
		}
		return c.projectBroad(decoded)
	}); err != nil {
		return err
	}

	for _, pt := range c.plan.liveParts {
		if err := c.collectPart(ctx, r, pt); err != nil {
			return err
		}
	}
	return nil
}
