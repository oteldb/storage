package engine

import (
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/signal"
)

// A part's rollup layout lives in its manifest and is carried in its index entry
// ([bucketindex.Entry.Rollup]), so a writer can check an adopted part's layout without opening it.

// entryRollup is the layout p's index entry records: its manifest's, or unknown.
func entryRollup(p *part) *bucketindex.Rollup {
	if !p.rollupKnown {
		return nil
	}

	r := &bucketindex.Rollup{}
	for _, t := range p.rollup {
		r.Tiers = append(r.Tiers, bucketindex.RollupTier{Before: t.Before, Interval: t.Interval, Agg: uint8(t.Agg)})
	}

	return r
}

// entryTiers is the layout an entry records. An unknown one — an entry no commit has filled in since
// its part was written, and a part whose manifest records none — is raw, as it is to every layout
// check: it constrains nothing.
func entryTiers(r *bucketindex.Rollup) []DownsampleTier {
	if r == nil {
		return nil
	}

	out := make([]DownsampleTier, 0, len(r.Tiers))
	for _, t := range r.Tiers {
		out = append(out, DownsampleTier{Before: t.Before, Interval: t.Interval, Agg: signal.Aggregation(t.Agg)})
	}

	return out
}

// foreignLayoutLocked is the layout of the adopted entry ent: its part's manifest where the part is
// open, else the layout the entry records. Caller holds e.mu.
func (e *Engine) foreignLayoutLocked(ent *bucketindex.Entry) []DownsampleTier {
	if p, ok := e.foreignParts[ent.Prefix]; ok && p.rollupKnown {
		return p.rollup
	}

	return entryTiers(ent.Rollup)
}

// unopenedLayoutsLocked stands in, by the layout its entry records, for each adopted part this engine
// could not open, so a merge plans against a rival's layout it cannot read. A stand-in carries a
// prefix, a time range and a layout and no reader: only the layout checks read it ([mergeCohorts],
// [resolvePolicy], the commit's [rollupGuard]), never a decode or a selection. Caller holds e.mu.
func (e *Engine) unopenedLayoutsLocked() []*part {
	var out []*part

	for i := range e.foreign {
		ent := &e.foreign[i]
		if _, open := e.foreignParts[ent.Prefix]; open || ent.Rollup == nil {
			continue
		}

		out = append(out, &part{
			prefix: ent.Prefix, minTime: ent.MinTime, maxTime: ent.MaxTime,
			rollup: entryTiers(ent.Rollup), rollupKnown: true,
		})
	}

	return out
}

// fillRollup records an adopted part's layout in its entry once the part opens, for the next commit
// to publish: an entry written before the index carried layouts is filled in by the first writer
// that reads the part.
func fillRollup(ent *bucketindex.Entry, p *part) {
	if ent.Rollup == nil {
		ent.Rollup = entryRollup(p)
	}
}
