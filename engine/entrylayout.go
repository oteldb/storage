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

// unopenedLayoutsLocked supplies reader-less bounds and layouts for adopted parts and unresolved
// losses. They constrain layout compatibility and rollup closure, never merge decoding. Caller holds e.mu.
func (e *Engine) unopenedLayoutsLocked() []*part {
	var out []*part

	for i := range e.foreign {
		ent := &e.foreign[i]
		if _, open := e.foreignParts[ent.Prefix]; open {
			continue
		}

		out = append(out, &part{
			prefix: ent.Prefix, minTime: ent.MinTime, maxTime: ent.MaxTime,
			rollup: entryTiers(ent.Rollup), rollupKnown: ent.Rollup != nil,
		})
	}

	for _, wants := range [][]bucketindex.Want{e.wants, e.pendingWants, e.adoptedWants, e.pendingHoles} {
		for i := range wants {
			w := &wants[i]
			lo, hi := w.MinTime, w.MaxTime
			if lo == 0 && hi == 0 {
				lo, hi = minInt64, maxInt64
			}
			out = append(out, &part{prefix: w.Prefix, minTime: lo, maxTime: hi})
		}
	}
	if e.head != nil && e.head.bytes > 0 {
		lo := minInt64
		if e.head.bufferedKnown {
			lo = e.head.bufferedMin
		}
		out = append(out, &part{prefix: "\x00head", minTime: lo, maxTime: e.head.newest})
	}
	if e.head != nil && e.head.detachedBytes > 0 {
		lo := minInt64
		if e.head.detachedKnown {
			lo = e.head.detachedMin
		}
		out = append(out, &part{prefix: "\x00frozen", minTime: lo, maxTime: e.head.newest})
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
