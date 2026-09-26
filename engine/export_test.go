package engine

import (
	"fmt"
	"maps"
	"slices"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// LosePart drops the part naming prefix from the live set and records the repair obligation its
// absence owes, standing in for the index load that discovers an unreadable part.
func (e *Engine) LosePart(prefix string, blocks bucketindex.Interval) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ent := bucketindex.Entry{Prefix: prefix, Blocks: blocks}
	for _, p := range e.parts {
		if p.prefix == prefix {
			ent.MinTime, ent.MaxTime, ent.Level = p.minTime, p.maxTime, p.level
		}
	}

	e.parts = replaceParts(e.parts, map[string]struct{}{prefix: {}})
	delete(e.indexed, prefix)

	ix := bucketindex.Index{Wanted: e.wants}
	ix.RecordWant(bucketindex.WantOf(ent, e.generation))
	e.wants = ix.Wanted
}

// SetPartBlocks assigns block identity to the live part naming prefix, which parts written before
// that identity existed carry none of.
func (e *Engine) SetPartBlocks(prefix string, blocks bucketindex.Interval, level uint32) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, p := range e.parts {
		if p.prefix == prefix {
			p.blocks, p.level = blocks, level
		}
	}
}

// WantPrefixes reports the outstanding repair obligations.
func (e *Engine) WantPrefixes() []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	out := make([]string, 0, len(e.wants))
	for i := range e.wants {
		out = append(out, e.wants[i].Prefix)
	}

	return out
}

// PartPrefixes reports the live parts' backend prefixes.
func (e *Engine) PartPrefixes() []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	out := make([]string, 0, len(e.parts))
	for _, p := range e.parts {
		out = append(out, p.prefix)
	}

	slices.Sort(out)

	return out
}

// SetMergeReadWindow sets how much of each source column a merge reads ahead.
func (e *Engine) SetMergeReadWindow(n int64) { e.mergeReadWindow = n }

// LoadState snapshots every field an index load replaces, so a test can tell that a failed load
// changed none of them. Part handles compare by identity.
func (e *Engine) LoadState() any {
	e.mu.RLock()
	defer e.mu.RUnlock()

	parts := make([]string, 0, len(e.parts))
	for _, p := range e.parts {
		parts = append(parts, fmt.Sprintf("%s@%p", p.prefix, p))
	}

	foreign := make([]string, 0, len(e.foreignParts))
	for prefix, p := range e.foreignParts {
		foreign = append(foreign, fmt.Sprintf("%s@%p", prefix, p))
	}

	slices.Sort(foreign)

	return struct {
		IndexVersion  backend.Version
		Foreign       []bucketindex.Entry
		ForeignParts  []string
		Parts         []string
		Indexed       map[string]struct{}
		Holes         []bucketindex.Entry
		LostParts     uint64
		Allocated     uint64
		IdentityDirty bool
		FlushedEpoch  uint64
		Epochs        []bucketindex.WriterEpoch
		AnonEpoch     uint64
		Generation    bucketindex.Generation
		Removals      []bucketindex.Removal
		Wants         []bucketindex.Want
		PendingWants  []bucketindex.Want
	}{
		e.indexVersion, slices.Clone(e.foreign), foreign, parts, maps.Clone(e.indexed),
		slices.Clone(e.holes), e.lostParts, e.allocated, e.identityDirty, e.flushedEpoch,
		slices.Clone(e.epochs), e.anonEpoch, e.generation, slices.Clone(e.removals),
		slices.Clone(e.wants), slices.Clone(e.pendingWants),
	}
}

// SetMergeResidentObserver installs fn to receive each streamed merge's peak writer residency, its
// largest run and its resident limit, and returns the restore. Callers must not run in parallel.
func SetMergeResidentObserver(fn func(peak, run, limit int64)) func() {
	old := mergeResidentObserver
	mergeResidentObserver = fn

	return func() { mergeResidentObserver = old }
}
