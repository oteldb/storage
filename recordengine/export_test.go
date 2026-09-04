package recordengine

import (
	"slices"

	"github.com/oteldb/storage/backend/bucketindex"
)

// SetMergeSplitDict forces the merge's byte-column carry on or off for the calling test and returns
// the restore. Off is the flat path every column took before the split (union dictionary + ids)
// carry existed, and is the oracle the split path is compared against.
func SetMergeSplitDict(v bool) func() {
	old := mergeSplitDict
	mergeSplitDict = v

	return func() { mergeSplitDict = old }
}

// ObserveMergeSplit installs fn to receive each merge's per-byte-column decision (true where the
// column took the split path), and returns the removal. It is what keeps a byte-identity test from
// passing by silently exercising the fallback everywhere.
func ObserveMergeSplit(fn func(split []bool)) func() {
	old := mergeSplitObserver
	mergeSplitObserver = fn

	return func() { mergeSplitObserver = old }
}

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
	for _, w := range e.wants {
		out = append(out, w.Prefix)
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
