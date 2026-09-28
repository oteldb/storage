package recordengine

import (
	"slices"

	"github.com/oteldb/storage/backend/bucketindex"
)

// A part's identity in block space is assigned by the commit that publishes it, out of the index
// that commit is building, under the tenure it writes as: see [bucketindex.Plan] and
// [bucketindex.Allocator], which both engines share.

// blockAssignment is an identity [Engine.nextIndexLocked] chose while building one candidate
// index. It is written onto the part only by the commit that lands: a part numbered before its
// CAS succeeds keeps a block the winner just took, and the retry — seeing it already numbered —
// would never re-allocate.
type blockAssignment struct {
	part   *part
	blocks bucketindex.Interval
	claim  bucketindex.Claim
	level  uint32
	term   uint64
}

// planFlushBlocks marks p as a flush output: a fresh block at level 0.
func planFlushBlocks(p *part) { p.pending = bucketindex.FlushPlan() }

// planMergeBlocks stamps the identity the outputs of a merge over src claim: see
// [bucketindex.PlanMerge].
func planMergeBlocks(src, out []*part) {
	ents := make([]bucketindex.Entry, len(src))
	for i, p := range src {
		ents[i] = bucketindex.Entry{Blocks: p.blocks, Claim: p.claim, Level: p.level}
	}

	for i, plan := range bucketindex.PlanMerge(ents, len(out)) {
		out[i].pending = plan
	}
}

// nextBlockLocked is [bucketindex.Index.NextBlock] over the whole state the commit under
// construction publishes: the persisted high-water mark, the entries already added — a rival
// writer's included, since its blocks are real claims and reusing one would make two different
// parts share an identity — this engine's own parts, the holes it carries, and every want
// outstanding or pending, whose part may yet be repaired back into the index. Caller holds e.mu.
func (e *Engine) nextBlockLocked(ix *bucketindex.Index) bucketindex.Block {
	seed := bucketindex.Index{
		Entries:         slices.Concat(ix.Entries, e.holes),
		Wanted:          slices.Concat(e.wants, e.pendingWants, e.adoptedWants, e.pendingHoles),
		AllocatedBlocks: e.allocated,
	}

	for _, p := range e.parts {
		seed.Entries = append(seed.Entries, bucketindex.Entry{Blocks: p.blocks, Claim: p.claim})
	}

	return seed.NextBlock(e.term())
}
