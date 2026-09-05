package recordengine

import (
	"slices"

	"github.com/oteldb/storage/backend/bucketindex"
)

// A part's identity in block-number space is assigned by the commit that publishes it, out of the
// index that commit is building. Allocation is the shard owner's alone and needs no coordination:
// the CAS that adds the part is what claims the number, so a writer that loses the race re-reads
// and allocates above the winner.

// blockPlan is the identity a newly written part awaits. A flush leaves blocks unset, so the
// commit allocates a fresh number for it; a merge inherits the union of its inputs' intervals,
// which is what makes the output supersede them.
type blockPlan struct {
	blocks bucketindex.Interval
	level  uint32
}

// blockAssignment is an identity [Engine.nextIndexLocked] chose while building one candidate
// index. It is written onto the part only by the commit that lands: a part numbered before its
// CAS succeeds keeps a block the winner just took, and the retry — seeing it already numbered —
// would never re-allocate.
type blockAssignment struct {
	part   *part
	blocks bucketindex.Interval
	level  uint32
}

// planFlushBlocks marks p as a flush output: a fresh block at level 0.
func planFlushBlocks(p *part) { p.pending = &blockPlan{} }

// planMergeBlocks stamps the identity the outputs of a merge over src claim.
//
// A merge does not allocate. The output covers the blocks its inputs covered, one level above the
// deepest of them, and that is precisely what makes supersession decidable from identity alone —
// so a repair can accept the successor of the part it wanted.
//
// Two cases allocate instead. A merge whose inputs all predate format v5 has no interval to
// inherit; a fresh one is how such a part migrates, a merge being the only thing that rewrites it.
// And a merge that splits its output over several parts cannot hand any of them the union: none
// holds all of that data, so a successor claim would answer a want with a fraction of the part.
// Both leave the outputs superseding nothing, which costs a repair the fallback to exact-prefix
// matching and is corrected by the next merge.
//
// A mixed merge — some inputs carrying an interval, some not — inherits the union of the ones that
// do. It cannot do better: a want naming a pre-v5 part records that part's unset interval, so no
// claim the output makes could contain it.
func planMergeBlocks(src, out []*part) {
	var (
		union bucketindex.Interval
		level uint32
	)

	for _, p := range src {
		level = max(level, p.level+1)
		union = union.Union(p.blocks)
	}

	inherit := len(out) == 1 && union.Valid()

	for _, p := range out {
		id := &blockPlan{level: level}
		if inherit {
			id.blocks = union
		}

		p.pending = id
	}
}

// nextBlockLocked is [bucketindex.Index.NextBlock] over the whole state the commit under
// construction publishes: the entries already added — a rival writer's included, since its blocks
// are real claims and reusing one would make two different parts share an identity — this engine's
// own parts, the holes it carries, and every want outstanding or pending, whose part may yet be
// repaired back into the index. Caller holds e.mu.
func (e *Engine) nextBlockLocked(ix *bucketindex.Index) uint64 {
	seed := bucketindex.Index{
		Entries: slices.Concat(ix.Entries, e.holes),
		Wanted:  slices.Concat(e.wants, e.pendingWants, e.pendingHoles),
	}

	for _, p := range e.parts {
		seed.Entries = append(seed.Entries, bucketindex.Entry{Blocks: p.blocks})
	}

	return seed.NextBlock()
}
