package engine

import (
	"slices"

	"github.com/oteldb/storage/backend/bucketindex"
)

// A part's identity in block-number space is assigned by the commit that publishes it, out of the
// index that commit is building. Allocation is the shard owner's alone and needs no coordination:
// the CAS that adds the part is what claims the number, so a writer that loses the race re-reads
// and allocates above the winner.

// blockPlan is the identity a newly written part awaits. A flush leaves blocks unset, so the
// commit allocates a fresh number for it; a merge that writes one part inherits the set of blocks
// its inputs covered, which is what makes the output supersede them.
type blockPlan struct {
	blocks bucketindex.Interval
	claim  bucketindex.Claim
	// group is set on the outputs of a merge that split, and shared by all of them: the commit
	// allocates the whole run at once so the fragments can name each other.
	group *splitGroup
	level uint32
}

// splitGroup is the joint claim the outputs of one split merge carry. The ancestor blocks are
// known when the merge is planned; the group's own blocks are not, because allocation happens per
// commit attempt — see [Engine.nextIndexLocked].
type splitGroup struct {
	blocks bucketindex.Interval
	n      int
}

// blockAssignment is an identity [Engine.nextIndexLocked] chose while building one candidate
// index. It is written onto the part only by the commit that lands: a part numbered before its
// CAS succeeds keeps a block the winner just took, and the retry — seeing it already numbered —
// would never re-allocate.
type blockAssignment struct {
	part   *part
	blocks bucketindex.Interval
	claim  bucketindex.Claim
	level  uint32
}

// planFlushBlocks marks p as a flush output: a fresh block at level 0.
func planFlushBlocks(p *part) { p.pending = &blockPlan{} }

// planMergeBlocks stamps the identity the outputs of a merge over src claim.
//
// A merge that writes one part does not allocate. The output covers exactly the blocks its inputs
// covered — a set, so a run that straddles a gap claims no block in it — one level above the
// deepest input, and that is precisely what makes supersession decidable from identity alone, so a
// repair can accept the successor of the part it wanted.
//
// A merge that splits its output cannot hand any fragment that set: none holds all of the data, so
// a successor claim would answer a want with a fraction of the part. The fragments take a fresh run
// of blocks instead and carry the inputs' set as a joint [bucketindex.Claim] over that run — which
// is what keeps the lineage: the claim is realized wherever every member of the run is present, and
// a later merge that consumes them all folds it back into an ordinary interval.
//
// A merge whose inputs all predate format v5 has no set to inherit; a fresh block is how such a
// part migrates, a merge being the only thing that rewrites it. A mixed merge — some inputs
// carrying an interval, some not — inherits the union of the ones that do. It cannot do better: a
// want naming a pre-v5 part records that part's unset interval, so no claim the output makes could
// contain it.
//
// One limit is deliberate: an output carries at most one unrealized claim, the widest, so a merge
// consuming fragments of two groups without completing either keeps the lineage of one. The other
// falls back to exact-prefix matching, which is where every split left it before claims existed.
func planMergeBlocks(src, out []*part) {
	var (
		union  bucketindex.Interval
		level  uint32
		claims []bucketindex.Claim
	)

	for _, p := range src {
		level = max(level, p.level+1)
		union = union.Union(p.blocks)

		if p.claim.Valid() {
			claims = append(claims, p.claim)
		}
	}

	union, claims = realizeClaims(union, claims)

	if len(out) == 1 {
		out[0].pending = &blockPlan{blocks: union, claim: widestClaim(claims), level: level}

		return
	}

	group := &splitGroup{blocks: union, n: len(out)}
	for _, p := range out {
		p.pending = &blockPlan{group: group, level: level}
	}
}

// realizeClaims folds into held every claim whose group is wholly inside it, repeating while that
// keeps realizing more — a group whose members were themselves split resolves only after the inner
// one has. It returns what is covered and the claims still owed a member.
func realizeClaims(held bucketindex.Interval, claims []bucketindex.Claim) (bucketindex.Interval, []bucketindex.Claim) {
	for len(claims) > 0 {
		rest := claims[:0]

		for _, c := range claims {
			if held.Contains(c.Group) {
				held = held.Union(c.Blocks)

				continue
			}

			rest = append(rest, c)
		}

		if len(rest) == len(claims) {
			break
		}

		claims = rest
	}

	return held, claims
}

func widestClaim(claims []bucketindex.Claim) bucketindex.Claim {
	var best bucketindex.Claim
	for _, c := range claims {
		if c.Blocks.Len() > best.Blocks.Len() {
			best = c
		}
	}

	return best
}

// nextBlockLocked is [bucketindex.Index.NextBlock] over the whole state the commit under
// construction publishes: the persisted high-water mark, the entries already added — a rival
// writer's included, since its blocks are real claims and reusing one would make two different
// parts share an identity — this engine's own parts, the holes it carries, and every want
// outstanding or pending, whose part may yet be repaired back into the index. Caller holds e.mu.
func (e *Engine) nextBlockLocked(ix *bucketindex.Index) uint64 {
	seed := bucketindex.Index{
		Entries:         slices.Concat(ix.Entries, e.holes),
		Wanted:          slices.Concat(e.wants, e.pendingWants, e.pendingHoles),
		AllocatedBlocks: e.allocated,
	}

	for _, p := range e.parts {
		seed.Entries = append(seed.Entries, bucketindex.Entry{Blocks: p.blocks, Claim: p.claim})
	}

	return seed.NextBlock()
}

// groupRun is the run of blocks one commit attempt allocated to a split group, and how far into it
// the fragments assigned so far have got.
type groupRun struct {
	run  bucketindex.Interval
	next uint64
}

// allocateBlocks turns a pending plan into a real identity, taking numbers from *next.
//
// A split group takes its whole run at once, before any of its fragments is numbered, so every
// fragment carries the same claim and can name the siblings a repair still has to fetch.
func allocateBlocks(
	id *blockPlan, next *uint64, groups map[*splitGroup]*groupRun,
) (bucketindex.Interval, bucketindex.Claim) {
	if id.blocks.Valid() {
		return id.blocks, id.claim
	}

	if id.group == nil {
		iv := bucketindex.Interval{Min: *next, Max: *next}
		*next++

		return iv, id.claim
	}

	g, ok := groups[id.group]
	if !ok {
		g = &groupRun{
			run:  bucketindex.Interval{Min: *next, Max: *next + uint64(id.group.n) - 1},
			next: *next,
		}
		groups[id.group] = g
		*next = g.run.Max + 1
	}

	iv := bucketindex.Interval{Min: g.next, Max: g.next}
	g.next++

	claim := bucketindex.Claim{Blocks: id.group.blocks, Group: g.run}
	if !claim.Valid() {
		return iv, bucketindex.Claim{}
	}

	return iv, claim
}
