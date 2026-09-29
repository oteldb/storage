package bucketindex

import (
	"cmp"
	"math"
	"slices"
)

// Block is one allocated block id: the ownership term that allocated it and its number within that
// term. A tenure allocates only under its own term, so two tenures of a shard — a displaced owner and
// its successor, or two replicas whose lineages diverged — never hand out the same id, with no
// coordination beyond the claim that already orders them. Term 0 is the writer with no cluster, and
// every block an index written before format v7 names.
//
// Blocks order by term, then number. Numbers start at 1 within each term, so {term, 0} is never a
// block, only the point below a term's first one.
type Block struct {
	Term uint64
	N    uint64
}

// Compare orders two blocks, as [cmp.Compare] does.
func (b Block) Compare(o Block) int {
	switch {
	case b.Term != o.Term:
		return cmp.Compare(b.Term, o.Term)
	default:
		return cmp.Compare(b.N, o.N)
	}
}

// next is the successor in block order. A number space that ends wraps into the next term.
func (b Block) next() Block {
	if b.N == math.MaxUint64 {
		return Block{Term: b.Term + 1}
	}

	return Block{Term: b.Term, N: b.N + 1}
}

// prev is the predecessor in block order, the inverse of [Block.next].
func (b Block) prev() Block {
	if b.N == 0 {
		return Block{Term: b.Term - 1, N: math.MaxUint64}
	}

	return Block{Term: b.Term, N: b.N - 1}
}

func (b Block) less(o Block) bool { return b.Compare(o) < 0 }

func minBlock(a, b Block) Block {
	if a.less(b) {
		return a
	}

	return b
}

// MaxBlock is the later of a and b in block order.
func MaxBlock(a, b Block) Block {
	if a.less(b) {
		return b
	}

	return a
}

// Interval is the exact set of blocks a part covers, the identity it carries alongside its prefix.
// A flush writes {b}; a merge writes the union of what its inputs covered.
//
// The set is stored as its bounds plus the runs of blocks it does *not* hold, because a part's
// blocks are contiguous in every ordinary case and the gap list is then empty. It is a set and not
// a hull because supersession means "holds these rows": merging the parts on either side of a lost
// one yields {1,3}, and a hull would claim block 2 — a want discharged by a part holding none of
// its data. A merge across tenures holds blocks of several terms, and the space between them is a
// gap like any other.
//
// The zero value is *unset* rather than a range covering block {0, 0}. That distinction is what
// keeps a part written before format v5 — which carries no interval — from accidentally containing,
// or being contained by, anything: see [Interval.Valid].
type Interval struct {
	Min Block
	Max Block
	// Gaps are the runs inside (Min, Max) the set does not cover, in ascending order, disjoint and
	// non-adjacent. The canonical form is the only valid one: two encodings of one set would break
	// both equality and the encode∘decode identity.
	Gaps []Gap
}

// Gap is a run of blocks an [Interval] skips: the blocks from Min to Max inclusive that lie inside
// the interval's bounds but that no part in it holds.
type Gap struct {
	Min Block
	Max Block
}

// Blocks builds the term-0 interval covering exactly the given block numbers: see [TermBlocks].
func Blocks(nums ...uint64) Interval { return TermBlocks(0, nums...) }

// TermBlocks builds the interval covering exactly the given block numbers of term, in any order and
// with duplicates. It returns the unset interval for an empty list or one naming block 0.
func TermBlocks(term uint64, nums ...uint64) Interval {
	runs := make([]Gap, 0, len(nums))
	for _, n := range nums {
		if n == 0 {
			return Interval{}
		}

		b := Block{Term: term, N: n}
		runs = append(runs, Gap{Min: b, Max: b})
	}

	return fromRuns(runs)
}

// Range is the contiguous run of term's blocks lo..hi. It is not validated: an inverted or
// zero-touching range is simply not [Interval.Valid].
func Range(term, lo, hi uint64) Interval {
	return Interval{Min: Block{Term: term, N: lo}, Max: Block{Term: term, N: hi}}
}

// Valid reports whether the interval names a real set of blocks: every held run lies within one
// term and starts at number 1 or above, the bounds are ordered, and the gap list is canonical.
// Everything else — the zero value of a pre-v5 entry, and any inversion or denormalization a
// corrupt or hostile encoding could produce — is unset, and takes part in no containment.
func (iv Interval) Valid() bool {
	if iv.Max.less(iv.Min) {
		return false
	}

	lo := iv.Min
	for _, g := range iv.Gaps {
		// Strictly inside the bounds, and separated from the previous gap by at least one held
		// block: otherwise the set has different bounds, or a shorter gap list describes it.
		if g.Max.less(g.Min) || !lo.less(g.Min) || !g.Max.less(iv.Max) {
			return false
		}

		if !validRun(lo, g.Min.prev()) {
			return false
		}

		lo = g.Max.next()
	}

	return validRun(lo, iv.Max)
}

func validRun(lo, hi Block) bool { return lo.N >= 1 && lo.Term == hi.Term && !hi.less(lo) }

// Contains reports whether iv covers every block o covers. Both intervals must be valid: an unset
// interval neither contains nor is contained, which is the fallback to exact-prefix matching for
// parts written before v5.
func (iv Interval) Contains(o Interval) bool {
	if !iv.Valid() || !o.Valid() {
		return false
	}

	i, n := 0, iv.nruns()

	for j := range o.nruns() {
		r := o.run(j)
		for i < n && iv.run(i).Max.less(r.Min) {
			i++
		}

		if i >= n {
			return false
		}

		if h := iv.run(i); r.Min.less(h.Min) || h.Max.less(r.Max) {
			return false
		}
	}

	return true
}

// Union is the set of blocks either covers, ignoring an unset operand. It is what a merge output
// claims: the blocks its inputs covered, which is what makes it supersede them.
func (iv Interval) Union(o Interval) Interval {
	switch {
	case !o.Valid():
		return iv
	case !iv.Valid():
		return o
	default:
		return fromRuns(o.appendRuns(iv.appendRuns(make([]Gap, 0, iv.nruns()+o.nruns()))))
	}
}

// Equal reports whether iv and o have the same encoding, which for canonical intervals is set
// equality.
func (iv Interval) Equal(o Interval) bool {
	return iv.Min == o.Min && iv.Max == o.Max && slices.Equal(iv.Gaps, o.Gaps)
}

// Len reports how many blocks the interval covers, 0 if unset. It orders candidate successors
// by how much of the shard they subsume.
func (iv Interval) Len() uint64 {
	if !iv.Valid() {
		return 0
	}

	var n uint64
	for i := range iv.nruns() {
		r := iv.run(i)
		n += r.Max.N - r.Min.N + 1
	}

	return n
}

// Each calls fn for every block the interval covers, in ascending order, stopping early if fn
// returns false. It is how a caller enumerates the members of a split group it is missing.
func (iv Interval) Each(fn func(Block) bool) {
	if !iv.Valid() {
		return
	}

	for i := range iv.nruns() {
		r := iv.run(i)
		for n := r.Min.N; ; n++ {
			if !fn(Block{Term: r.Min.Term, N: n}) {
				return
			}

			if n == r.Max.N {
				break
			}
		}
	}
}

// nruns and run enumerate the ascending, disjoint runs of blocks the interval holds, without
// materializing them: the relations over identity run per entry on every commit. They assume
// validity.
func (iv Interval) nruns() int { return len(iv.Gaps) + 1 }

func (iv Interval) run(i int) Gap {
	r := Gap{Min: iv.Min, Max: iv.Max}
	if i > 0 {
		r.Min = iv.Gaps[i-1].Max.next()
	}

	if i < len(iv.Gaps) {
		r.Max = iv.Gaps[i].Min.prev()
	}

	return r
}

func (iv Interval) appendRuns(dst []Gap) []Gap {
	for i := range iv.nruns() {
		dst = append(dst, iv.run(i))
	}

	return dst
}

// fromRuns is the inverse of [Interval.runs] over an arbitrary run list: it sorts, coalesces and
// derives the canonical bounds-and-gaps form. A run naming a block numbered 0 makes the whole set
// unset, as it does everywhere else.
func fromRuns(runs []Gap) Interval {
	if len(runs) == 0 {
		return Interval{}
	}

	slices.SortFunc(runs, func(a, b Gap) int {
		if c := a.Min.Compare(b.Min); c != 0 {
			return c
		}

		return a.Max.Compare(b.Max)
	})

	merged := runs[:1]

	for _, r := range runs {
		if r.Min.N == 0 {
			return Interval{}
		}
	}

	for _, r := range runs[1:] {
		last := &merged[len(merged)-1]
		// r.Min.prev(), not last.Max.next(): the top of a term's number space is a legal block,
		// and last.Max.next() would carry into the next term there and split a run that is
		// really contiguous.
		if !last.Max.less(r.Min.prev()) {
			last.Max = MaxBlock(last.Max, r.Max)

			continue
		}

		merged = append(merged, r)
	}

	iv := Interval{Min: merged[0].Min, Max: merged[len(merged)-1].Max}
	for i := 1; i < len(merged); i++ {
		iv.Gaps = append(iv.Gaps, Gap{Min: merged[i-1].Max.next(), Max: merged[i].Min.prev()})
	}

	return iv
}

// Claim is the ancestry a split merge's outputs hold jointly. A merge that writes several parts
// can hand none of them the blocks its inputs covered — no fragment holds all of that data, so a
// single-part successor claim would answer a want with a fraction of the part — but the fragments
// together do hold it, and without recording that the lineage would be severed at the split and
// never restored.
//
// Blocks is what the group jointly covers; Group is the members' own blocks, allocated as one run
// by the commit that publishes them. The claim is *realized* only where every block of Group is
// present, at which point the blocks it names are covered as if one part held them — which is what
// [Index.Satisfying] tests and what a merge folds back into a single output's identity.
type Claim struct {
	Blocks Interval
	Group  Interval
}

// Valid reports whether the claim names a real group and a real set of ancestor blocks. An unset
// claim realizes nothing.
func (c Claim) Valid() bool { return c.Blocks.Valid() && c.Group.Valid() }

// Equal reports whether c and o name the same ancestry and the same group.
func (c Claim) Equal(o Claim) bool { return c.Blocks.Equal(o.Blocks) && c.Group.Equal(o.Group) }

// Supersedes reports whether e's data wholly subsumes o's: e sits at a higher merge level and
// covers every block o covers, or, for a split-group member, the whole ancestry its group claims;
// or e is o's identity written by a later tenure. It is decidable from identity alone — no index
// comparison, no bookkeeping — which is what lets a repair terminate: by the time a want is
// serviced the data may exist only inside a merged successor, and that successor has to count as
// satisfaction.
//
// A split merge's fragment covers only its own fresh blocks and supersedes nothing, however much of
// an input it happens to hold; the group's joint claim is resolved against a whole index by
// [Index.Satisfying], never here. A member of a group split again reaches the outer group's claim
// only through claims other entries carry, which [Lineage.Subsumes] resolves over a set of entries.
//
// A part written before format v5 carries no interval, so it neither supersedes nor is superseded
// by anything; wants naming it fall back to exact-prefix matching until a merge rewrites it with
// an interval.
func (e Entry) Supersedes(o Entry) bool {
	var own Lineage
	if o.Claim.Valid() {
		own = Lineage{o.Claim}
	}

	return own.Subsumes(e, o)
}

// NextBlock returns the block the next part committed to this index under term takes: one above
// the highest block ever allocated under this prefix, within term.
//
// Allocation is the shard owner's alone, out of its own index, and is claimed by the same CAS
// commit that adds the part — so it needs no coordination and works with the cluster layer
// absent. Two writers racing over one index resolve through that CAS: one commit lands, and the
// loser re-reads and re-allocates above the winner. Two tenures that never see each other's
// index — a displaced owner, replicas whose lineages diverged — cannot collide at all, because each
// allocates only under its own term.
//
// A term below the highest one this index has allocated under continues that term's sequence
// instead. Only a writer with no cluster — term 0, the only writer of its prefix — reaches that,
// and continuing is what keeps it from renumbering over blocks a term-0 index retired long ago. A
// clustered writer below the index's term is refused before it allocates (see [CheckTenure]).
//
// The high-water mark is carried in the index rather than recomputed from the live set, because
// the live set shrinks: retention that empties a shard, or a compaction that retires every part
// covering a range, would otherwise rewind the counter and hand a new part an identity an expired
// one held. Outstanding wants and unrealized group claims still count towards it, so an index
// written before the mark existed does not renumber over a part it is still owed.
func (ix *Index) NextBlock(term uint64) Block {
	top := ix.AllocatedBlocks

	bump := func(iv Interval) {
		if iv.Valid() {
			top = MaxBlock(top, iv.Max)
		}
	}

	for i := range ix.Entries {
		bump(ix.Entries[i].Blocks)
		bump(ix.Entries[i].Claim.Group)
	}

	for i := range ix.Wanted {
		bump(ix.Wanted[i].Blocks)
		bump(ix.Wanted[i].Claim.Group)
	}

	if top.Term < term {
		return Block{Term: term, N: 1}
	}

	return Block{Term: top.Term, N: top.N + 1}
}

// Covered is the set of blocks the index's data-bearing parts hold between them, including the
// ancestor blocks of every split group — any the index's lineage records — whose members are all
// present. It is what decides a want is met when no single part contains it.
func (ix *Index) Covered() Interval { return ix.Relations().Covered() }

// Satisfying returns a part in this index whose data answers w, if any: the part itself if the
// index still has it, otherwise the largest part containing every block w covers — or, when no
// single part does but a split group jointly covers it, the largest member of that group.
//
// A group answer is deliberately partial: it names one member, and the caller that copies it must
// come back for the rest (see [Index.Missing]). Reporting the group as satisfying at all is the
// point — the data exists, so no loss may be acknowledged.
//
// A hole never satisfies: it is an acknowledgement that the data is gone, so answering a want with
// one — or offering one to a peer repairing the same part — would spread the loss instead of
// repairing it. Use [Index.Discharging] for the weaker question of whether the obligation is over.
//
// "Largest" is widest interval first, then highest level, then prefix, so the answer does not
// depend on index order and a caller fetches the fewest objects for the most data.
func (ix *Index) Satisfying(w Want) (Entry, bool) { return ix.Relations().Satisfying(w) }

// SatisfyingWith is [Index.Satisfying] relating identities by extra lineage as well as this index's
// own: the catalog of the index that owes w, which is what relates a member of a group split again
// to a successor of the outer ancestry when the index answering w never saw either split.
func (ix *Index) SatisfyingWith(w Want, extra Lineage) (Entry, bool) {
	return ix.Relations(extra...).Satisfying(w)
}

// Discharging returns the entry that ends w as an obligation: a part satisfying it, or the hole
// committed in its place. It is what decides a want is no longer outstanding.
func (ix *Index) Discharging(w Want) (Entry, bool) { return ix.Relations().Discharging(w) }

// Missing returns the blocks this index would need to answer w and does not hold: the members of
// every split group derived from w's rows, followed through every group the lineage records, that no
// later group consumed. A member split again is not asked for; the members of its split are.
//
// It is what makes a group repairable one member at a time. A want naming a pre-split part is
// answered by no single peer entry, so repair asks for the group's blocks instead, and the peer
// resolves each of those against its own index — through containment, or its own lineage for a
// member it split again.
func (ix *Index) Missing(w Want) []Block { return ix.Relations().Missing(w) }

func betterSuccessor(a, b Entry) bool {
	switch {
	case a.Blocks.Len() != b.Blocks.Len():
		return a.Blocks.Len() > b.Blocks.Len()
	case a.Level != b.Level:
		return a.Level > b.Level
	default:
		return a.Prefix < b.Prefix
	}
}

// groupClaim is one split group as an index sees it: what it jointly covers, the members that must
// all be present for that to hold, and the lowest level any present member sits at.
type groupClaim struct {
	blocks Interval
	group  Interval
	level  uint32
}

// groupsOf collects the distinct split groups the data-bearing entries carry. A group's own block
// run is allocated once per shard, so its bounds identify it.
func groupsOf(entries []Entry) []groupClaim {
	var out []groupClaim

	for i := range entries {
		c := entries[i].Claim
		if !entries[i].Data() || !c.Valid() {
			continue
		}

		k := slices.IndexFunc(out, func(g groupClaim) bool {
			return g.group.Min == c.Group.Min && g.group.Max == c.Group.Max
		})
		if k < 0 {
			out = append(out, groupClaim{blocks: c.Blocks, group: c.Group, level: entries[i].Level})

			continue
		}

		out[k].level = min(out[k].level, entries[i].Level)
	}

	return out
}

// Complete reports whether every member of the split group e belongs to is present among entries,
// so the ancestry it claims is covered as if one part held it. It is false for an entry carrying no
// claim: there is no group to complete.
func (e Entry) Complete(entries []Entry) bool {
	if !e.Claim.Valid() {
		return false
	}

	return (&Index{Entries: entries}).Covered().Contains(e.Claim.Group)
}

// Subsumed is [Lineage.Subsumed] over the lineage live and added record themselves.
func Subsumed(live, added []Entry) map[string]struct{} {
	return LineageOf(slices.Concat(live, added)).Subsumed(live, added)
}

// Subsumed returns the prefixes among live whose rows are wholly inside the parts added: one added
// part holds them at a higher level, or a split group the addition completes claims them.
//
// Either direction left out keeps two representations of one set of rows live: a part holding a
// group's whole ancestry beside its members, or the ancestors beside the group a repair completes.
func (l Lineage) Subsumed(live, added []Entry) map[string]struct{} {
	return l.Relater().Subsumed(live, added)
}

// Subsumed is [Lineage.Subsumed] over r's lineage.
func (r *Relater) Subsumed(live, added []Entry) map[string]struct{} {
	out := make(map[string]struct{})

	for i := range added {
		for j := range live {
			if live[j].Prefix != added[i].Prefix && r.Subsumes(added[i], live[j]) {
				out[live[j].Prefix] = struct{}{}
			}
		}
	}

	all := slices.Concat(live, added)
	held := (&Index{Entries: all}).Covered()

	for _, g := range groupsOf(all) {
		if !held.Contains(g.group) {
			continue
		}

		claimed := r.holds(g.blocks)

		for j := range live {
			if claimed.Contains(live[j].Blocks) && live[j].Level < g.level {
				out[live[j].Prefix] = struct{}{}
			}
		}
	}

	return out
}

// Single is the interval holding b alone.
func Single(b Block) Interval { return Interval{Min: b, Max: b} }
