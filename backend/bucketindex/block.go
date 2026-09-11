package bucketindex

import "slices"

// Interval is the exact set of block numbers a part covers, the identity it carries alongside its
// prefix. A flush writes {n}; a merge writes the union of what its inputs covered.
//
// The set is stored as its bounds plus the runs of numbers it does *not* hold, because a part's
// blocks are contiguous in every ordinary case and the gap list is then empty. It is a set and not
// a hull because supersession means "holds these rows": merging the parts on either side of a lost
// one yields {1,3}, and a hull would claim block 2 — a want discharged by a part holding none of
// its data.
//
// Block numbers start at 1, so the zero value is *unset* rather than a range covering block 0.
// That distinction is what keeps a part written before format v5 — which carries no interval —
// from accidentally containing, or being contained by, anything: see [Interval.Valid].
type Interval struct {
	Min uint64
	Max uint64
	// Gaps are the runs inside (Min, Max) the set does not cover, in ascending order, disjoint and
	// non-adjacent. The canonical form is the only valid one: two encodings of one set would break
	// both equality and the encode∘decode identity.
	Gaps []Gap
}

// Gap is a run of block numbers an [Interval] skips: the numbers from Min to Max inclusive that
// lie inside the interval's bounds but that no part in it holds.
type Gap struct {
	Min uint64
	Max uint64
}

// Blocks builds the interval covering exactly the given block numbers, in any order and with
// duplicates. It returns the unset interval for an empty list or one naming block 0.
func Blocks(nums ...uint64) Interval {
	runs := make([]Gap, 0, len(nums))
	for _, n := range nums {
		if n == 0 {
			return Interval{}
		}

		runs = append(runs, Gap{Min: n, Max: n})
	}

	return fromRuns(runs)
}

// Valid reports whether the interval names a real set of blocks: numbering starts at 1, the bounds
// are ordered, and the gap list is canonical. Everything else — the zero value of a pre-v5 entry,
// and any inversion or denormalization a corrupt or hostile encoding could produce — is unset, and
// takes part in no containment.
func (iv Interval) Valid() bool {
	if iv.Min < 1 || iv.Min > iv.Max {
		return false
	}

	prev := iv.Min
	for _, g := range iv.Gaps {
		// Strictly inside the bounds, and separated from the previous gap by at least one held
		// block: otherwise the set has different bounds, or a shorter gap list describes it.
		if g.Min > g.Max || g.Min <= prev || g.Max >= iv.Max {
			return false
		}

		prev = g.Max + 1
	}

	return true
}

// Contains reports whether iv covers every block o covers. Both intervals must be valid: an unset
// interval neither contains nor is contained, which is the fallback to exact-prefix matching for
// parts written before v5.
func (iv Interval) Contains(o Interval) bool {
	if !iv.Valid() || !o.Valid() {
		return false
	}

	held := iv.runs()
	i := 0

	for _, r := range o.runs() {
		for i < len(held) && held[i].Max < r.Min {
			i++
		}

		if i >= len(held) || held[i].Min > r.Min || held[i].Max < r.Max {
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
		return fromRuns(append(iv.runs(), o.runs()...))
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

	n := iv.Max - iv.Min + 1
	for _, g := range iv.Gaps {
		n -= g.Max - g.Min + 1
	}

	return n
}

// Each calls fn for every block the interval covers, in ascending order, stopping early if fn
// returns false. It is how a caller enumerates the members of a split group it is missing.
func (iv Interval) Each(fn func(uint64) bool) {
	if !iv.Valid() {
		return
	}

	for _, r := range iv.runs() {
		for b := r.Min; b <= r.Max; b++ {
			if !fn(b) {
				return
			}
		}
	}
}

// runs returns the ascending, disjoint runs of blocks the interval holds. It assumes validity.
func (iv Interval) runs() []Gap {
	out := make([]Gap, 0, len(iv.Gaps)+1)

	lo := iv.Min
	for _, g := range iv.Gaps {
		out = append(out, Gap{Min: lo, Max: g.Min - 1})
		lo = g.Max + 1
	}

	return append(out, Gap{Min: lo, Max: iv.Max})
}

// fromRuns is the inverse of [Interval.runs] over an arbitrary run list: it sorts, coalesces and
// derives the canonical bounds-and-gaps form. Runs naming block 0 make the whole set unset, as
// they do everywhere else.
func fromRuns(runs []Gap) Interval {
	if len(runs) == 0 {
		return Interval{}
	}

	slices.SortFunc(runs, func(a, b Gap) int {
		switch {
		case a.Min != b.Min:
			return int(int64(a.Min) - int64(b.Min))
		case a.Max < b.Max:
			return -1
		case a.Max > b.Max:
			return 1
		default:
			return 0
		}
	})

	if runs[0].Min == 0 {
		return Interval{}
	}

	merged := runs[:1]

	for _, r := range runs[1:] {
		last := &merged[len(merged)-1]
		// r.Min-1, not last.Max+1: the top of the number space is a legal block, and last.Max+1
		// would wrap to 0 there and split a run that is really contiguous.
		if r.Min-1 <= last.Max {
			last.Max = max(last.Max, r.Max)

			continue
		}

		merged = append(merged, r)
	}

	iv := Interval{Min: merged[0].Min, Max: merged[len(merged)-1].Max}
	for i := 1; i < len(merged); i++ {
		iv.Gaps = append(iv.Gaps, Gap{Min: merged[i-1].Max + 1, Max: merged[i].Min - 1})
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

// Supersedes reports whether e's data wholly subsumes o's: e covers every block o covers, and sits
// at a higher merge level. It is decidable from identity alone — no index comparison, no
// bookkeeping — which is what lets a repair terminate: by the time a want is serviced the data may
// exist only inside a merged successor, and that successor has to count as satisfaction.
//
// It is a single-part relation on purpose. A split merge's fragment covers only its own fresh
// blocks and supersedes nothing, however much of an input it happens to hold; the group's joint
// claim is resolved against a whole index by [Index.Satisfying], never here.
//
// A part written before format v5 carries no interval, so it neither supersedes nor is superseded
// by anything; wants naming it fall back to exact-prefix matching until a merge rewrites it with
// an interval.
func (e Entry) Supersedes(o Entry) bool {
	return e.Blocks.Contains(o.Blocks) && e.Level > o.Level
}

// NextBlock returns the block number the next part committed to this index takes: one above the
// highest block ever allocated under this prefix.
//
// Allocation is the shard owner's alone, out of its own index, and is claimed by the same CAS
// commit that adds the part — so it needs no coordination and works with the cluster layer
// absent. Two writers racing a handoff resolve through that CAS: one commit lands, and the loser
// re-reads and re-allocates above the winner.
//
// The high-water mark is carried in the index rather than recomputed from the live set, because
// the live set shrinks: retention that empties a shard, or a compaction that retires every part
// covering a range, would otherwise rewind the counter and hand a new part an identity an expired
// one held. Outstanding wants and unrealized group claims still count towards it, so an index
// written before the mark existed does not renumber over a part it is still owed.
func (ix *Index) NextBlock() uint64 {
	maxBlock := ix.AllocatedBlocks

	bump := func(iv Interval) {
		if iv.Valid() && iv.Max > maxBlock {
			maxBlock = iv.Max
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

	return maxBlock + 1
}

// Covered is the set of blocks the index's data-bearing parts hold between them, including the
// ancestor blocks of every split group whose members are all present. It is what decides a want is
// met when no single part contains it.
func (ix *Index) Covered() Interval { return ix.covered(Entry.Data) }

func (ix *Index) covered(admit func(Entry) bool) Interval {
	var (
		held   Interval
		claims []Claim
	)

	for i := range ix.Entries {
		e := ix.Entries[i]
		if !admit(e) {
			continue
		}

		held = held.Union(e.Blocks)

		if e.Claim.Valid() {
			claims = append(claims, e.Claim)
		}
	}

	// A group whose members were themselves split resolves only once the inner group has, so the
	// pass repeats while it keeps realizing claims. Each round retires at least one claim, so it
	// terminates in at most len(claims) rounds.
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

	return held
}

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
func (ix *Index) Satisfying(w Want) (Entry, bool) { return ix.satisfying(w, Entry.Data) }

// Discharging returns the entry that ends w as an obligation: a part satisfying it, or the hole
// committed in its place. It is what decides a want is no longer outstanding.
func (ix *Index) Discharging(w Want) (Entry, bool) {
	return ix.satisfying(w, func(Entry) bool { return true })
}

func (ix *Index) satisfying(w Want, admit func(Entry) bool) (Entry, bool) {
	var (
		best  Entry
		found bool
	)

	for i := range ix.Entries {
		e := ix.Entries[i]
		if !admit(e) {
			continue
		}

		if e.Prefix == w.Prefix {
			return e, true
		}

		if !e.Blocks.Contains(w.Blocks) {
			continue
		}

		if !found || betterSuccessor(e, best) {
			best, found = e, true
		}
	}

	if found {
		return best, true
	}

	return ix.jointlySatisfying(w, admit)
}

// jointlySatisfying answers a want no single part contains: it holds only where the whole index
// covers w's blocks, and then names the best member of the split group that supplies them.
func (ix *Index) jointlySatisfying(w Want, admit func(Entry) bool) (Entry, bool) {
	if !w.Blocks.Valid() || !ix.covered(admit).Contains(w.Blocks) {
		return Entry{}, false
	}

	var (
		best  Entry
		found bool
	)

	for i := range ix.Entries {
		e := ix.Entries[i]
		if !admit(e) || !e.Claim.Blocks.Contains(w.Blocks) {
			continue
		}

		if !found || betterSuccessor(e, best) {
			best, found = e, true
		}
	}

	return best, found
}

// Missing returns the blocks of the split groups this index would need to answer w and does not
// hold: the members of every group whose claim covers w but whose own blocks are not all present.
//
// It is what makes a group repairable one member at a time. A want naming a pre-split part is
// answered by no single peer entry, so repair asks for the group's blocks instead, and the peer
// resolves each of those against its own index by ordinary containment.
func (ix *Index) Missing(w Want) []uint64 {
	if !w.Blocks.Valid() {
		return nil
	}

	held := ix.covered(Entry.Data)
	if held.Contains(w.Blocks) {
		return nil
	}

	var out []uint64

	for i := range ix.Entries {
		c := ix.Entries[i].Claim
		if !ix.Entries[i].Data() || !c.Valid() || !c.Blocks.Contains(w.Blocks) {
			continue
		}

		c.Group.Each(func(b uint64) bool {
			if !held.Contains(Blocks(b)) && !slices.Contains(out, b) {
				out = append(out, b)
			}

			return true
		})
	}

	slices.Sort(out)

	return out
}

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

// Subsumed returns the prefixes among live whose rows are wholly inside the parts added: one that
// supersedes them outright, or a split group the addition completes, whose joint claim covers them.
//
// The group case is what [Entry.Supersedes] alone cannot give. A repair that brings back the last
// member of a split makes the whole group's ancestry present, and the parts that ancestry covers
// would otherwise stay live beside it and have their rows read twice.
func Subsumed(live, added []Entry) map[string]struct{} {
	out := make(map[string]struct{})

	for i := range added {
		for j := range live {
			if live[j].Prefix != added[i].Prefix && added[i].Supersedes(live[j]) {
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

		for j := range live {
			if g.blocks.Contains(live[j].Blocks) && live[j].Level < g.level {
				out[live[j].Prefix] = struct{}{}
			}
		}
	}

	return out
}
