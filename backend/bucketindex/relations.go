package bucketindex

import "slices"

// Relations answers the identity questions of one index — which part satisfies or discharges a want,
// what the index covers, what a want still misses — relating identities by the index's lineage and
// extra. Its closures are computed once and shared by every question, so a batch of wants against
// the live set costs one closure per part rather than one per part per want.
type Relations struct {
	ix     *Index
	r      *Relater
	known  map[claimKey]struct{}
	covers [2]*Interval
}

// Relations returns the relations of ix, with extra lineage — the catalog of the index that owes the
// wants asked about.
func (ix *Index) Relations(extra ...Claim) *Relations {
	l := ix.Lineage().With(extra...)

	rel := &Relations{ix: ix, r: l.Relater(), known: make(map[claimKey]struct{}, len(l))}
	for _, c := range l {
		rel.known[c.key()] = struct{}{}
	}

	return rel
}

// Covered is [Index.Covered] over rel's lineage.
func (rel *Relations) Covered() Interval { return rel.covered(Entry.Data, 0) }

// Supersedes reports whether a data-bearing entry of the index subsumes o.
func (rel *Relations) Supersedes(o Entry) bool {
	r := rel.forClaim(o.Claim).r

	for i := range rel.ix.Entries {
		if e := &rel.ix.Entries[i]; e.Data() && r.Subsumes(*e, o) {
			return true
		}
	}

	return false
}

// Satisfying is [Index.Satisfying] over rel's lineage.
func (rel *Relations) Satisfying(w Want) (Entry, bool) {
	return rel.forClaim(w.Claim).satisfying(w, Entry.Data, 0)
}

// Discharging is [Index.Discharging] over rel's lineage.
func (rel *Relations) Discharging(w Want) (Entry, bool) {
	return rel.forClaim(w.Claim).satisfying(w, func(Entry) bool { return true }, 1)
}

// Missing is [Index.Missing] over rel's lineage.
func (rel *Relations) Missing(w Want) []Block {
	if !w.Blocks.Valid() {
		return nil
	}

	rel = rel.forClaim(w.Claim)
	if _, ok := rel.satisfying(w, Entry.Data, 0); ok {
		return nil
	}

	held := rel.Covered()
	_, via := rel.r.descendants(w.Blocks)

	var consumed Interval

	for i, c := range rel.r.l {
		if via[i] {
			consumed = consumed.Union(c.Blocks)
		}
	}

	var out []Block

	for i, c := range rel.r.l {
		if !via[i] {
			continue
		}

		c.Group.Each(func(b Block) bool {
			one := Single(b)
			if !held.Contains(one) && !consumed.Contains(one) && !slices.Contains(out, b) {
				out = append(out, b)
			}

			return true
		})
	}

	slices.SortFunc(out, Block.Compare)

	return out
}

// forClaim is rel, or relations that also know c when rel does not.
func (rel *Relations) forClaim(c Claim) *Relations {
	if !c.Valid() {
		return rel
	}

	if _, ok := rel.known[c.key()]; ok {
		return rel
	}

	return rel.ix.Relations(slices.Concat(rel.r.l, []Claim{c})...)
}

func (rel *Relations) covered(admit func(Entry) bool, slot int) Interval {
	if c := rel.covers[slot]; c != nil {
		return *c
	}

	runs := make([]Gap, 0, len(rel.ix.Entries))

	for i := range rel.ix.Entries {
		e := &rel.ix.Entries[i]
		if admit(*e) && e.Blocks.Valid() {
			runs = e.Blocks.appendRuns(runs)
		}
	}

	held := rel.r.realize(fromRuns(runs))
	rel.covers[slot] = &held

	return held
}

func (rel *Relations) satisfying(w Want, admit func(Entry) bool, slot int) (Entry, bool) {
	var (
		best  Entry
		found bool
	)

	ix := rel.ix
	owed := w.Entry()

	for i := range ix.Entries {
		e := ix.Entries[i]
		if !admit(e) {
			continue
		}

		if e.Prefix == w.Prefix {
			return e, true
		}

		if !e.Blocks.Contains(w.Blocks) && !rel.r.Subsumes(e, owed) {
			continue
		}

		if !found || betterSuccessor(e, best) {
			best, found = e, true
		}
	}

	if found {
		return best, true
	}

	return rel.jointlySatisfying(w, admit, slot)
}

// jointlySatisfying answers a want no single part contains: it holds only where the whole index
// covers w's blocks, and then names the best member of a split group that supplies some of them —
// one whose group's ancestry, followed through every group lineage records, reaches w's blocks.
func (rel *Relations) jointlySatisfying(w Want, admit func(Entry) bool, slot int) (Entry, bool) {
	if !w.Blocks.Valid() || !rel.covered(admit, slot).Contains(w.Blocks) {
		return Entry{}, false
	}

	var (
		best  Entry
		found bool
	)

	for i := range rel.ix.Entries {
		e := rel.ix.Entries[i]
		if !admit(e) || !e.Claim.Valid() {
			continue
		}

		if up, _ := rel.forClaim(e.Claim).r.ancestry(e.Blocks); !meets(up, w.Blocks) {
			continue
		}

		if !found || betterSuccessor(e, best) {
			best, found = e, true
		}
	}

	return best, found
}
