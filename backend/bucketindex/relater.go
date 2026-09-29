package bucketindex

import (
	"cmp"
	"slices"
)

// Relater answers identity questions over one lineage, computing each closure by a work queue over
// an index of the claims' block runs and remembering it, so a batch — every outstanding want of a
// commit against every live entry, every candidate a repair judges — pays for each closure once.
//
// A closure is followed only along the claims that can extend it: the ones whose runs meet a run the
// closure just gained. The naive fixed point rescans every claim until nothing changes, which a deeply
// nested lineage ordered against the scan turns into one pass per level, per candidate, per want.
type Relater struct {
	l      Lineage
	blocks runIndex
	groups runIndex

	holdsOf  map[string][]closure
	upOf     map[string][]closure
	realized map[string][]closure
	key      []byte
}

type closure struct {
	iv  Interval
	out Interval
	via []bool
}

// Relater returns a relater over l.
func (l Lineage) Relater() *Relater {
	r := &Relater{
		l:        l,
		holdsOf:  make(map[string][]closure),
		upOf:     make(map[string][]closure),
		realized: make(map[string][]closure),
	}

	for i, c := range l {
		r.blocks.add(c.Blocks, i)
		r.groups.add(c.Group, i)
	}

	r.blocks.build()
	r.groups.build()

	return r
}

// Lineage is the lineage r relates by.
func (r *Relater) Lineage() Lineage { return r.l }

// Subsumes is [Lineage.Subsumes] over r's lineage.
func (r *Relater) Subsumes(e, o Entry) bool {
	if e.Term > o.Term && e.sameIdentity(o) {
		return true
	}

	return e.Level > o.Level && r.holds(e.Blocks).Contains(o.Blocks)
}

// Overlaps is [Lineage.Overlaps] over r's lineage.
func (r *Relater) Overlaps(a, b Entry) bool {
	upA, viaA := r.ancestry(a.Blocks)
	upB, viaB := r.ancestry(b.Blocks)

	if meets(a.Blocks, upB) || meets(b.Blocks, upA) {
		return true
	}

	common := intersect(upA, upB)
	if !common.Valid() {
		return false
	}

	for i, c := range r.l {
		if viaA[i] != viaB[i] && meets(c.Blocks, common) {
			return true
		}
	}

	return false
}

// Ancestors returns the claims iv's rows derive through: each group iv's blocks belong to, and
// again each group the members of that one's ancestry belong to.
func (r *Relater) Ancestors(iv Interval) []Claim {
	_, via := r.ancestry(iv)

	var out []Claim

	for i, ok := range via {
		if ok {
			out = append(out, r.l[i])
		}
	}

	return out
}

// holds is every block whose rows iv includes: iv, and the members of each group whose claimed
// ancestry it covers, repeated so a group split again resolves through the outer one.
func (r *Relater) holds(iv Interval) Interval {
	if len(r.l) == 0 {
		return iv
	}

	if c, ok := r.lookup(r.holdsOf, iv); ok {
		return c.out
	}

	out, _ := r.close(iv, &r.blocks, func(c Claim, held Interval) (bool, Interval) {
		return held.Contains(c.Blocks), c.Group
	})

	r.remember(r.holdsOf, iv, out, nil)

	return out
}

// ancestry is iv with every block its rows derive from, and which groups that descent passes through.
func (r *Relater) ancestry(iv Interval) (Interval, []bool) {
	if len(r.l) == 0 {
		return iv, []bool{}
	}

	if c, ok := r.lookup(r.upOf, iv); ok {
		return c.out, c.via
	}

	out, via := r.close(iv, &r.groups, func(c Claim, _ Interval) (bool, Interval) { return true, c.Blocks })

	r.remember(r.upOf, iv, out, via)

	return out, via
}

// descendants is iv with every block derived from its rows, and which groups that derivation passes
// through: each group whose claimed ancestry iv meets, and again from that group's members.
func (r *Relater) descendants(iv Interval) (Interval, []bool) {
	return r.close(iv, &r.blocks, func(c Claim, _ Interval) (bool, Interval) { return true, c.Group })
}

// realize is held with the ancestry of every group whose members it holds, repeated so a group whose
// members were split again resolves once the inner groups have, and which groups that completes.
func (r *Relater) realize(held Interval) (Interval, []bool) {
	if len(r.l) == 0 {
		return held, []bool{}
	}

	if c, ok := r.lookup(r.realized, held); ok {
		return c.out, c.via
	}

	out, via := r.close(held, &r.groups, func(c Claim, held Interval) (bool, Interval) {
		return held.Contains(c.Group), c.Blocks
	})

	r.remember(r.realized, held, out, via)

	return out, via
}

// level is the lowest level among the data-bearing entries derived from group: the level its
// completion stands at, whichever members or descendants of them complete it.
func (r *Relater) level(group Interval, entries []Entry) (uint32, bool) {
	d, _ := r.descendants(group)

	var (
		level uint32
		found bool
	)

	for i := range entries {
		e := &entries[i]
		if e.Data() && e.Blocks.Valid() && d.Contains(e.Blocks) && (!found || e.Level < level) {
			level, found = e.Level, true
		}
	}

	return level, found
}

// close grows iv to a fixed point: each claim whose indexed runs meet a run iv gains is offered to
// take, which says whether the claim applies to the closure so far and what it adds. A claim that
// does not apply yet is offered again whenever iv gains another run it meets, so none is missed, and
// one that applies is used once.
func (r *Relater) close(
	iv Interval, index *runIndex, take func(c Claim, iv Interval) (bool, Interval),
) (Interval, []bool) {
	via := make([]bool, len(r.l))
	if !iv.Valid() {
		return iv, via
	}

	queue := iv.appendRuns(nil)

	for len(queue) > 0 {
		run := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		index.meeting(run, func(i int) {
			if via[i] {
				return
			}

			ok, add := take(r.l[i], iv)
			if !ok {
				return
			}

			via[i] = true

			if grown := iv.Union(add); !grown.Equal(iv) {
				iv = grown
				queue = add.appendRuns(queue)
			}
		})
	}

	return iv, via
}

// runIndex finds the claims one of whose runs meets a given run: the runs sorted by their low bound,
// with the running maximum of their high bounds, so a search skips every run ending before the query.
type runIndex struct {
	runs  []indexedRun
	reach []Block
}

type indexedRun struct {
	Gap

	claim int
}

func (x *runIndex) add(iv Interval, claim int) {
	if !iv.Valid() {
		return
	}

	for i := range iv.nruns() {
		x.runs = append(x.runs, indexedRun{Gap: iv.run(i), claim: claim})
	}
}

func (x *runIndex) build() {
	slices.SortFunc(x.runs, func(a, b indexedRun) int {
		return cmp.Or(a.Min.Compare(b.Min), a.Max.Compare(b.Max), cmp.Compare(a.claim, b.claim))
	})

	x.reach = make([]Block, len(x.runs))
	for i := range x.runs {
		x.reach[i] = x.runs[i].Max
		if i > 0 {
			x.reach[i] = MaxBlock(x.reach[i], x.reach[i-1])
		}
	}
}

// meeting calls fn with the claim of every indexed run that meets q.
func (x *runIndex) meeting(q Gap, fn func(claim int)) {
	// reach is non-decreasing, so the first run that can reach q is found by search; runs starting
	// after q ends cannot meet it.
	i, _ := slices.BinarySearchFunc(x.reach, q.Min, Block.Compare)

	for ; i < len(x.runs) && !q.Max.less(x.runs[i].Min); i++ {
		if !x.runs[i].Max.less(q.Min) {
			fn(x.runs[i].claim)
		}
	}
}

// lookup finds iv's remembered closure by its encoding, which for a valid interval is unique; the
// lookup itself allocates nothing.
func (r *Relater) lookup(cache map[string][]closure, iv Interval) (closure, bool) {
	r.key = appendInterval(r.key[:0], iv)

	cs := cache[string(r.key)]
	for i := range cs {
		if cs[i].iv.Equal(iv) {
			return cs[i], true
		}
	}

	return closure{}, false
}

func (r *Relater) remember(cache map[string][]closure, iv, out Interval, via []bool) {
	k := string(appendInterval(r.key[:0], iv))
	cache[k] = append(cache[k], closure{iv: iv, out: out, via: via})
}
