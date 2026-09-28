package bucketindex

// Plan is the identity a newly written part awaits until the commit that publishes it assigns one. A
// flush leaves Blocks unset, so the commit allocates a fresh block for it; a merge that writes one
// part inherits the set of blocks its inputs covered, which is what makes the output supersede them.
type Plan struct {
	Blocks Interval
	Claim  Claim
	// Group is set on the outputs of a merge that split, and shared by all of them: the commit
	// allocates the whole run at once so the fragments can name each other.
	Group *SplitGroup
	Level uint32
}

// SplitGroup is the joint claim the outputs of one split merge carry. The ancestor blocks are known
// when the merge is planned; the group's own blocks are not, because allocation happens per commit
// attempt.
type SplitGroup struct {
	Blocks Interval
	N      int
}

// FlushPlan is the plan of a flush output: a fresh block at level 0.
func FlushPlan() *Plan { return &Plan{} }

// PlanMerge returns the plans of the n outputs of a merge over src, whose identities are read from
// their Blocks, Claim and Level.
//
// A merge that writes one part does not allocate. The output covers exactly the blocks its inputs
// covered — a set, so a run that straddles a gap claims no block in it — one level above the
// deepest input, and that is precisely what makes supersession decidable from identity alone, so a
// repair can accept the successor of the part it wanted.
//
// A merge that splits its output cannot hand any fragment that set: none holds all of the data, so
// a successor claim would answer a want with a fraction of the part. The fragments take a fresh run
// of blocks instead and carry the inputs' set as a joint [Claim] over that run — which is what keeps
// the lineage: the claim is realized wherever every member of the run is present, and a later merge
// that consumes them all folds it back into an ordinary interval.
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
func PlanMerge(src []Entry, n int) []*Plan {
	var (
		union  Interval
		level  uint32
		claims []Claim
	)

	for i := range src {
		level = max(level, src[i].Level+1)
		union = union.Union(src[i].Blocks)

		if src[i].Claim.Valid() {
			claims = append(claims, src[i].Claim)
		}
	}

	union, claims = realizeClaims(union, claims)

	out := make([]*Plan, n)

	if n == 1 {
		out[0] = &Plan{Blocks: union, Claim: widestClaim(claims), Level: level}

		return out
	}

	group := &SplitGroup{Blocks: union, N: n}
	for i := range out {
		out[i] = &Plan{Group: group, Level: level}
	}

	return out
}

// realizeClaims folds into held every claim whose group is wholly inside it, repeating while that
// keeps realizing more — a group whose members were themselves split resolves only after the inner
// one has. It returns what is covered and the claims still owed a member.
func realizeClaims(held Interval, claims []Claim) (Interval, []Claim) {
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

func widestClaim(claims []Claim) Claim {
	var best Claim
	for _, c := range claims {
		if c.Blocks.Len() > best.Blocks.Len() {
			best = c
		}
	}

	return best
}

// Allocator assigns the identities of one commit attempt, taking blocks upward from the one
// [Index.NextBlock] returned. A fresh one is made per attempt: a rival's entries are only known after
// a rebase, and reusing an attempt's allocation would hand a part a block the winner already claimed.
type Allocator struct {
	next   Block
	groups map[*SplitGroup]*groupRun
}

// groupRun is the run of blocks one commit attempt allocated to a split group, and how far into it
// the fragments assigned so far have got.
type groupRun struct {
	run  Interval
	next Block
}

// NewAllocator starts allocating at next.
func NewAllocator(next Block) *Allocator {
	return &Allocator{next: next, groups: make(map[*SplitGroup]*groupRun)}
}

// Term is the term every block this allocator hands out belongs to: the tenure the commit writes as.
func (a *Allocator) Term() uint64 { return a.next.Term }

// Mark is the allocation high-water mark after everything assigned so far: the last block handed
// out, or the one below the first if none was.
func (a *Allocator) Mark() Block { return Block{Term: a.next.Term, N: a.next.N - 1} }

// Assign turns p into a real identity. A plan that already names its blocks keeps them.
//
// A split group takes its whole run at once, before any of its fragments is numbered, so every
// fragment carries the same claim and can name the siblings a repair still has to fetch.
func (a *Allocator) Assign(p *Plan) (Interval, Claim) {
	if p.Blocks.Valid() {
		return p.Blocks, p.Claim
	}

	if p.Group == nil {
		b := a.take(1)

		return Interval{Min: b, Max: b}, p.Claim
	}

	g, ok := a.groups[p.Group]
	if !ok {
		lo := a.take(uint64(p.Group.N))
		g = &groupRun{run: Interval{Min: lo, Max: Block{Term: lo.Term, N: lo.N + uint64(p.Group.N) - 1}}, next: lo}
		a.groups[p.Group] = g
	}

	b := g.next
	g.next.N++

	claim := Claim{Blocks: p.Group.Blocks, Group: g.run}
	if !claim.Valid() {
		return Interval{Min: b, Max: b}, Claim{}
	}

	return Interval{Min: b, Max: b}, claim
}

func (a *Allocator) take(n uint64) Block {
	b := a.next
	a.next.N += n

	return b
}
