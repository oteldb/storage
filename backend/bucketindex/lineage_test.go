package bucketindex_test

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oteldb/storage/backend/bucketindex"
)

func TestLineageOverlaps(t *testing.T) {
	t.Parallel()

	outer := members3("f", 10, bucketindex.Blocks(1, 2), 1)
	// f2 merged with block 7 and split again.
	inner := members3("g", 20, bucketindex.Blocks(7, 12), 2)
	// Another lineage split block 1 with block 3.
	rival := members3("h", 30, bucketindex.Blocks(1, 3), 1)
	flush := func(b uint64) bucketindex.Entry {
		return bucketindex.Entry{Prefix: "b", Blocks: bucketindex.Blocks(b)}
	}

	lineage := bucketindex.LineageOf(slices.Concat(outer, inner, rival))

	for _, tc := range []struct {
		name string
		a, b bucketindex.Entry
		want bool
	}{
		{"Siblings", outer[0], outer[1], false},
		{"Cousins", outer[0], inner[0], false},
		{"AncestorOfMember", flush(1), outer[0], true},
		{"AncestorOfNestedMember", flush(1), inner[0], true},
		{"MemberOfInner", outer[2], inner[1], true},
		{"OtherAncestryOfNestedMember", flush(7), inner[0], true},
		{"Unrelated", flush(5), outer[0], false},
		{"AncestorsApart", flush(1), flush(2), false},
		{"RivalLineages", outer[0], rival[0], true},
		{"RivalAndUnrelatedAncestor", rival[0], flush(2), false},
		{"Unset", bucketindex.Entry{}, outer[0], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, lineage.Overlaps(tc.a, tc.b))
			assert.Equal(t, tc.want, lineage.Overlaps(tc.b, tc.a), "overlap is symmetric")
		})
	}
}

// modelPart is an entry and the rows it really holds.
type modelPart struct {
	ent  bucketindex.Entry
	rows map[int]struct{}
}

// lineageModel grows parts the way the engines do — flushes, merges that fold complete groups, and
// split merges — over two replicas that compact independently, allocating from one counter so
// identities stay unique.
type lineageModel struct {
	rnd  *rand.Rand
	next uint64
	all  []modelPart
	live [2][]modelPart
}

func newLineageModel(seed uint64) *lineageModel {
	m := &lineageModel{rnd: rand.New(rand.NewPCG(seed, seed^0x9e37))}

	for range 3 + m.rnd.IntN(3) {
		m.next++
		p := modelPart{
			ent:  bucketindex.Entry{Prefix: "p", Blocks: bucketindex.Blocks(m.next)},
			rows: map[int]struct{}{int(m.next) * 10: {}, int(m.next)*10 + 1: {}, int(m.next)*10 + 2: {}},
		}
		m.all = append(m.all, p)
		m.live[0] = append(m.live[0], p)
		m.live[1] = append(m.live[1], p)
	}

	return m
}

func (m *lineageModel) step() {
	r := m.rnd.IntN(2)
	live := m.live[r]

	if len(live) == 0 {
		return
	}

	m.rnd.Shuffle(len(live), func(i, j int) { live[i], live[j] = live[j], live[i] })

	n := 1 + m.rnd.IntN(min(3, len(live)))
	src, rest := live[:n], slices.Clone(live[n:])

	var (
		union  bucketindex.Interval
		level  uint32
		claims []bucketindex.Claim
		rows   []int
	)

	for i := range src {
		p := &src[i]
		union = union.Union(p.ent.Blocks)
		level = max(level, p.ent.Level+1)

		if p.ent.Claim.Valid() {
			claims = append(claims, p.ent.Claim)
		}

		for row := range p.rows {
			rows = append(rows, row)
		}
	}

	for folded := true; folded; {
		folded = false

		for i, c := range claims {
			if union.Contains(c.Group) {
				union, folded = union.Union(c.Blocks), true
				claims = slices.Delete(claims, i, i+1)

				break
			}
		}
	}

	var out []modelPart

	if k := 2 + m.rnd.IntN(2); len(rows) >= k && m.rnd.IntN(2) == 0 {
		slices.Sort(rows)
		m.rnd.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })

		c := bucketindex.Claim{Blocks: union, Group: bucketindex.Interval{Min: m.next + 1, Max: m.next + uint64(k)}}
		for i := range k {
			m.next++
			p := modelPart{
				ent:  bucketindex.Entry{Prefix: "p", Blocks: bucketindex.Blocks(m.next), Claim: c, Level: level},
				rows: map[int]struct{}{},
			}

			for j := i; j < len(rows); j += k {
				p.rows[rows[j]] = struct{}{}
			}

			out = append(out, p)
		}
	} else {
		p := modelPart{ent: bucketindex.Entry{Prefix: "p", Blocks: union, Level: level}, rows: map[int]struct{}{}}
		for _, c := range claims {
			if c.Blocks.Len() > p.ent.Claim.Blocks.Len() {
				p.ent.Claim = c
			}
		}

		for _, row := range rows {
			p.rows[row] = struct{}{}
		}

		out = append(out, p)
	}

	m.all = append(m.all, out...)
	m.live[r] = append(rest, out...)
}

func shareRow(a, b map[int]struct{}) bool {
	for row := range a {
		if _, ok := b[row]; ok {
			return true
		}
	}

	return false
}

func holdsAll(a, b map[int]struct{}) bool {
	for row := range b {
		if _, ok := a[row]; !ok {
			return false
		}
	}

	return true
}

// TestLineageIsSound checks both relations against the rows each part really holds, over random
// histories: Overlaps never calls parts sharing a row disjoint, and Subsumes never claims rows a part
// lacks.
func TestLineageIsSound(t *testing.T) {
	t.Parallel()

	for seed := range uint64(300) {
		m := newLineageModel(seed)
		for range 12 {
			m.step()
		}

		entries := make([]bucketindex.Entry, len(m.all))
		for i := range m.all {
			entries[i] = m.all[i].ent
		}

		lineage := bucketindex.LineageOf(entries)

		for _, a := range m.all {
			for _, b := range m.all {
				if shareRow(a.rows, b.rows) {
					assert.True(t, lineage.Overlaps(a.ent, b.ent), "seed %d: %+v and %+v share rows", seed, a.ent, b.ent)
				}

				if lineage.Subsumes(a.ent, b.ent) {
					assert.True(t, holdsAll(a.rows, b.rows), "seed %d: %+v lacks rows of %+v", seed, a.ent, b.ent)
				}
			}
		}
	}
}
