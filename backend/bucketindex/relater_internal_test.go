package bucketindex

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// The fixed points below rescan every claim until nothing changes: the definitions the [Relater]'s
// work queues must agree with.

func naiveHolds(l Lineage, iv Interval) Interval {
	used := make([]bool, len(l))

	for changed := true; changed; {
		changed = false

		for i, c := range l {
			if !used[i] && iv.Contains(c.Blocks) {
				used[i], changed = true, true
				iv = iv.Union(c.Group)
			}
		}
	}

	return iv
}

func naiveClose(l Lineage, iv Interval, meet func(c Claim) Interval, add func(c Claim) Interval) (Interval, []bool) {
	via := make([]bool, len(l))

	for changed := true; changed; {
		changed = false

		for i, c := range l {
			if !via[i] && meets(iv, meet(c)) {
				via[i], changed = true, true
				iv = iv.Union(add(c))
			}
		}
	}

	return iv, via
}

func naiveRealize(l Lineage, held Interval) Interval {
	claims := append(Lineage(nil), l...)

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

// randomLineage draws nested groups over a small block space in a random order, so closures need
// several levels and the scan order is sometimes against them.
func randomLineage(rnd *rand.Rand) Lineage {
	var l Lineage

	for range rnd.IntN(12) + 1 {
		var blocks, group Interval

		for range rnd.IntN(3) + 1 {
			blocks = blocks.Union(TermBlocks(rnd.Uint64N(2), rnd.Uint64N(30)+1))
		}

		for range rnd.IntN(3) + 1 {
			group = group.Union(TermBlocks(rnd.Uint64N(2), rnd.Uint64N(30)+1))
		}

		if c := (Claim{Blocks: blocks, Group: group}); c.Valid() {
			l = append(l, c)
		}
	}

	rnd.Shuffle(len(l), func(i, j int) { l[i], l[j] = l[j], l[i] })

	return l
}

func randomSet(rnd *rand.Rand) Interval {
	var iv Interval

	for range rnd.IntN(4) + 1 {
		iv = iv.Union(TermBlocks(rnd.Uint64N(2), rnd.Uint64N(30)+1))
	}

	return iv
}

func TestRelaterMatchesTheFixedPoints(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewPCG(71, 73))

	for range 3000 {
		l := randomLineage(rnd)
		r := l.Relater()
		iv := randomSet(rnd)

		require.Equal(t, naiveHolds(l, iv), r.holds(iv), "holds %v over %v", iv, l)
		require.Equal(t, naiveHolds(l, iv), r.holds(iv), "a remembered closure is the same closure")

		up, via := naiveClose(l, iv, func(c Claim) Interval { return c.Group }, func(c Claim) Interval { return c.Blocks })
		gotUp, gotVia := r.ancestry(iv)
		require.Equal(t, up, gotUp, "ancestry %v over %v", iv, l)
		require.Equal(t, via, gotVia)

		other := randomSet(rnd)
		_, otherVia := naiveClose(l, other, func(c Claim) Interval { return c.Group }, func(c Claim) Interval { return c.Blocks })
		reached := make([]bool, len(l))
		r.reachAncestry(reached, iv)
		require.Equal(t, via, reached, "the groups %v's ancestry passes through over %v", iv, l)
		r.reachAncestry(reached, other)

		for i := range via {
			require.Equal(t, via[i] || otherVia[i], reached[i], "reached accumulates across calls")
		}

		down, dvia := naiveClose(l, iv, func(c Claim) Interval { return c.Blocks }, func(c Claim) Interval { return c.Group })
		gotDown, gotDvia := r.descendants(iv)
		require.Equal(t, down, gotDown, "descendants %v over %v", iv, l)
		require.Equal(t, dvia, gotDvia)

		held := naiveRealize(l, iv)
		complete := make([]bool, len(l))

		for i, c := range l {
			complete[i] = held.Contains(c.Group)
		}

		gotHeld, gotComplete := r.realize(iv)
		require.Equal(t, held, gotHeld, "realize %v over %v", iv, l)
		require.Equal(t, complete, gotComplete, "the groups %v completes over %v", iv, l)
	}
}
