package repair

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/backend/bucketindex"
)

// peer answers wants from its index, as the cluster layer does, with per-block overrides.
type peer struct {
	ix bucketindex.Index
	// member overrides the answer to a member want for a block.
	member map[bucketindex.Block]bucketindex.FetchResult
	calls  [][]bucketindex.Want
	// disk, when set, receives every part the peer answers with, as a copy would.
	disk map[string]bucketindex.Entry
}

func (p *peer) FetchWants(_ context.Context, wants []bucketindex.Want) []bucketindex.FetchResult {
	p.calls = append(p.calls, wants)

	out := make([]bucketindex.FetchResult, len(wants))

	for i := range wants {
		w := &wants[i]
		if r, ok := p.member[w.Blocks.Min]; ok && w.Prefix == "" {
			out[i] = r

			continue
		}

		ent, ok := p.ix.Satisfying(*w)
		if !ok {
			out[i].Outcome = bucketindex.WantAbsent

			continue
		}

		out[i].Entry = ent

		if p.disk != nil {
			p.disk[ent.Prefix] = ent
		}
	}

	return out
}

func (p *peer) asked() int {
	n := 0
	for _, c := range p.calls {
		n += len(c)
	}

	return n
}

func want(prefix string, b uint64) bucketindex.Want {
	return bucketindex.Want{Prefix: prefix, Blocks: bucketindex.Blocks(b), MinTime: 1, MaxTime: 2}
}

// group returns the n members of a split group over run [first, first+n) jointly claiming claimed.
func group(name string, first, n uint64, claimed bucketindex.Interval) []bucketindex.Entry {
	c := bucketindex.Claim{Blocks: claimed, Group: bucketindex.Range(0, first, first+n-1)}
	out := make([]bucketindex.Entry, n)

	for i := range n {
		out[i] = bucketindex.Entry{
			Prefix: fmt.Sprintf("%s%02d", name, i), Blocks: bucketindex.Blocks(first + i), Claim: c, Level: 1,
		}
	}

	return out
}

func prefixes(u Unit) []string {
	out := make([]string, len(u))
	for i := range u {
		out[i] = u[i].Entry.Prefix
	}

	return out
}

func TestRunLocal(t *testing.T) {
	t.Parallel()

	local := bucketindex.Entry{Prefix: "succ", Blocks: bucketindex.Range(0, 1, 2), Level: 1}
	p := &peer{}

	plan := Pass{Fetcher: p}.Run(context.Background(), []bucketindex.Entry{local},
		[]bucketindex.Want{want("a", 1)}, []bucketindex.Entry{want("h", 2).Entry()})

	assert.Equal(t, int64(1), plan.Stats.Local)
	assert.Equal(t, int64(1), plan.Stats.Revoked)
	assert.Empty(t, plan.Units)
	assert.Empty(t, p.calls, "nothing covered locally reaches a peer")
}

func TestRunHeld(t *testing.T) {
	t.Parallel()

	p := &peer{}
	held := Pass{Fetcher: p, Hold: func(_ context.Context, prefix string) bool { return prefix == "a" }}

	plan := held.Run(context.Background(), nil, []bucketindex.Want{want("a", 1)}, nil)

	require.Len(t, plan.Units, 1)
	assert.True(t, plan.Units[0][0].Held)
	assert.Empty(t, p.calls)
}

func TestRunCapsTargets(t *testing.T) {
	t.Parallel()

	p := &peer{}

	wants := make([]bucketindex.Want, 0, FetchesPerCycle+2)
	for i := range uint64(FetchesPerCycle + 2) {
		wants = append(wants, want(strconv.FormatUint(i, 10), i+1))
	}

	plan := Pass{Fetcher: p}.Run(context.Background(), nil, wants, nil)

	assert.Equal(t, FetchesPerCycle, p.asked())
	assert.Equal(t, int64(FetchesPerCycle), plan.Stats.Unsatisfiable)
	assert.Len(t, plan.Attempts, FetchesPerCycle)
}

func TestRunNoFetcher(t *testing.T) {
	t.Parallel()

	plan := Pass{}.Run(context.Background(), nil, []bucketindex.Want{want("a", 1)}, nil)

	assert.Zero(t, plan)
}

func TestRunFailures(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		fetch fetcherFunc
	}{
		{"Error", func(w []bucketindex.Want) []bucketindex.FetchResult {
			out := make([]bucketindex.FetchResult, len(w))
			for i := range out {
				out[i].Err = errors.New("unreachable")
			}

			return out
		}},
		{"Short", func([]bucketindex.Want) []bucketindex.FetchResult { return nil }},
		{"Incomplete", func(w []bucketindex.Want) []bucketindex.FetchResult {
			out := make([]bucketindex.FetchResult, len(w))
			for i := range out {
				out[i].Outcome = bucketindex.WantIncomplete
			}

			return out
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			plan := Pass{Fetcher: tc.fetch}.Run(context.Background(), nil,
				[]bucketindex.Want{want("a", 1)}, []bucketindex.Entry{want("h", 2).Entry()})

			assert.Empty(t, plan.Units)

			if tc.name == "Incomplete" {
				assert.Equal(t, int64(2), plan.Stats.Incomplete)
				assert.Len(t, plan.Attempts, 2)
				assert.Empty(t, plan.Failed)

				return
			}

			assert.Equal(t, int64(2), plan.Stats.Failed)
			assert.Equal(t, []string{"a"}, plan.Failed, "a hole's failed attempt is no want's")
			assert.Empty(t, plan.Attempts, "a failed attempt concludes nothing")
		})
	}
}

type fetcherFunc func([]bucketindex.Want) []bucketindex.FetchResult

func (f fetcherFunc) FetchWants(_ context.Context, w []bucketindex.Want) []bucketindex.FetchResult {
	return f(w)
}

func TestRunCompletesWideGroup(t *testing.T) {
	t.Parallel()

	const width = 32

	members := group("f", 10, width, bucketindex.Blocks(1))
	p := &peer{ix: bucketindex.Index{Entries: members}}

	plan := Pass{Fetcher: p}.Run(context.Background(), nil, []bucketindex.Want{want("lost", 1)}, nil)

	require.Len(t, plan.Units, 1)
	assert.Len(t, plan.Units[0], width, "the whole group in one pass, whatever the per-cycle cap")
	assert.Equal(t, "lost", plan.Units[0][0].Want.Prefix)
	assert.Len(t, p.calls, 2, "the want, then every missing member in one call")
	assert.Equal(t, width, p.asked())
	assert.Zero(t, plan.Stats)
	require.Len(t, plan.Attempts, 1)
	assert.Equal(t, bucketindex.WantSatisfied, plan.Attempts[0].Outcome)
}

func TestRunSharedGroupAskedOnce(t *testing.T) {
	t.Parallel()

	members := group("f", 10, 6, bucketindex.Range(0, 1, 2))
	p := &peer{ix: bucketindex.Index{Entries: members}}

	plan := Pass{Fetcher: p}.Run(context.Background(), nil,
		[]bucketindex.Want{want("a", 1), want("b", 2)}, nil)

	require.Len(t, plan.Units, 2)
	assert.Equal(t, 2+5, p.asked(), "each member block is asked for once per pass")
}

func TestRunNestedGroup(t *testing.T) {
	t.Parallel()

	outer := group("f", 10, 3, bucketindex.Blocks(1))
	// f02 was split again, with block 7, into a group over 20..21.
	inner := group("g", 20, 2, bucketindex.Blocks(12, 7))
	p := &peer{ix: bucketindex.Index{Entries: append(outer[:2:2], inner...)}}

	plan := Pass{Fetcher: p}.Run(context.Background(), nil, []bucketindex.Want{want("lost", 1)}, nil)

	require.Len(t, plan.Units, 1)
	assert.ElementsMatch(t, []string{"f00", "f01", "g00", "g01"}, prefixes(plan.Units[0]))
	assert.Len(t, p.calls, 3, "the inner group is a third round")
}

func TestRunShortGroup(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		member   bucketindex.FetchResult
		outcome  bucketindex.WantOutcome
		failed   bool
		observed func(s bucketindex.RepairStats) int64
	}{
		{
			"Absent",
			bucketindex.FetchResult{Outcome: bucketindex.WantAbsent},
			bucketindex.WantAbsent, false,
			func(s bucketindex.RepairStats) int64 { return s.Unsatisfiable },
		},
		{
			"Incomplete",
			bucketindex.FetchResult{Outcome: bucketindex.WantIncomplete},
			bucketindex.WantIncomplete, false,
			func(s bucketindex.RepairStats) int64 { return s.Incomplete },
		},
		{
			"Error",
			bucketindex.FetchResult{Err: errors.New("unreachable")},
			0, true,
			func(s bucketindex.RepairStats) int64 { return s.Failed - 14 },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			members := group("f", 10, 8, bucketindex.Blocks(1))
			p := &peer{
				ix:     bucketindex.Index{Entries: members},
				member: map[bucketindex.Block]bucketindex.FetchResult{{N: 17}: tc.member},
			}

			plan := Pass{Fetcher: p}.Run(context.Background(), nil,
				[]bucketindex.Want{want("lost", 1)}, []bucketindex.Entry{want("hole", 1).Entry()})

			assert.Empty(t, plan.Units, "no part of a short group is committed")
			assert.Equal(t, int64(1), tc.observed(plan.Stats))
			assert.GreaterOrEqual(t, plan.Stats.Failed, int64(2*7),
				"each unit's seven parts were fetched but not committed")

			if tc.failed {
				assert.Equal(t, []string{"lost"}, plan.Failed, "a failed member concludes nothing about the want")
				assert.Empty(t, plan.Attempts)

				return
			}

			require.Len(t, plan.Attempts, 2)
			assert.Equal(t, "hole", plan.Attempts[0].Want.Prefix)

			assert.Equal(t, "lost", plan.Attempts[1].Want.Prefix)
			assert.Equal(t, tc.outcome, plan.Attempts[1].Outcome)
		})
	}
}

func TestAdmit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	members := group("f", 10, 3, bucketindex.Blocks(1))

	unitOf := func(w bucketindex.Want, hole, held bool, entries ...bucketindex.Entry) Unit {
		u := make(Unit, 0, len(entries))
		u = append(u, Result{Target: Target{Want: w, Hole: hole}, Entry: entries[0], Held: held})
		for _, ent := range entries[1:] {
			u = append(u, Result{Target: Target{Want: bucketindex.Want{Blocks: ent.Blocks}, Member: true}, Entry: ent})
		}

		return u
	}

	unit := func(prefix string, hole, held bool, entries ...bucketindex.Entry) Unit {
		return unitOf(want(prefix, 1), hole, held, entries...)
	}

	final := func(live, got []bucketindex.Entry, retired map[string]struct{}) []string {
		var out []string

		for _, e := range slices.Concat(live, got) {
			if _, ok := retired[e.Prefix]; !ok {
				out = append(out, e.Prefix)
			}
		}

		return out
	}

	opener := func(bad ...string) (func(*Result) error, *[]string) {
		var opened []string

		return func(r *Result) error {
			if slices.Contains(bad, r.Entry.Prefix) {
				return errors.New("unreadable")
			}

			opened = append(opened, r.Entry.Prefix)

			return nil
		}, &opened
	}

	t.Run("Whole", func(t *testing.T) {
		t.Parallel()

		var stats bucketindex.RepairStats

		open, opened := opener()
		got, _ := Admit(ctx, nil, nil, []Unit{unit("a", false, false, members...)}, open, &stats)

		assert.Equal(t, members, got)
		assert.Equal(t, []string{"f00", "f01", "f02"}, *opened)
		assert.Equal(t, int64(3), stats.Fetched)
	})

	t.Run("MemberWillNotOpen", func(t *testing.T) {
		t.Parallel()

		var stats bucketindex.RepairStats

		plain := bucketindex.Entry{Prefix: "b", Blocks: bucketindex.Blocks(5)}
		open, _ := opener("f02")
		got, _ := Admit(ctx, nil, nil, []Unit{
			unit("a", false, false, members...),
			unit("b", false, false, plain),
		}, open, &stats)

		assert.Equal(t, []bucketindex.Entry{plain}, got, "the group is dropped whole, the other unit is not")
		assert.Equal(t, int64(3), stats.Failed)
		assert.Equal(t, int64(1), stats.Fetched)
	})

	t.Run("ShortAtCommit", func(t *testing.T) {
		t.Parallel()

		var stats bucketindex.RepairStats

		open, _ := opener()
		got, _ := Admit(ctx, nil, nil, []Unit{unit("a", false, false, members[:2]...)}, open, &stats)

		assert.Empty(t, got)
		assert.Equal(t, int64(2), stats.Failed)
	})

	t.Run("AlreadyLiveAndCovered", func(t *testing.T) {
		t.Parallel()

		var stats bucketindex.RepairStats

		live := []bucketindex.Entry{members[0]}
		succ := bucketindex.Entry{Prefix: "s", Blocks: bucketindex.Range(0, 1, 2), Level: 2}
		phantom := bucketindex.Entry{Prefix: "x", Blocks: bucketindex.Blocks(2)}

		open, opened := opener("x")
		got, retired := Admit(ctx, live, nil, []Unit{
			unit("a", false, false, members...),
			unit("b", false, true, succ),
			unit("c", true, false, phantom),
		}, open, &stats)

		assert.Equal(t, []bucketindex.Entry{succ}, got, "the successor holds every member's rows")
		assert.Equal(t, []string{"s"}, final(live, got, retired),
			"block 1 is live once, not as the group and the successor")
		assert.Equal(t, []string{"f01", "f02", "s"}, *opened, "a live part is not opened again")
		assert.Equal(t, bucketindex.RepairStats{Local: 1, Revoked: 1}, stats,
			"only parts published count, and a copy the commit already covers is no failure")
	})

	// A part covering only some of a group's claim overlaps every member and replaces none: the
	// commit keeps one of the two representations, whichever order the units come in.
	t.Run("PartialSuccessor", func(t *testing.T) {
		t.Parallel()

		wide := group("g", 20, 2, bucketindex.Range(0, 1, 2))
		part := bucketindex.Entry{Prefix: "p", Blocks: bucketindex.Range(0, 2, 3), Level: 2}
		units := []Unit{
			unit("a", false, false, wide...),
			unitOf(want("b", 3), false, false, part),
		}

		for _, tc := range []struct {
			name string
			live []bucketindex.Entry
			want []bucketindex.Entry
		}{
			{"MemberLive", wide[:1], wide[1:]},
			{"NothingLive", nil, wide},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				for _, order := range [][]Unit{units, {units[1], units[0]}} {
					var stats bucketindex.RepairStats

					open, _ := opener()
					got, _ := Admit(ctx, tc.live, nil, order, open, &stats)

					assert.ElementsMatch(t, tc.want, got)
					assert.Equal(t, int64(1), stats.Failed, "the part left out is counted")
				}
			})
		}
	})

	t.Run("SuccessorAtTheGroupsLevel", func(t *testing.T) {
		t.Parallel()

		var stats bucketindex.RepairStats

		rival := bucketindex.Entry{Prefix: "r", Blocks: bucketindex.Range(0, 1, 2), Level: 1}
		open, _ := opener()
		got, _ := Admit(ctx, members[:1], nil, []Unit{
			unit("a", false, false, members...),
			unit("b", false, false, rival),
		}, open, &stats)

		assert.Equal(t, members[1:], got, "neither replaces the other, and the group is partly live already")
	})

	// The outer group's claim is carried only by members the commit leaves out, yet it is what
	// relates the successor to the live inner group.
	t.Run("NestedGroup", func(t *testing.T) {
		t.Parallel()

		inner := group("g", 20, 2, bucketindex.Blocks(7, 12))
		for i := range inner {
			inner[i].Level = 2
		}

		for _, tc := range []struct {
			name    string
			blocks  bucketindex.Interval
			got     []bucketindex.Entry
			retired []string
		}{
			{"Whole", bucketindex.Blocks(1, 2, 7), nil, []string{"g00", "g01"}},
			{"WithoutBlock7", bucketindex.Blocks(1, 2), members[:2], nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				var stats bucketindex.RepairStats

				succ := bucketindex.Entry{Prefix: "s", Blocks: tc.blocks, Level: 3}
				if tc.got == nil {
					tc.got = []bucketindex.Entry{succ}
				}

				open, _ := opener()
				got, retired := Admit(ctx, inner, nil, []Unit{
					unit("a", false, false, members[:2]...),
					unit("b", false, false, succ),
				}, open, &stats)

				assert.Equal(t, tc.got, got)
				assert.ElementsMatch(t, tc.retired, slices.Collect(maps.Keys(retired)))
			})
		}
	})
}

func TestConfirmLost(t *testing.T) {
	t.Parallel()

	evidence := map[string]int{"gone": 2}
	wants := []bucketindex.Want{want("a", 1), want("b", 2), want("c", 3)}
	absent := func(prefix string) Result {
		return Result{Target: Target{Want: want(prefix, 1)}, Outcome: bucketindex.WantAbsent}
	}

	for range HoleConfirmations - 1 {
		assert.Empty(t, ConfirmLost(evidence, wants, []Result{absent("a"), absent("b"), absent("c")}, nil))
	}

	assert.NotContains(t, evidence, "gone", "evidence for a want no longer outstanding is forgotten")

	satisfied := absent("b")
	satisfied.Outcome = bucketindex.WantSatisfied
	member := absent("c")
	member.Member = true

	lost := ConfirmLost(evidence, wants, []Result{absent("a"), satisfied, member}, nil)
	assert.Equal(t, []bucketindex.Want{wants[0]}, lost)
	assert.Equal(t, map[string]int{"c": 2}, evidence, "a member is never evidence, and any other outcome resets")

	ConfirmLost(evidence, wants, nil, []string{"c"})
	assert.Empty(t, evidence, "a failed attempt breaks the run")
}
