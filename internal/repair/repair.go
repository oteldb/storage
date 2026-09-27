// Package repair is the part-repair pass the metric and record engines share: which wants and holes
// a cycle attempts, the fetch rounds that complete a split group, the absence evidence a hole is
// earned by, and which of the fetched parts a commit may publish. The engines keep only what touches
// their own part type and state. See engine/ARCH.md, "Repair".
package repair

import (
	"cmp"
	"context"
	"maps"
	"slices"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend/bucketindex"
)

// FetchesPerCycle bounds how many wants and holes one pass asks peers for. The split-group members
// an answer needs are not counted against it: see [Pass.Run].
const FetchesPerCycle = 4

// HoleConfirmations is how many consecutive repair attempts must reach the same definitive-absence
// conclusion before the loss is acknowledged with a hole.
const HoleConfirmations = 3

// Target is one part a pass tries to make local: an outstanding want, the identity a hole stands in
// for, or a member of a split group some other target's answer needs.
type Target struct {
	Want bucketindex.Want
	Hole bool
	// Member marks a split-group member asked for by block. It names no prefix, earns no evidence
	// and never reaches the index as a want.
	Member bool
}

// Result is what one target's attempt concluded.
type Result struct {
	Target

	Entry   bucketindex.Entry
	Outcome bucketindex.WantOutcome
	// Held marks a part this node's own backend still held, opened without a peer.
	Held bool
}

// Unit is one want or hole and the split-group members its answer needs, in that order: what a
// commit publishes whole or not at all.
type Unit []Result

// Pass is one engine's repair pass.
type Pass struct {
	Fetcher bucketindex.PartFetcher
	// Hold opens the part at prefix from this node's own backend and reports whether it is there.
	Hold func(ctx context.Context, prefix string) bool
	// Prefix names the engine in logs.
	Prefix string
	// Tried is the pass each want or hole was last sent to peers in; the cap takes the least recently
	// tried first, so wants that make no progress cannot starve the rest.
	Tried map[string]uint64
}

// Plan is what a pass concluded before its commit.
type Plan struct {
	// Units are the answers whose every split group is complete, ordered by the target's prefix.
	Units []Unit
	// Attempts are the wants and holes that reached a conclusion, with their unit's outcome.
	Attempts []Result
	// Failed names the wants whose attempt concluded nothing.
	Failed []string
	// Asked names the wants and holes sent to peers.
	Asked []string
	// Stats counts everything but the parts the commit publishes; [Admit] adds those.
	Stats bucketindex.RepairStats
}

// Run plans one pass over wants and holes against entries, the engine's live part set.
//
// At most [FetchesPerCycle] targets go to peers. A target answered by a split-group member is then
// completed in further rounds, each one [bucketindex.PartFetcher] call for every unit's missing
// members, until no unit is short of a member it has not asked for.
func (p Pass) Run(
	ctx context.Context, entries []bucketindex.Entry, wants []bucketindex.Want, holes []bucketindex.Entry,
) Plan {
	var plan Plan

	ix := bucketindex.Index{Entries: entries}
	pending := make([]Target, 0, len(wants)+len(holes))

	for i := range wants {
		if _, ok := ix.Satisfying(wants[i]); ok {
			plan.Stats.Local++

			continue
		}

		pending = append(pending, Target{Want: wants[i]})
	}

	for i := range holes {
		w := bucketindex.WantOf(holes[i], bucketindex.Generation{})
		if _, ok := ix.Satisfying(w); ok {
			plan.Stats.Revoked++

			continue
		}

		pending = append(pending, Target{Want: w, Hole: true})
	}

	var (
		units  []*unit
		remote []Target
	)

	for i := range pending {
		t := &pending[i]
		if p.Hold != nil && p.Hold(ctx, t.Want.Prefix) {
			units = append(units, newUnit(Result{
				Target: *t, Entry: t.Want.Entry(), Outcome: bucketindex.WantSatisfied, Held: true,
			}))

			continue
		}

		remote = append(remote, *t)
	}

	slices.SortStableFunc(remote, func(a, b Target) int {
		return cmp.Compare(p.Tried[a.Want.Prefix], p.Tried[b.Want.Prefix])
	})
	remote = remote[:min(len(remote), FetchesPerCycle)]

	fetched := p.fetch(ctx, remote, &plan.Stats)
	for i := range fetched {
		a := &fetched[i]
		plan.Asked = append(plan.Asked, a.Want.Prefix)

		switch {
		case a.err != nil:
			if !a.Hole {
				plan.Failed = append(plan.Failed, a.Want.Prefix)
			}
		case a.Outcome != bucketindex.WantSatisfied:
			plan.Attempts = append(plan.Attempts, a.Result)
		default:
			units = append(units, newUnit(a.Result))
		}
	}

	p.complete(ctx, &ix, units, &plan.Stats)

	slices.SortFunc(units, func(a, b *unit) int { return cmp.Compare(a.results[0].Want.Prefix, b.results[0].Want.Prefix) })

	for _, u := range units {
		target := u.results[0]

		if u.whole(&ix) {
			plan.Units = append(plan.Units, u.results)
			plan.Attempts = append(plan.Attempts, target)

			continue
		}

		// Every part the unit brought is left out of the commit, so none of them was repaired.
		plan.Stats.Failed += int64(len(u.results))

		zctx.From(ctx).Warn("repair could not complete a split group",
			zap.String("prefix", p.Prefix), zap.String("want", target.Want.Prefix),
			zap.Int("members", len(u.results)))

		if u.worst == outcomeFailed {
			if !target.Hole {
				plan.Failed = append(plan.Failed, target.Want.Prefix)
			}

			continue
		}

		target.Outcome = u.worst.want()
		plan.Attempts = append(plan.Attempts, target)
	}

	return plan
}

// attempt is one target's fetch; err is a transient failure and concludes nothing.
type attempt struct {
	Result

	err error
}

// fetch asks the fetcher for every target in one call and counts each attempt that brought nothing.
func (p Pass) fetch(ctx context.Context, targets []Target, stats *bucketindex.RepairStats) []attempt {
	if p.Fetcher == nil || len(targets) == 0 {
		return nil
	}

	wants := make([]bucketindex.Want, len(targets))
	for i := range targets {
		wants[i] = targets[i].Want
	}

	results := p.Fetcher.FetchWants(ctx, wants)
	out := make([]attempt, len(targets))

	for i := range targets {
		t := &targets[i]

		// A short answer is a broken fetcher, not evidence: count it as a transient failure so the
		// want stays outstanding.
		r := bucketindex.FetchResult{
			Err: errors.Errorf("repair fetcher returned %d results for %d wants", len(results), len(targets)),
		}
		if i < len(results) {
			r = results[i]
		}

		out[i] = attempt{Result: Result{Target: *t, Entry: r.Entry, Outcome: r.Outcome}, err: r.Err}

		lg := zctx.From(ctx).With(zap.String("prefix", p.Prefix), zap.String("want", t.Want.Prefix))

		switch {
		case r.Err != nil:
			stats.Failed++

			lg.Warn("repair fetch failed", zap.Error(r.Err))
		case r.Outcome == bucketindex.WantAbsent:
			stats.Unsatisfiable++

			lg.Warn("no owner holds a part satisfying the want")
		case r.Outcome == bucketindex.WantIncomplete:
			stats.Incomplete++

			lg.Warn("repair could not reach every owner of the shard")
		}
	}

	return out
}

// complete runs the member rounds: each asks, in one fetcher call, for every block some unit's split
// groups still lack. A block is asked for once per pass however many units need it.
func (p Pass) complete(ctx context.Context, ix *bucketindex.Index, units []*unit, stats *bucketindex.RepairStats) {
	answers := make(map[uint64]attempt)

	for {
		var (
			ask      []Target
			askedBy  = make(map[uint64][]*unit)
			progress bool
		)

		for _, u := range units {
			if u.worst != outcomeNone {
				continue
			}

			for _, b := range u.missing(ix) {
				u.asked[b] = struct{}{}
				progress = true

				if a, ok := answers[b]; ok {
					u.take(a)

					continue
				}

				if _, dup := askedBy[b]; !dup {
					w := u.results[0].Want
					ask = append(ask, Target{
						Want:   bucketindex.Want{Blocks: bucketindex.Blocks(b), MinTime: w.MinTime, MaxTime: w.MaxTime},
						Member: true,
					})
				}

				askedBy[b] = append(askedBy[b], u)
			}
		}

		if !progress {
			return
		}

		fetched := p.fetch(ctx, ask, stats)
		for i := range fetched {
			b := fetched[i].Want.Blocks.Min
			answers[b] = fetched[i]

			for _, u := range askedBy[b] {
				u.take(fetched[i])
			}
		}
	}
}

// outcome ranks why a unit is short of a member: the strongest claim wins, and only absence
// throughout is evidence.
type outcome uint8

const (
	outcomeNone outcome = iota
	outcomeAbsent
	outcomeIncomplete
	outcomeFailed
)

func (o outcome) want() bucketindex.WantOutcome {
	if o == outcomeAbsent {
		return bucketindex.WantAbsent
	}

	return bucketindex.WantIncomplete
}

// unit is a [Unit] under construction.
type unit struct {
	results []Result
	asked   map[uint64]struct{}
	// worst is why the unit can no longer complete this pass; outcomeNone while it still can.
	worst outcome
}

func newUnit(r Result) *unit {
	return &unit{results: []Result{r}, asked: make(map[uint64]struct{})}
}

func (u *unit) entries(ix *bucketindex.Index) []bucketindex.Entry {
	out := slices.Clone(ix.Entries)
	for i := range u.results {
		out = append(out, u.results[i].Entry)
	}

	return out
}

// missing is the member blocks the unit still needs and has not asked for: every group of its parts
// while its target is not yet answered, and otherwise the groups whose lone members would duplicate
// rows the index already holds ([groupsNeeded]).
func (u *unit) missing(ix *bucketindex.Index) []uint64 {
	entries := u.entries(ix)
	held := (&bucketindex.Index{Entries: entries}).Covered()
	out := make(map[uint64]struct{})

	for _, c := range groupsNeeded(ix.Entries, u.results[0].Want, entries[len(ix.Entries):]) {
		c.Group.Each(func(b uint64) bool {
			if _, asked := u.asked[b]; !asked && !held.Contains(bucketindex.Blocks(b)) {
				out[b] = struct{}{}
			}

			return true
		})
	}

	return slices.Sorted(maps.Keys(out))
}

func (u *unit) take(a attempt) {
	switch {
	case a.err != nil:
		u.worst = outcomeFailed
	case a.Outcome == bucketindex.WantAbsent:
		u.worst = max(u.worst, outcomeAbsent)
	case a.Outcome == bucketindex.WantIncomplete:
		u.worst = max(u.worst, outcomeIncomplete)
	default:
		for i := range u.results {
			if u.results[i].Entry.Prefix == a.Entry.Prefix {
				return
			}
		}

		u.results = append(u.results, a.Result)
	}
}

// whole reports whether the unit may be committed over the index ([committable]). A unit that
// stopped short for no recorded reason is a failure, never evidence; so is one whose target's own
// answer satisfies it, because a member's absence says nothing about a part that exists.
func (u *unit) whole(ix *bucketindex.Index) bool {
	entries := u.entries(ix)
	if committable(ix.Entries, u.results[0].Want, entries[len(ix.Entries):]) {
		return true
	}

	target := u.results[0]
	if _, own := (&bucketindex.Index{Entries: []bucketindex.Entry{target.Entry}}).Satisfying(target.Want); own ||
		u.worst == outcomeNone {
		u.worst = outcomeFailed
	}

	return false
}
