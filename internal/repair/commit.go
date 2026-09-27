package repair

import (
	"context"
	"slices"

	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend/bucketindex"
)

// Admit returns the entries a repair commit publishes out of units and the prefixes of the live
// parts it retires, given live, the engine's part set, and counts into stats what it publishes and
// what it leaves out.
//
// open makes a part readable for the commit, once per part; a part already live is not opened. A
// part that will not open fails its unit unless what it was fetched for is covered anyway. Units are
// then judged together, each against live and every other unit still in: one that is not
// [committable] is dropped, and the judgement repeats until none is. Judging against the whole set,
// rather than the units before it, is what makes the outcome independent of unit order.
//
// The units taken are then pruned ([prune]): a part another published or live part subsumes is left
// out, what the published parts subsume is retired, and a unit that would leave two overlapping
// representations of some rows live is dropped. Either can change what the rest may commit, so
// judgement and pruning repeat until neither drops a unit.
func Admit(
	ctx context.Context, live []bucketindex.Entry, units []Unit, open func(*Result) error, stats *bucketindex.RepairStats,
) ([]bucketindex.Entry, map[string]struct{}) {
	have := make(map[string]struct{}, len(live))
	for i := range live {
		have[live[i].Prefix] = struct{}{}
	}

	openErr := openAll(ctx, units, have, open)
	added := unitParts(units, have, openErr)

	in := make([]bool, len(units))
	for k := range in {
		in[k] = true
	}

	why := make([]string, len(units))

	var p pruned

	for {
		in = settle(live, units, added, openErr, in)

		p = prune(live, units, added, openErr, in)
		if len(p.drop) == 0 {
			break
		}

		for _, k := range p.drop {
			in[k], why[k] = false, p.reason
		}
	}

	var out []bucketindex.Entry

	for k, u := range units {
		if !in[k] {
			if why[k] == "" {
				why[k] = "repaired split group is not complete at commit"
			}

			zctx.From(ctx).Warn(why[k], zap.String("want", u[0].Want.Prefix))

			stats.Failed += int64(len(u))

			continue
		}

		if u[0].Hole {
			stats.Revoked++
		}

		for i := range u {
			r := &u[i]
			if _, dup := have[r.Entry.Prefix]; dup || openErr[r.Entry.Prefix] != nil {
				continue
			}

			have[r.Entry.Prefix] = struct{}{}

			if _, ok := p.kept[r.Entry.Prefix]; !ok {
				continue
			}

			switch {
			case r.Hole:
			case r.Held:
				stats.Local++
			default:
				stats.Fetched++
			}

			out = append(out, r.Entry)
		}
	}

	return out, p.retired
}

// unitParts is, per unit, the parts it would add: those neither live nor failing to open.
func unitParts(units []Unit, have map[string]struct{}, openErr map[string]error) [][]bucketindex.Entry {
	added := make([][]bucketindex.Entry, len(units))

	for k, u := range units {
		for i := range u {
			p := u[i].Entry.Prefix
			if _, dup := have[p]; !dup && openErr[p] == nil {
				added[k] = append(added[k], u[i].Entry)
			}
		}
	}

	return added
}

// openAll opens every part of units that is not live, once, and returns what each open concluded.
func openAll(
	ctx context.Context, units []Unit, have map[string]struct{}, open func(*Result) error,
) map[string]error {
	openErr := make(map[string]error)

	for _, u := range units {
		for i := range u {
			r := &u[i]
			if _, dup := have[r.Entry.Prefix]; dup {
				continue
			}

			if _, done := openErr[r.Entry.Prefix]; done {
				continue
			}

			err := open(r)
			if err != nil {
				zctx.From(ctx).Warn("repaired part is not readable",
					zap.String("part", r.Entry.Prefix), zap.Error(err))
			}

			openErr[r.Entry.Prefix] = err
		}
	}

	return openErr
}

// settle returns which of the units in the commit takes: every unit is judged against live and every
// other unit still in, all at once, until no verdict changes. A unit once out stays out.
func settle(
	live []bucketindex.Entry, units []Unit, added [][]bucketindex.Entry, openErr map[string]error, in []bool,
) []bool {
	for {
		verdict := make([]bool, len(units))

		for k, u := range units {
			if !in[k] {
				continue
			}

			base := slices.Clone(live)
			for o := range units {
				if o != k && in[o] {
					base = append(base, added[o]...)
				}
			}

			verdict[k] = admissible(base, u, added[k], openErr)
		}

		if slices.Equal(verdict, in) {
			return in
		}

		in = verdict
	}
}

// admissible reports whether u may be committed over base with its opened parts added: every part
// that would not open was fetched for something the commit covers anyway, and u is [committable].
func admissible(base []bucketindex.Entry, u Unit, added []bucketindex.Entry, openErr map[string]error) bool {
	all := &bucketindex.Index{Entries: slices.Concat(base, added)}

	for i := range u {
		if openErr[u[i].Entry.Prefix] == nil {
			continue
		}

		if _, covered := all.Satisfying(u[i].Want); !covered {
			return false
		}
	}

	return committable(base, u[0].Want, added)
}

// ConfirmLost advances evidence, the per-want count of consecutive definitive-absence conclusions,
// with a pass's attempts and returns the wants that have earned a hole. Only [bucketindex.WantAbsent]
// is evidence; any other conclusion, and a failed attempt named in failed, resets the count.
// Evidence for a want no longer outstanding is forgotten.
func ConfirmLost(
	evidence map[string]int, wants []bucketindex.Want, attempts []Result, failed []string,
) []bucketindex.Want {
	outstanding := make(map[string]struct{}, len(wants))
	for i := range wants {
		outstanding[wants[i].Prefix] = struct{}{}
	}

	for prefix := range evidence {
		if _, ok := outstanding[prefix]; !ok {
			delete(evidence, prefix)
		}
	}

	for _, prefix := range failed {
		delete(evidence, prefix)
	}

	var lost []bucketindex.Want

	for i := range attempts {
		r := &attempts[i]
		if r.Hole || r.Member {
			continue
		}

		if r.Outcome != bucketindex.WantAbsent {
			delete(evidence, r.Want.Prefix)

			continue
		}

		evidence[r.Want.Prefix]++
		if evidence[r.Want.Prefix] >= HoleConfirmations {
			lost = append(lost, r.Want)
			delete(evidence, r.Want.Prefix)
		}
	}

	return lost
}
