package repair

import (
	"context"
	"slices"

	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend/bucketindex"
)

// Admit returns the entries a repair commit publishes out of units, given live, the engine's part
// set, and counts into stats what it publishes and what it leaves out.
//
// open makes a part readable for the commit; a part that will not open fails its whole unit, unless
// what it was fetched for is already covered by this commit, in which case it is skipped. A part
// already live, or admitted by an earlier unit, is not opened again. A unit that is not
// [committable] once its parts are open is dropped whole. Last, a part another published or live
// part supersedes is left out: a peer that merged between two rounds can answer one unit with both
// a member and the successor containing it, and publishing both would read its rows twice.
func Admit(
	ctx context.Context, live []bucketindex.Entry, units []Unit, open func(*Result) error, stats *bucketindex.RepairStats,
) []bucketindex.Entry {
	have := make(map[string]struct{}, len(live))
	for i := range live {
		have[live[i].Prefix] = struct{}{}
	}

	var (
		admitted []bucketindex.Entry
		picked   []*Result
	)

	for _, u := range units {
		base := slices.Concat(live, admitted)
		held := slices.Clone(base)
		added := make([]*Result, 0, len(u))
		ok := true

		for i := range u {
			r := &u[i]
			if _, dup := have[r.Entry.Prefix]; dup || slices.ContainsFunc(added, r.samePart) {
				continue
			}

			if err := open(r); err != nil {
				if _, covered := (&bucketindex.Index{Entries: held}).Satisfying(r.Want); covered {
					continue
				}

				zctx.From(ctx).Warn("repaired part is not readable",
					zap.String("part", r.Entry.Prefix), zap.Error(err))

				ok = false

				break
			}

			held = append(held, r.Entry)
			added = append(added, r)
		}

		if ok && !committable(base, u[0].Want, held[len(base):]) {
			zctx.From(ctx).Warn("repaired split group is not complete at commit",
				zap.String("want", u[0].Want.Prefix))

			ok = false
		}

		if !ok {
			stats.Failed += int64(len(u))

			continue
		}

		if u[0].Hole {
			stats.Revoked++
		}

		for _, r := range added {
			have[r.Entry.Prefix] = struct{}{}
			admitted = append(admitted, r.Entry)
			picked = append(picked, r)
		}
	}

	superseded := bucketindex.Subsumed(admitted, slices.Concat(live, admitted))
	out := admitted[:0]

	for i, r := range picked {
		if _, drop := superseded[r.Entry.Prefix]; drop {
			continue
		}

		switch {
		case r.Hole:
		case r.Held:
			stats.Local++
		default:
			stats.Fetched++
		}

		out = append(out, admitted[i])
	}

	return out
}

func (r *Result) samePart(o *Result) bool { return r.Entry.Prefix == o.Entry.Prefix }

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
