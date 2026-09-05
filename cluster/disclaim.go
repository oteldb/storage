package cluster

import (
	"sync/atomic"

	"github.com/go-faster/errors"
)

// A fan-out across a shard's owners has to decide what "nobody answered" means, and there are two
// answers with opposite consequences:
//
//   - Every owner disclaims the shard because none of them holds it. The shard has no data anywhere,
//     and an empty result is the truth.
//   - An owner holds the shard and knows it is missing data for the window — a head lost at restart,
//     or an unsatisfied repair obligation. Answering empty there is a wrong number dressed as a
//     successful query, and no consumer can tell it from real absence.
//
// [Disclaims] is where the two are kept apart, so no fan-out has to re-derive the rule. Both kinds
// still fail over — a complete owner answering is the good outcome, and the common one — but only an
// all-absent fan-out may collapse to empty.

// Disclaims tallies how a shard's owners refused a read. The zero value is ready to use, and [Note]
// is safe to call from the concurrent attempts of a hedged fan-out.
type Disclaims struct {
	absent     atomic.Int64
	incomplete atomic.Int64
}

// Note records one owner's answer, counting it only when it is a disclaim.
func (d *Disclaims) Note(err error) {
	switch {
	// [ErrShardIncomplete] unwraps to [ErrShardAbsent], so it must be tested first.
	case errors.Is(err, ErrShardIncomplete):
		d.incomplete.Add(1)
	case errors.Is(err, ErrShardAbsent):
		d.absent.Add(1)
	}
}

// Empty reports whether the fan-out may answer with an empty result: every one of the owners
// disclaimed, and every disclaim was an absence. One incomplete owner is enough to deny it — the
// caller then returns the error it already has.
func (d *Disclaims) Empty(owners int) bool {
	return d.incomplete.Load() == 0 && int(d.absent.Load()) >= owners
}

// Failed reports the other end of [Disclaims.Empty]: every one of the owners disclaimed, and at
// least one of them held the shard and knew it was short. There is no complete answer anywhere, so
// the read fails.
func (d *Disclaims) Failed(owners int) bool {
	return d.incomplete.Load() > 0 && int(d.absent.Load()+d.incomplete.Load()) >= owners
}

// Absent is how many owners answered that they do not hold the shard.
func (d *Disclaims) Absent() int64 { return d.absent.Load() }

// Incomplete is how many owners answered that they hold the shard but are missing data for the
// window.
func (d *Disclaims) Incomplete() int64 { return d.incomplete.Load() }
