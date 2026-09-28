package bucketindex

import "github.com/go-faster/errors"

// ErrSuperseded is returned (wrapped) by a clustered writer that refuses to commit because it can no
// longer prove it writes as the shard's current tenure. Whatever the refused commit would have
// published is left unreferenced, for the orphan sweep to reclaim.
var ErrSuperseded = errors.New("bucketindex: commit fenced: not the shard's current tenure")

// CheckTenure is the commit fence a clustered writer runs before each conditional index write.
// stamped is the ownership term the operation began under, current the term the writer holds now
// (0 for none, including a claim it can no longer prove), and indexed the term of the index the
// commit is built on.
//
// Each refusal closes a way for a writer that has been displaced to commit anyway: a claim that
// lapsed or was fenced, a tenure that ended and restarted while the operation was in flight, and an
// index a later tenure already wrote, which a rebase reveals and a commit must never adopt.
func CheckTenure(stamped, current, indexed uint64) error {
	switch {
	case current == 0:
		return errors.Wrap(ErrSuperseded, "no claim on the shard")
	case stamped != current:
		return errors.Wrapf(ErrSuperseded, "operation began under term %d, the claim is now term %d", stamped, current)
	case indexed > current:
		return errors.Wrapf(ErrSuperseded, "index committed under term %d, above this writer's %d", indexed, current)
	default:
		return nil
	}
}
