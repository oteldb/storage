// Package diskguard refuses a write the medium cannot store, instead of accepting it and losing it
// later.
//
// A flush that runs out of disk or of inodes fails after the head has been detached, burns a part
// sequence, and leaves the head to keep growing behind it — so a node with a full disk manufactures
// its own damage for as long as the disk stays full, while still acking every write. The guard
// closes that loop: a flush asks whether the backend can take the part *before* writing it, and a
// failure latches, turning the engine's ingest path into a distinct, retryable error until a later
// flush finds room again.
//
// Bytes and inodes are checked as independent axes. A filesystem with terabytes free can still fail
// every create once its inode table is exhausted, and a part is many small objects, so the two
// failures are equally likely and byte accounting cannot see the second one.
package diskguard

import (
	"context"
	"sync/atomic"
	"syscall"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// Defaults for the headroom left unused. They are small on purpose: the guard exists to catch a
// medium that is *actually* out, not to enforce a capacity policy, and a large default would turn a
// healthy small volume read-only. The headroom's job is only to leave a merge room for its output,
// since a merge must write before it can retire the inputs it frees.
const (
	DefaultReserveBytes  = 64 << 20
	DefaultReserveInodes = 1024
)

// Reserve is the headroom a store keeps free beyond what a pending write needs. A zero field takes
// the default above; a negative one disables that axis.
type Reserve struct {
	Bytes  int64
	Inodes int64
}

func (r Reserve) bytes() int64 {
	if r.Bytes == 0 {
		return DefaultReserveBytes
	}

	return r.Bytes
}

func (r Reserve) inodes() int64 {
	if r.Inodes == 0 {
		return DefaultReserveInodes
	}

	return r.Inodes
}

// Guard is one engine's disk-pressure latch. The zero value is usable and takes the default
// reserve. Safe for concurrent use.
type Guard struct {
	reserve Reserve
	// failed holds the reason the medium was last found unable to take a write, or nil when it can.
	// A pointer so the reason travels with the flag: an operator needs to know which axis ran out.
	failed atomic.Pointer[error]
	// rejections counts the writes turned away since the last time the state was published.
	rejections atomic.Int64
}

// New returns a guard keeping r free beyond each write's own requirement.
func New(r Reserve) *Guard { return &Guard{reserve: r} }

// Admit reports whether b can take a write of needBytes across needObjects objects, and latches the
// verdict either way — so a passing check is also what clears a previous failure. A backend that
// cannot report an axis ([backend.ErrSpaceUnknown]) is unbounded on that axis, never failing.
//
// A backend that reports neither axis therefore clears a latch set by [Guard.Observe] on the next
// attempt. That is the intended behavior and the only one available: nothing can say when such a
// medium has room again except writing to it, so the flush cadence is the retry, and a write that
// fails again re-latches immediately.
//
// The returned error wraps [backend.ErrNoSpace]. A probe that fails for any other reason (a statfs
// that errored) is not a verdict: it is returned as-is and leaves the latch alone, because refusing
// writes on a broken probe is a worse failure than the one being guarded against.
func (g *Guard) Admit(ctx context.Context, b backend.Backend, needBytes, needObjects int64) error {
	if b == nil {
		return nil
	}

	verdict, probeErr := g.check(ctx, b, needBytes, needObjects)
	if probeErr != nil {
		return probeErr
	}

	if verdict != nil {
		g.failed.Store(&verdict)

		return verdict
	}

	g.failed.Store(nil)

	return nil
}

// Observe latches err when it is an out-of-space failure, so a write that raced past [Guard.Admit]
// and got ENOSPC from the medium itself closes the ingest path just as a failed check would. Any
// other error is left alone: a transient backend fault must not make the node read-only.
func (g *Guard) Observe(err error) {
	if !IsNoSpace(err) {
		return
	}

	// Restated over [backend.ErrNoSpace] rather than kept verbatim: this is the latched *state*, read
	// by an ingest path that matches one sentinel, while the original error — ENOSPC and all — was
	// already returned to whoever ran the flush.
	latched := errors.Wrapf(backend.ErrNoSpace, "the medium refused a write (%v)", err)
	g.failed.Store(&latched)
}

// Refuse is [Guard.Err] for the ingest path: it returns the same reason and counts one refused
// write, which [Guard.TakeRejections] later publishes.
func (g *Guard) Refuse() error {
	err := g.Err()
	if err != nil {
		g.rejections.Add(1)
	}

	return err
}

// TakeRejections returns the writes refused since the previous call and resets the count, so the
// caller that publishes the state also publishes its cost.
func (g *Guard) TakeRejections() int64 { return g.rejections.Swap(0) }

// Err returns why the medium cannot take writes, or nil while it can. The error wraps
// [backend.ErrNoSpace].
func (g *Guard) Err() error {
	if p := g.failed.Load(); p != nil {
		return *p
	}

	return nil
}

// Exhausted reports whether the medium is currently refusing writes.
func (g *Guard) Exhausted() bool { return g.failed.Load() != nil }

func (g *Guard) check(ctx context.Context, b backend.Backend, needBytes, needObjects int64) (verdict, probeErr error) {
	if reserve := g.reserve.bytes(); reserve >= 0 {
		free, err := backend.FreeSpace(ctx, b)
		switch {
		case errors.Is(err, backend.ErrSpaceUnknown):
		case err != nil:
			return nil, errors.Wrap(err, "probe free space")
		case free < needBytes+reserve:
			return errors.Wrapf(backend.ErrNoSpace,
				"%d bytes free, need %d plus %d reserved", free, needBytes, reserve), nil
		}
	}

	if reserve := g.reserve.inodes(); reserve >= 0 {
		free, err := backend.FreeInodes(ctx, b)
		switch {
		case errors.Is(err, backend.ErrSpaceUnknown):
		case err != nil:
			return nil, errors.Wrap(err, "probe free inodes")
		case free < needObjects+reserve:
			return errors.Wrapf(backend.ErrNoSpace,
				"%d inodes free, need %d plus %d reserved", free, needObjects, reserve), nil
		}
	}

	return nil, nil
}

// IsNoSpace reports whether err says the medium is out of room — the library's own
// [backend.ErrNoSpace], or the ENOSPC a filesystem returns for both a full disk and an exhausted
// inode table.
func IsNoSpace(err error) bool {
	return err != nil && (errors.Is(err, backend.ErrNoSpace) || errors.Is(err, syscall.ENOSPC))
}
