package backend

import (
	"context"

	"github.com/go-faster/errors"
)

// ErrSpaceUnknown is returned by [FreeSpace] for a backend that cannot report available capacity —
// an ephemeral or object-store backend, where "free space" has no local meaning.
var ErrSpaceUnknown = errors.New("backend: free space is not reportable")

// ErrNoSpace is the sentinel every "the medium cannot take this write" failure carries: a
// pre-flight check that found the backend short of room or of inodes, and the ENOSPC an actual
// write returned. A caller matches it with [errors.Is] to tell an exhausted node from a transient
// backend fault — the two need opposite responses, and they are otherwise indistinguishable.
var ErrNoSpace = errors.New("backend: out of space")

// SpaceReporter is the optional capability of reporting how much room a backend has left, so the
// merge engine can size its output parts against the disk they land on. A backend over a bounded
// local medium implements it; [Memory] and object stores do not.
//
// A wrapper around a [Backend] must forward it, or the capability is silently lost.
type SpaceReporter interface {
	FreeSpace(ctx context.Context) (int64, error)
}

// InodeReporter is the optional capability of reporting how many objects a backend can still
// create, whatever their size. It is a separate axis from [SpaceReporter], not a refinement of it:
// a part is many small objects, so a filesystem with terabytes free can still fail every create
// once its inode table is exhausted, and the two failures are identical from a byte-accounting
// point of view.
//
// A wrapper around a [Backend] must forward it, or the capability is silently lost.
type InodeReporter interface {
	FreeInodes(ctx context.Context) (int64, error)
}

// FreeSpace reports the bytes b has available, or an error wrapping [ErrSpaceUnknown] if b does not
// implement [SpaceReporter]. Treat that error as "unbounded", not as a failure.
func FreeSpace(ctx context.Context, b Backend) (int64, error) {
	r, ok := b.(SpaceReporter)
	if !ok {
		return 0, ErrSpaceUnknown
	}

	return r.FreeSpace(ctx)
}

// FreeInodes reports how many more objects b can create, or an error wrapping [ErrSpaceUnknown] if
// b does not implement [InodeReporter] — which includes a filesystem that allocates inodes
// dynamically and so has no count to give. Treat that error as "unbounded", not as a failure.
func FreeInodes(ctx context.Context, b Backend) (int64, error) {
	r, ok := b.(InodeReporter)
	if !ok {
		return 0, ErrSpaceUnknown
	}

	return r.FreeInodes(ctx)
}
