package backend

import (
	"context"

	"github.com/go-faster/errors"
)

// ErrSpaceUnknown is returned by [FreeSpace] for a backend that cannot report available capacity —
// an ephemeral or object-store backend, where "free space" has no local meaning.
var ErrSpaceUnknown = errors.New("backend: free space is not reportable")

// SpaceReporter is the optional capability of reporting how much room a backend has left, so the
// merge engine can size its output parts against the disk they land on. A backend over a bounded
// local medium implements it; [Memory] and object stores do not.
//
// A wrapper around a [Backend] must forward it, or the capability is silently lost.
type SpaceReporter interface {
	FreeSpace(ctx context.Context) (int64, error)
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
