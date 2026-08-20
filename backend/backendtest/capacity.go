package backendtest

import (
	"context"
	"sync/atomic"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// Unknown is the free-bytes/free-inodes value that makes a [Capacity] report
// [backend.ErrSpaceUnknown] for that axis, modeling a backend without the capability.
const Unknown = int64(-1)

// Capacity wraps a backend with a settable capacity report, so a test can put an engine on a full
// disk or an exhausted inode table without needing a real one. The two axes are set independently:
// a filesystem with terabytes free can still fail every create, and a test must be able to say so.
//
// Like [github.com/oteldb/storage/backend/faultbackend] it forwards no other optional capability —
// each has a mandatory fallback, so a wrapped backend runs the same code, only slower.
type Capacity struct {
	backend.Backend

	freeBytes  atomic.Int64
	freeInodes atomic.Int64
}

var (
	_ backend.SpaceReporter = (*Capacity)(nil)
	_ backend.InodeReporter = (*Capacity)(nil)
)

// WithCapacity wraps b reporting freeBytes bytes and freeInodes inodes available ([Unknown] for an
// axis the backend cannot report).
func WithCapacity(b backend.Backend, freeBytes, freeInodes int64) *Capacity {
	c := &Capacity{Backend: b}
	c.freeBytes.Store(freeBytes)
	c.freeInodes.Store(freeInodes)

	return c
}

// SetFreeBytes changes the reported free bytes — the disk filling up, or being emptied.
func (c *Capacity) SetFreeBytes(n int64) { c.freeBytes.Store(n) }

// SetFreeInodes changes the reported free inodes.
func (c *Capacity) SetFreeInodes(n int64) { c.freeInodes.Store(n) }

// FreeSpace implements [backend.SpaceReporter].
func (c *Capacity) FreeSpace(context.Context) (int64, error) {
	return report(c.freeBytes.Load(), "free space")
}

// FreeInodes implements [backend.InodeReporter].
func (c *Capacity) FreeInodes(context.Context) (int64, error) {
	return report(c.freeInodes.Load(), "free inodes")
}

func report(n int64, what string) (int64, error) {
	if n == Unknown {
		return 0, errors.Wrapf(backend.ErrSpaceUnknown, "%s is not reported", what)
	}

	return n, nil
}
