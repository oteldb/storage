//go:build !unix

package file

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

var _ backend.SpaceReporter = (*File)(nil)

// FreeSpace is unsupported without a portable statfs, so the caller falls back to its ceiling.
func (f *File) FreeSpace(context.Context) (int64, error) {
	return 0, errors.Wrapf(backend.ErrSpaceUnknown, "free space on %s", "this platform")
}
