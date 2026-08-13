//go:build unix

package file

import (
	"context"
	"syscall"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

var _ backend.SpaceReporter = (*File)(nil)

// FreeSpace reports the bytes available to an unprivileged writer on the filesystem holding the
// root directory. It uses the unprivileged figure (not the total free blocks), so the reserved
// root allowance is never counted as usable.
func (f *File) FreeSpace(context.Context) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(f.root, &st); err != nil {
		return 0, errors.Wrapf(err, "statfs %q", f.root)
	}

	//nolint:unconvert // Statfs_t field types differ by platform (Bsize is int64 on Linux, uint32 on
	// Darwin), so one conversion looks redundant on whichever platform is linting and is required on
	// the other.
	return int64(st.Bavail) * int64(st.Bsize), nil
}
