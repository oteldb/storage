//go:build unix

package file

import (
	"context"
	"syscall"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

var (
	_ backend.SpaceReporter = (*File)(nil)
	_ backend.InodeReporter = (*File)(nil)
)

// FreeSpace reports the bytes available on the filesystem holding the root directory. It takes the
// unprivileged figure, so the reserved root allowance is never counted as usable.
func (f *File) FreeSpace(context.Context) (int64, error) {
	st, err := f.statfs()
	if err != nil {
		return 0, err
	}

	//nolint:unconvert // Statfs_t field types differ by platform (Bsize is int64 on Linux, uint32 on
	// Darwin), so one conversion looks redundant on whichever platform is linting and is required on
	// the other.
	return int64(st.Bavail) * int64(st.Bsize), nil
}

// FreeInodes reports how many more files can be created on the filesystem holding the root
// directory. A filesystem that allocates inodes dynamically (btrfs, and tmpfs on some kernels)
// reports a zero total; it has no ceiling to report, so that is [backend.ErrSpaceUnknown] rather
// than "none left" — the difference between an unbounded backend and a wedged one.
func (f *File) FreeInodes(context.Context) (int64, error) {
	st, err := f.statfs()
	if err != nil {
		return 0, err
	}

	if int64(st.Files) <= 0 {
		return 0, errors.Wrapf(backend.ErrSpaceUnknown, "inodes on %q are not accounted", f.dir)
	}

	return int64(st.Ffree), nil
}

func (f *File) statfs() (syscall.Statfs_t, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(f.dir, &st); err != nil {
		return st, errors.Wrapf(err, "statfs %q", f.dir)
	}

	return st, nil
}
