package vfs

import (
	"io/fs"
	"os"
	"slices"
	"strings"

	"github.com/go-faster/errors"
)

// osFS is [FS] over a real directory, through [os.Root] so a name cannot escape it — the same
// containment the file backend already relied on.
type osFS struct{ root *os.Root }

// OpenRoot opens dir (creating it and its parents when missing) as a rooted [FS].
func OpenRoot(dir string, perm fs.FileMode) (FS, error) {
	if err := os.MkdirAll(dir, perm); err != nil {
		return nil, errors.Wrapf(err, "create dir %q", dir)
	}

	return Open(dir)
}

// Open opens an existing dir as a rooted [FS]. Unlike [OpenRoot] it creates nothing, so a caller
// that opens one handle per operation does not pay a mkdir syscall on every one of them.
func Open(dir string) (FS, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "open root %q", dir)
	}

	return osFS{root: root}, nil
}

func (f osFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	return wrapOpen(f.root.OpenFile(name, flag, perm))
}

func (f osFS) ReadFile(name string) ([]byte, error) { return f.root.ReadFile(name) }

func (f osFS) ReadDir(name string) ([]fs.DirEntry, error) {
	d, err := f.root.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = d.Close() }()

	// Sorted here because the contract says so and os.File.ReadDir does not — only the package-level
	// os.ReadDir does, and that one takes a path outside the root.
	entries, err := d.ReadDir(-1)
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })

	return entries, err
}

func (f osFS) Stat(name string) (fs.FileInfo, error) { return f.root.Stat(name) }

func (f osFS) MkdirAll(name string, perm fs.FileMode) error { return f.root.MkdirAll(name, perm) }

func (f osFS) Rename(oldname, newname string) error { return f.root.Rename(oldname, newname) }

func (f osFS) Link(oldname, newname string) error { return f.root.Link(oldname, newname) }

func (f osFS) Remove(name string) error { return f.root.Remove(name) }

func (f osFS) SyncDir(name string) error { return syncDir(f.root, name) }

func (f osFS) Close() error { return f.root.Close() }

func wrapOpen(f *os.File, err error) (File, error) {
	if err != nil {
		return nil, err
	}

	return f, nil
}
