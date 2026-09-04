// Package file implements a [backend.Backend] over a local directory tree. Keys map to
// files under a root; writes are atomic (temp file + rename) and durable (the temp file is
// fsynced, then the directory holding the published name is), so a reader never observes a
// partially written object and a power cut never takes a name a write already reported as
// stored — the property the "manifest written last" part commit relies on (DESIGN.md §8,
// _ref/docs/storage-engine.md §2).
package file

import (
	"context"
	"io/fs"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs"
)

// File is a directory-backed [backend.Backend]. Keys are slash-delimited and map to
// paths under root. Safe for concurrent use (the filesystem serializes renames; reads
// and writes touch distinct temp files).
//
// Every path operation goes through a [vfs.FS] opened for that operation alone — an [os.Root] in
// production. That buys two things over joining strings onto a root path. Containment stops being
// a check this package has to get right — a symlink inside the tree pointing out of it is refused,
// not merely a "..". And on Windows the file handles it opens carry FILE_SHARE_DELETE, without
// which a concurrent reader blocks the rename that publishes an object: plain [os.Open] there asks
// for FILE_SHARE_READ|FILE_SHARE_WRITE only, and MoveFileEx over a destination someone holds open
// fails with ERROR_ACCESS_DENIED. Rename-over-an-open-file is a POSIX guarantee the write path
// depends on; os.Root is what extends it to Windows.
//
// The handle is per-operation rather than held on the File because [os.OpenRoot] opens its own
// directory handle through the same [syscall.Open] that omits FILE_SHARE_DELETE
// (os/root_windows.go). A root kept for the backend's lifetime would therefore pin its data
// directory against removal on Windows, and [backend.Backend] has no Close with which to release
// it. The cost is one extra open and close per operation.
type File struct {
	dir  string
	open func() (vfs.FS, error)
}

var _ backend.Backend = (*File)(nil)

// New returns a [File] backend rooted at dir, creating dir if it does not exist.
func New(dir string) (*File, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "resolve root %q", dir)
	}

	if err := os.MkdirAll(abs, 0o750); err != nil {
		return nil, errors.Wrapf(err, "create root %q", abs)
	}

	f := &File{dir: abs, open: func() (vfs.FS, error) { return vfs.Open(abs) }}

	// Fail at construction rather than at the first operation if the tree cannot be opened.
	root, err := f.openRoot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()

	// Directories left behind by a version without Delete-time pruning (or by a crash between
	// an object delete and its rmdir) make every List traverse dead subtrees forever. Sweep
	// them once at open; best-effort, an unreadable subtree is not a reason to fail.
	f.pruneEmpty(root, ".")

	return f, nil
}

// newFS returns a backend over fsys, sharing the one filesystem across operations. It is the test
// seam for durability: only a fake filesystem can present the state a power cut leaves behind, and
// that state is not reachable through [New].
func newFS(fsys vfs.FS) *File {
	return &File{open: func() (vfs.FS, error) { return nopCloseFS{fsys}, nil }}
}

// nopCloseFS keeps a shared filesystem alive across the per-operation Close every path here does.
type nopCloseFS struct{ vfs.FS }

func (nopCloseFS) Close() error { return nil }

// IsEphemeral reports false: data persists on disk.
func (*File) IsEphemeral() bool { return false }

// IsNodeLocal reports true: a directory tree is private to its node unless the root happens to be a
// shared mount, which the backend cannot tell. See [backend.NodeLocal] for how to read that.
func (*File) IsNodeLocal() bool { return true }

// Write stores data under key atomically and durably: it writes a temp file in the destination
// directory, fsyncs it, renames it over the final path, and fsyncs the directories the new name
// hangs from.
func (f *File) Write(_ context.Context, key string, data []byte) (rerr error) {
	p, err := f.rel(key)
	if err != nil {
		return err
	}

	root, err := f.openRoot()
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()

	tmp, tmpName, created, err := createTemp(root, path.Dir(p))
	if err != nil {
		return err
	}

	// On any failure past this point, remove the temp file.
	defer func() {
		if rerr != nil {
			_ = root.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()

		return errors.Wrap(err, "write temp")
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()

		return errors.Wrap(err, "sync temp")
	}

	if err := tmp.Close(); err != nil {
		return errors.Wrap(err, "close temp")
	}

	if err := root.Rename(tmpName, p); err != nil {
		return errors.Wrapf(err, "rename into %q", key)
	}

	if err := publish(root, created, path.Dir(p)); err != nil {
		return errors.Wrapf(err, "publish %q", key)
	}

	return nil
}

// PutIfAbsent stores data under key only if it does not already exist, returning whether the
// write happened. It writes a temp file then hard-links it to the final path: the link fails
// with EEXIST if the destination exists, giving an atomic, exclusive create (the conditional
// commit primitive). A reader never sees a partial object — the link publishes a fully
// written file.
func (f *File) PutIfAbsent(_ context.Context, key string, data []byte) (written bool, rerr error) {
	p, err := f.rel(key)
	if err != nil {
		return false, err
	}

	root, err := f.openRoot()
	if err != nil {
		return false, err
	}
	defer func() { _ = root.Close() }()

	tmp, tmpName, created, err := createTemp(root, path.Dir(p))
	if err != nil {
		return false, err
	}

	defer func() { _ = root.Remove(tmpName) }() // the link, if made, is the durable copy

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()

		return false, errors.Wrap(err, "write temp")
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()

		return false, errors.Wrap(err, "sync temp")
	}

	if err := tmp.Close(); err != nil {
		return false, errors.Wrap(err, "close temp")
	}

	if err := root.Link(tmpName, p); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil // key already present
		}

		return false, errors.Wrapf(err, "link into %q", key)
	}

	if err := publish(root, created, path.Dir(p)); err != nil {
		return false, errors.Wrapf(err, "publish %q", key)
	}

	return true, nil
}

// Read returns the value stored under key, or an [backend.ErrNotExist]-wrapping error.
func (f *File) Read(_ context.Context, key string) ([]byte, error) {
	p, err := f.rel(key)
	if err != nil {
		return nil, err
	}

	root, err := f.openRoot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()

	data, err := root.ReadFile(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errors.Wrapf(backend.ErrNotExist, "read %q", key)
		}

		return nil, errors.Wrapf(err, "read %q", key)
	}

	return data, nil
}

// ReadAt returns the object's [off, off+n) range, clamped to its end, with one pread — the file
// backend never maps a part column into memory to hand back a slice of it. Implements
// [backend.ReaderAt].
func (f *File) ReadAt(_ context.Context, key string, off, n int64) ([]byte, error) {
	p, err := f.rel(key)
	if err != nil {
		return nil, err
	}

	root, err := f.openRoot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()

	// Sized against the file so a clamped request allocates what it can actually return, not what it
	// asked for: callers take an object's trailer by asking for more than may be there.
	fi, err := root.Stat(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errors.Wrapf(backend.ErrNotExist, "read %q", key)
		}

		return nil, errors.Wrapf(err, "stat %q", key)
	}

	if off >= fi.Size() {
		return []byte{}, nil
	}

	file, err := root.OpenFile(p, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errors.Wrapf(backend.ErrNotExist, "read %q", key)
		}

		return nil, errors.Wrapf(err, "open %q", key)
	}
	defer func() { _ = file.Close() }()

	buf := make([]byte, min(n, fi.Size()-off))

	// ReadAt reports io.EOF only on a short read, which the size bound above already rules out.
	if _, err := file.ReadAt(buf, off); err != nil {
		return nil, errors.Wrapf(err, "read %q at [%d,+%d)", key, off, len(buf))
	}

	return buf, nil
}

// Size returns the byte size of the object stored under key (a stat, no read), or an
// [backend.ErrNotExist]-wrapping error if absent. It implements [backend.Sizer].
func (f *File) Size(_ context.Context, key string) (int64, error) {
	p, err := f.rel(key)
	if err != nil {
		return 0, err
	}

	root, err := f.openRoot()
	if err != nil {
		return 0, err
	}
	defer func() { _ = root.Close() }()

	fi, err := root.Stat(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, errors.Wrapf(backend.ErrNotExist, "size %q", key)
		}

		return 0, errors.Wrapf(err, "size %q", key)
	}

	return fi.Size(), nil
}

// List returns, sorted ascending, every key with the given prefix.
//
// The prefix bounds the work, not just the result: keys map to paths, so only the subtree
// under the prefix's directory component is traversed, and within it only the children whose
// name can still extend into the prefix's final (possibly partial) segment.
func (f *File) List(_ context.Context, prefix string) ([]string, error) {
	dirKey, leaf := path.Split(prefix)

	base, err := f.rel(dirKey)
	if err != nil {
		return nil, err
	}

	root, err := f.openRoot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()

	var keys []string
	if err := walk(root, base, 0, leaf, prefix, &keys); err != nil {
		return nil, errors.Wrap(err, "walk")
	}

	slices.Sort(keys)

	return keys, nil
}

// walk appends to keys every key under dir matching prefix. depth counts levels below the prefix's
// directory component, so the leaf filter — which only constrains the prefix's own last, possibly
// partial, segment — applies to that directory's immediate children and nothing deeper.
func walk(root vfs.FS, dir string, depth int, leaf, prefix string, keys *[]string) error {
	// A prefix whose directory does not exist lists empty, as on an object store.
	if depth == 0 && !isDir(root, dir) {
		return nil
	}

	entries, err := root.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		p := path.Join(dir, e.Name())

		if e.IsDir() {
			if depth == 0 && leaf != "" && !strings.HasPrefix(e.Name(), leaf) {
				continue
			}

			if err := walk(root, p, depth+1, leaf, prefix, keys); err != nil {
				return err
			}

			continue
		}

		// Skip leftover temp files from interrupted writes.
		if strings.HasPrefix(e.Name(), tmpPrefix) {
			continue
		}

		if strings.HasPrefix(p, prefix) {
			*keys = append(*keys, p)
		}
	}

	return nil
}

func isDir(root vfs.FS, name string) bool {
	fi, err := root.Stat(name)

	return err == nil && fi.IsDir()
}

// Delete removes key, or returns an [backend.ErrNotExist]-wrapping error if absent.
func (f *File) Delete(_ context.Context, key string) error {
	p, err := f.rel(key)
	if err != nil {
		return err
	}

	root, err := f.openRoot()
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()

	if err := root.Remove(p); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return errors.Wrapf(backend.ErrNotExist, "delete %q", key)
		}

		return errors.Wrapf(err, "delete %q", key)
	}

	if err := root.SyncDir(path.Dir(p)); err != nil {
		return errors.Wrapf(err, "sync dir of %q", key)
	}

	pruneParents(root, p)

	return nil
}

// openRoot opens a filesystem handle for one operation. The caller closes it.
func (f *File) openRoot() (vfs.FS, error) { return f.open() }

// pruneParents removes the directories left empty by deleting p, up to (but never including)
// the root. Without it a deleted part leaves its directories behind forever, and every List
// keeps paying for them: the traversal cost grows with parts ever created, not parts retained.
func pruneParents(root vfs.FS, p string) {
	for dir := path.Dir(p); dir != "." && dir != "/"; dir = path.Dir(dir) {
		// Fails with ENOTEMPTY as soon as a directory still holds objects.
		if err := root.Remove(dir); err != nil {
			return
		}

		if err := root.SyncDir(path.Dir(dir)); err != nil {
			return
		}
	}
}

// pruneEmpty removes every empty directory under dir (not dir itself), reporting whether dir
// is empty afterwards. Errors are ignored: it is an optimization, not a correctness step.
func (f *File) pruneEmpty(root vfs.FS, dir string) bool {
	entries, err := root.ReadDir(dir)
	if err != nil {
		return false
	}

	empty := true

	for _, e := range entries {
		if !e.IsDir() {
			empty = false

			continue
		}

		sub := path.Join(dir, e.Name())
		if f.pruneEmpty(root, sub) && root.Remove(sub) == nil {
			continue
		}

		empty = false
	}

	return empty
}

// tmpPrefix names the half-written files [List] must not report as objects.
const tmpPrefix = ".tmp-"

// createTemp creates a temp file in dir (relative to the root), creating dir first, and returns
// it with its root-relative name and the directories it had to create, outermost first — those
// are names their own parents must sync before the published object's name means anything. It
// retries once when dir vanishes between the two: a concurrent [File.Delete] prunes directories
// its last object leaves empty.
//
// [vfs.FS] has no CreateTemp, so the exclusive-create loop is here: O_EXCL makes the name
// claim atomic, and a collision just draws another.
func createTemp(root vfs.FS, dir string) (vfs.File, string, []string, error) {
	for attempt := 0; ; attempt++ {
		created := missingDirs(root, dir)

		if err := root.MkdirAll(dir, 0o750); err != nil {
			return nil, "", nil, errors.Wrapf(err, "mkdir %q", dir)
		}

		for range 10 {
			name := path.Join(dir, tmpPrefix+strconv.FormatUint(rand.Uint64(), 36)) //nolint:gosec // temp file names need no unpredictability

			tmp, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
			if err == nil {
				return tmp, name, created, nil
			}

			if errors.Is(err, fs.ErrExist) {
				continue
			}

			// The directory was pruned under us; remake it and try again, once.
			if attempt == 0 && errors.Is(err, fs.ErrNotExist) {
				break
			}

			return nil, "", nil, errors.Wrap(err, "create temp")
		}

		if attempt > 0 {
			return nil, "", nil, errors.Errorf("create temp in %q: no free name", dir)
		}
	}
}

// missingDirs returns the components of dir that do not exist yet, outermost first. A racing
// writer may create one of them in between, which only costs a redundant directory sync later.
func missingDirs(root vfs.FS, dir string) []string {
	var missing []string

	for d := dir; d != "." && d != "/"; d = path.Dir(d) {
		if _, err := root.Stat(d); err == nil {
			break
		}

		missing = append(missing, d)
	}

	slices.Reverse(missing)

	return missing
}

// publish makes a just-renamed or just-linked name durable: the directories created for it, from
// the outermost in (a child's entry only means something once its parent's entry is on the disk),
// then the directory the name itself lives in. Without this the object's bytes are on the disk
// with nothing naming them, and a power cut takes the write back.
func publish(root vfs.FS, created []string, dir string) error {
	for _, d := range created {
		if err := root.SyncDir(path.Dir(d)); err != nil {
			return errors.Wrapf(err, "sync dir %q", path.Dir(d))
		}
	}

	if err := root.SyncDir(dir); err != nil {
		return errors.Wrapf(err, "sync dir %q", dir)
	}

	return nil
}

// rel maps a slash-delimited key to a path relative to the root, rejecting any key that would
// escape it. [os.Root] refuses an escaping path in the kernel too; this check runs first so the
// caller is told which key was at fault rather than being handed a path error.
func (f *File) rel(key string) (string, error) {
	// Resolving against "/" collapses any leading "..", so a key that meant to climb out of the
	// root no longer matches its own cleaned form.
	p := strings.TrimPrefix(path.Clean("/"+key), "/")
	if p == "" {
		p = "."
	}

	if p != path.Clean("./"+key) {
		return "", errors.Errorf("backend/file: key %q escapes root", key)
	}

	return p, nil
}
