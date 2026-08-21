// Package file implements a [backend.Backend] over a local directory tree. Keys map to
// files under a root; writes are atomic (temp file + rename) so a reader never observes
// a partially written object — the property the "manifest written last" part commit
// relies on (DESIGN.md §8, _ref/docs/storage-engine.md §2).
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
)

// File is a directory-backed [backend.Backend]. Keys are slash-delimited and map to
// paths under root. Safe for concurrent use (the filesystem serializes renames; reads
// and writes touch distinct temp files).
//
// Every path operation goes through an [os.Root] opened for that operation alone. That buys two
// things over joining strings onto a root path. Containment stops being a check this package has
// to get right — a symlink inside the tree pointing out of it is refused, not merely a "..". And
// on Windows the file handles it opens carry FILE_SHARE_DELETE, without which a concurrent reader
// blocks the rename that publishes an object: plain [os.Open] there asks for
// FILE_SHARE_READ|FILE_SHARE_WRITE only, and MoveFileEx over a destination someone holds open
// fails with ERROR_ACCESS_DENIED. Rename-over-an-open-file is a POSIX guarantee the write path
// depends on; os.Root is what extends it to Windows.
//
// The handle is per-operation rather than held on the File because [os.OpenRoot] opens its own
// directory handle through the same [syscall.Open] that omits FILE_SHARE_DELETE
// (os/root_windows.go). A root kept for the backend's lifetime would therefore pin its data
// directory against removal on Windows, and [backend.Backend] has no Close with which to release
// it. The cost is one extra open and close per operation.
type File struct {
	dir string
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

	f := &File{dir: abs}

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

// IsEphemeral reports false: data persists on disk.
func (*File) IsEphemeral() bool { return false }

// IsNodeLocal reports true: a directory tree is private to its node unless the root happens to be a
// shared mount, which the backend cannot tell. See [backend.NodeLocal] for how to read that.
func (*File) IsNodeLocal() bool { return true }

// Write stores data under key atomically: it writes a temp file in the destination
// directory, fsyncs it, and renames it over the final path.
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

	tmp, tmpName, err := createTemp(root, filepath.Dir(p))
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

	tmp, tmpName, err := createTemp(root, filepath.Dir(p))
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

	file, err := root.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errors.Wrapf(backend.ErrNotExist, "read %q", key)
		}

		return nil, errors.Wrapf(err, "open %q", key)
	}
	defer func() { _ = file.Close() }()

	// Sized against the file so a clamped request allocates what it can actually return, not what it
	// asked for: callers take an object's trailer by asking for more than may be there.
	fi, err := file.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "stat %q", key)
	}

	if off >= fi.Size() {
		return []byte{}, nil
	}

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

	rel, err := f.rel(dirKey)
	if err != nil {
		return nil, err
	}

	// [os.Root.FS] walks in slash paths relative to the root, so an entry's own path is already
	// the key — no relativizing against an absolute root.
	root, err := f.openRoot()
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()

	base := filepath.ToSlash(rel)

	var keys []string

	err = fs.WalkDir(root.FS(), base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// A prefix whose directory does not exist lists empty, as on an object store.
			if p == base && errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll
			}

			return err
		}

		if d.IsDir() {
			if leaf != "" && p != base && path.Dir(p) == base && !strings.HasPrefix(d.Name(), leaf) {
				return fs.SkipDir
			}

			return nil
		}

		// Skip leftover temp files from interrupted writes.
		if strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}

		if strings.HasPrefix(p, prefix) {
			keys = append(keys, p)
		}

		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "walk")
	}

	slices.Sort(keys)

	return keys, nil
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

	pruneParents(root, p)

	return nil
}

// openRoot opens a root handle for one operation. The caller closes it.
func (f *File) openRoot() (*os.Root, error) {
	root, err := os.OpenRoot(f.dir)
	if err != nil {
		return nil, errors.Wrapf(err, "open root %q", f.dir)
	}

	return root, nil
}

// pruneParents removes the directories left empty by deleting p, up to (but never including)
// the root. Without it a deleted part leaves its directories behind forever, and every List
// keeps paying for them: the traversal cost grows with parts ever created, not parts retained.
func pruneParents(root *os.Root, p string) {
	for dir := path.Dir(filepath.ToSlash(p)); dir != "." && dir != "/"; dir = path.Dir(dir) {
		// Fails with ENOTEMPTY as soon as a directory still holds objects.
		if err := root.Remove(filepath.FromSlash(dir)); err != nil {
			return
		}
	}
}

// pruneEmpty removes every empty directory under dir (not dir itself), reporting whether dir
// is empty afterwards. Errors are ignored: it is an optimization, not a correctness step.
func (f *File) pruneEmpty(root *os.Root, dir string) bool {
	entries, err := fs.ReadDir(root.FS(), dir)
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
		if f.pruneEmpty(root, sub) && root.Remove(filepath.FromSlash(sub)) == nil {
			continue
		}

		empty = false
	}

	return empty
}

// createTemp creates a temp file in dir (relative to the root), creating dir first, and returns
// it with its root-relative name. It retries once when dir vanishes between the two: a
// concurrent [File.Delete] prunes directories its last object leaves empty.
//
// [os.Root] has no CreateTemp, so the exclusive-create loop is here: O_EXCL makes the name
// claim atomic, and a collision just draws another.
func createTemp(root *os.Root, dir string) (*os.File, string, error) {
	for attempt := 0; ; attempt++ {
		if err := root.MkdirAll(dir, 0o750); err != nil {
			return nil, "", errors.Wrapf(err, "mkdir %q", dir)
		}

		for range 10 {
			name := filepath.Join(dir, ".tmp-"+strconv.FormatUint(rand.Uint64(), 36)) //nolint:gosec // temp file names need no unpredictability

			tmp, err := root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
			if err == nil {
				return tmp, name, nil
			}

			if errors.Is(err, fs.ErrExist) {
				continue
			}

			// The directory was pruned under us; remake it and try again, once.
			if attempt == 0 && errors.Is(err, fs.ErrNotExist) {
				break
			}

			return nil, "", errors.Wrap(err, "create temp")
		}

		if attempt > 0 {
			return nil, "", errors.Errorf("create temp in %q: no free name", dir)
		}
	}
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

	return filepath.FromSlash(p), nil
}
