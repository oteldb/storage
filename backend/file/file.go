// Package file implements a [backend.Backend] over a local directory tree. Keys map to
// files under a root; writes are atomic (temp file + rename) so a reader never observes
// a partially written object — the property the "manifest written last" part commit
// relies on (DESIGN.md §8, _ref/docs/storage-engine.md §2).
package file

import (
	"context"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// File is a directory-backed [backend.Backend]. Keys are slash-delimited and map to
// paths under root. Safe for concurrent use (the filesystem serializes renames; reads
// and writes touch distinct temp files).
type File struct {
	root string
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

	// Directories left behind by a version without Delete-time pruning (or by a crash between
	// an object delete and its rmdir) make every List traverse dead subtrees forever. Sweep
	// them once at open; best-effort, an unreadable subtree is not a reason to fail.
	pruneEmpty(abs)

	return &File{root: abs}, nil
}

// IsEphemeral reports false: data persists on disk.
func (*File) IsEphemeral() bool { return false }

// Write stores data under key atomically: it writes a temp file in the destination
// directory, fsyncs it, and renames it over the final path.
func (f *File) Write(_ context.Context, key string, data []byte) (rerr error) {
	p, err := f.path(key)
	if err != nil {
		return err
	}

	tmp, err := createTemp(filepath.Dir(p))
	if err != nil {
		return err
	}

	tmpName := tmp.Name()
	// On any failure past this point, remove the temp file.
	defer func() {
		if rerr != nil {
			_ = os.Remove(tmpName)
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

	if err := os.Rename(tmpName, p); err != nil {
		return errors.Wrapf(err, "rename into %q", p)
	}

	return nil
}

// PutIfAbsent stores data under key only if it does not already exist, returning whether the
// write happened. It writes a temp file then hard-links it to the final path: os.Link fails
// with EEXIST if the destination exists, giving an atomic, exclusive create (the conditional
// commit primitive). A reader never sees a partial object — the link publishes a fully
// written file.
func (f *File) PutIfAbsent(_ context.Context, key string, data []byte) (written bool, rerr error) {
	p, err := f.path(key)
	if err != nil {
		return false, err
	}

	tmp, err := createTemp(filepath.Dir(p))
	if err != nil {
		return false, err
	}

	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // the link, if made, is the durable copy

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

	if err := os.Link(tmpName, p); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil // key already present
		}

		return false, errors.Wrapf(err, "link into %q", p)
	}

	return true, nil
}

// Read returns the value stored under key, or an [backend.ErrNotExist]-wrapping error.
func (f *File) Read(_ context.Context, key string) ([]byte, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(p) //nolint:gosec // p is validated by f.path to stay within root
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
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(p) //nolint:gosec // p is validated by f.path to stay within root
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

// Size returns the byte size of the object stored under key (os.Stat, no read), or an
// [backend.ErrNotExist]-wrapping error if absent. It implements [backend.Sizer].
func (f *File) Size(_ context.Context, key string) (int64, error) {
	p, err := f.path(key)
	if err != nil {
		return 0, err
	}

	fi, err := os.Stat(p)
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

	base, err := f.path(dirKey)
	if err != nil {
		return nil, err
	}

	var keys []string

	err = filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// A prefix whose directory does not exist lists empty, as on an object store.
			if p == base && errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll
			}

			return err
		}

		if d.IsDir() {
			if leaf != "" && p != base && filepath.Dir(p) == base && !strings.HasPrefix(d.Name(), leaf) {
				return fs.SkipDir
			}

			return nil
		}

		// Skip leftover temp files from interrupted writes.
		if strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}

		rel, err := filepath.Rel(f.root, p)
		if err != nil {
			return errors.Wrapf(err, "relativize %q", p)
		}

		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
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
	p, err := f.path(key)
	if err != nil {
		return err
	}

	if err := os.Remove(p); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return errors.Wrapf(backend.ErrNotExist, "delete %q", key)
		}

		return errors.Wrapf(err, "delete %q", key)
	}

	f.pruneParents(p)

	return nil
}

// pruneParents removes the directories left empty by deleting p, up to (but never including)
// the root. Without it a deleted part leaves its directories behind forever, and every List
// keeps paying for them: the traversal cost grows with parts ever created, not parts retained.
func (f *File) pruneParents(p string) {
	for dir := filepath.Dir(p); strings.HasPrefix(dir, f.root+string(os.PathSeparator)); dir = filepath.Dir(dir) {
		// Fails with ENOTEMPTY as soon as a directory still holds objects.
		if err := os.Remove(dir); err != nil {
			return
		}
	}
}

// pruneEmpty removes every empty directory under dir (not dir itself), reporting whether dir
// is empty afterwards. Errors are ignored: it is an optimization, not a correctness step.
func pruneEmpty(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}

	empty := true

	for _, e := range entries {
		if !e.IsDir() {
			empty = false

			continue
		}

		sub := filepath.Join(dir, e.Name())
		if pruneEmpty(sub) && os.Remove(sub) == nil {
			continue
		}

		empty = false
	}

	return empty
}

// createTemp creates a temp file in dir, creating dir first. It retries once when dir vanishes
// between the two: a concurrent [File.Delete] prunes directories its last object leaves empty.
func createTemp(dir string) (*os.File, error) {
	for attempt := 0; ; attempt++ {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, errors.Wrapf(err, "mkdir %q", dir)
		}

		tmp, err := os.CreateTemp(dir, ".tmp-*")
		if err == nil {
			return tmp, nil
		}

		if attempt > 0 || !errors.Is(err, fs.ErrNotExist) {
			return nil, errors.Wrap(err, "create temp")
		}
	}
}

// path maps a slash-delimited key to an absolute filesystem path under root, rejecting
// any key that would escape root (e.g. via "..").
func (f *File) path(key string) (string, error) {
	p := filepath.Join(f.root, filepath.FromSlash(key))
	// filepath.Join cleans the result; ensure it is still under root.
	if p != f.root && !strings.HasPrefix(p, f.root+string(os.PathSeparator)) {
		return "", errors.Errorf("backend/file: key %q escapes root", key)
	}

	return p, nil
}
