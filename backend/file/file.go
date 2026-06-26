// Package file implements a [backend.Backend] over a local directory tree. Keys map to
// files under a root; writes are atomic (temp file + rename) so a reader never observes
// a partially written object — the property the "manifest written last" part commit
// relies on (DESIGN.md §8, _ref/docs/storage-engine.md §2).
package file

import (
	"context"
	"io/fs"
	"os"
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

	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return errors.Wrapf(err, "mkdir %q", dir)
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return errors.Wrap(err, "create temp")
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

	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return false, errors.Wrapf(err, "mkdir %q", dir)
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return false, errors.Wrap(err, "create temp")
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

// List returns, sorted ascending, every key with the given prefix.
func (f *File) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string

	err := filepath.WalkDir(f.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		// Skip leftover temp files from interrupted writes.
		if strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}

		rel, err := filepath.Rel(f.root, path)
		if err != nil {
			return errors.Wrapf(err, "relativize %q", path)
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

	return nil
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
