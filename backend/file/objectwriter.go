package file

import (
	"bufio"
	"context"
	"path"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs"
)

var _ backend.ObjectCreator = (*File)(nil)

// objectWriteBufferBytes is the userspace buffer between the caller's appends and the write(2)
// syscalls. A streamed column arrives one compression frame at a time (tens of KiB), so this only
// coalesces the small directory/trailer appends that follow them.
const objectWriteBufferBytes = 64 << 10

// CreateObject builds key's object incrementally in the temp file that [File.Write] uses for its
// whole-object write, renaming it over the final path on commit. Nothing is visible under key until
// then, so the atomicity contract is the same one; the difference is only that the bytes reach the
// filesystem as they are produced rather than all at once. Implements [backend.ObjectCreator].
func (f *File) CreateObject(_ context.Context, key string) (backend.ObjectWriter, error) {
	p, err := f.rel(key)
	if err != nil {
		return nil, err
	}

	root, err := f.openRoot()
	if err != nil {
		return nil, err
	}

	tmp, name, created, err := createTemp(root, path.Dir(p))
	if err != nil {
		_ = root.Close()

		return nil, err
	}

	return &objectWriter{
		root:    root,
		key:     key,
		path:    p,
		tmp:     tmp,
		name:    name,
		created: created,
		buf:     bufio.NewWriterSize(tmp, objectWriteBufferBytes),
	}, nil
}

// objectWriter streams one object into a temp file, publishing it with a rename. Both names are
// relative to root, which resolves them the same way every other path in this package is resolved.
// The root handle stays open for the writer's lifetime — the temp file it holds is only published
// at commit — and is released by [objectWriter.Commit] or [objectWriter.Abort].
type objectWriter struct {
	root    vfs.FS
	key     string
	path    string
	tmp     vfs.File
	name    string
	created []string
	buf     *bufio.Writer
}

func (w *objectWriter) Write(p []byte) (int, error) {
	if w.tmp == nil {
		return 0, errors.New("backend/file: write after commit")
	}

	n, err := w.buf.Write(p)
	if err != nil {
		return n, errors.Wrap(err, "write temp")
	}

	return n, nil
}

func (w *objectWriter) Commit(_ context.Context) error {
	if w.tmp == nil {
		return errors.New("backend/file: commit after commit")
	}

	// Past this point the temp file is either renamed or removed; either way the handle is done.
	tmp, name := w.tmp, w.name
	w.tmp = nil

	if err := w.buf.Flush(); err != nil {
		_ = tmp.Close()
		_ = w.root.Remove(name)
		_ = w.root.Close()

		return errors.Wrap(err, "flush temp")
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = w.root.Remove(name)
		_ = w.root.Close()

		return errors.Wrap(err, "sync temp")
	}

	if err := tmp.Close(); err != nil {
		_ = w.root.Remove(name)
		_ = w.root.Close()

		return errors.Wrap(err, "close temp")
	}

	if err := w.root.Rename(name, w.path); err != nil {
		_ = w.root.Remove(name)
		_ = w.root.Close()

		return errors.Wrapf(err, "rename into %q", w.key)
	}

	if err := publish(w.root, w.created, path.Dir(w.path)); err != nil {
		_ = w.root.Close()

		return errors.Wrapf(err, "publish %q", w.key)
	}

	if err := w.root.Close(); err != nil {
		return errors.Wrap(err, "close root")
	}

	return nil
}

func (w *objectWriter) Abort() {
	if w.tmp == nil {
		return
	}

	name := w.name

	_ = w.tmp.Close()
	_ = w.root.Remove(name)
	_ = w.root.Close()

	w.tmp, w.buf = nil, nil
}
