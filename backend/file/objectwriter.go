package file

import (
	"bufio"
	"context"
	"os"
	"path/filepath"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
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

	tmp, name, err := f.createTemp(filepath.Dir(p))
	if err != nil {
		return nil, err
	}

	return &objectWriter{
		root: f.root,
		key:  key,
		path: p,
		tmp:  tmp,
		name: name,
		buf:  bufio.NewWriterSize(tmp, objectWriteBufferBytes),
	}, nil
}

// objectWriter streams one object into a temp file, publishing it with a rename. Both names are
// relative to root, which resolves them the same way every other path in this package is resolved.
type objectWriter struct {
	root *os.Root
	key  string
	path string
	tmp  *os.File
	name string
	buf  *bufio.Writer
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

		return errors.Wrap(err, "flush temp")
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = w.root.Remove(name)

		return errors.Wrap(err, "sync temp")
	}

	if err := tmp.Close(); err != nil {
		_ = w.root.Remove(name)

		return errors.Wrap(err, "close temp")
	}

	if err := w.root.Rename(name, w.path); err != nil {
		_ = w.root.Remove(name)

		return errors.Wrapf(err, "rename into %q", w.key)
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

	w.tmp, w.buf = nil, nil
}
