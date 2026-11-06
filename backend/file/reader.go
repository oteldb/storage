package file

import (
	"context"
	"io"
	"io/fs"
	"math"
	"os"
	"slices"
	"sync/atomic"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs"
)

var (
	_ backend.ReadAppender = (*File)(nil)
	_ backend.ObjectOpener = (*File)(nil)
)

// ReadAtAppend appends a clamped range directly into caller-owned storage.
func (f *File) ReadAtAppend(_ context.Context, dst []byte, key string, off, n int64) ([]byte, error) {
	if off < 0 || n < 0 {
		return dst, errors.New("file: negative read range")
	}
	root, file, size, err := f.openRead(key)
	if err != nil {
		return dst, err
	}
	defer func() { _ = root.Close() }()
	defer func() { _ = file.Close() }()
	if off >= size {
		return dst, nil
	}
	n = min(n, size-off)
	if n > int64(math.MaxInt-len(dst)) {
		return dst, errors.New("file: read range exceeds slice size")
	}
	start := len(dst)
	dst = slices.Grow(dst, int(n))[:start+int(n)]
	if _, err := file.ReadAt(dst[start:], off); err != nil {
		return dst[:start], errors.Wrapf(err, "read %q at [%d,+%d)", key, off, n)
	}
	return dst, nil
}

func (f *File) openRead(key string) (vfs.FS, vfs.File, int64, error) {
	p, err := f.rel(key)
	if err != nil {
		return nil, nil, 0, err
	}
	root, err := f.openRoot()
	if err != nil {
		return nil, nil, 0, err
	}
	file, err := root.OpenFile(p, os.O_RDONLY, 0)
	if err != nil {
		_ = root.Close()
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, 0, errors.Wrapf(backend.ErrNotExist, "read %q", key)
		}
		return nil, nil, 0, errors.Wrapf(err, "open %q", key)
	}
	// Size and bytes belong to the same opened generation, including after replacement.
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		_ = root.Close()
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, 0, errors.Wrapf(backend.ErrNotExist, "read %q", key)
		}
		return nil, nil, 0, errors.Wrapf(err, "stat %q", key)
	}
	return root, file, info.Size(), nil
}

// OpenReader pins one opened object until Close; replacement never changes its bytes or size.
func (f *File) OpenReader(ctx context.Context, key string) (backend.ObjectReader, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	root, file, size, err := f.openRead(key)
	if err != nil {
		return nil, true, err
	}
	if err := root.Close(); err != nil {
		_ = file.Close()
		return nil, true, errors.Wrap(err, "close reader root")
	}
	return &objectReader{file: file, size: size}, true, nil
}

// ReaderMemory reserves file state and owned path strings; the handle has no internal byte buffer.
func (f *File) ReaderMemory(key string) (int64, bool) {
	return 1024 + 4*int64(len(f.dir)+len(key)), true
}

type objectReader struct {
	file   vfs.File
	size   int64
	pos    int64
	closed atomic.Bool
}

func (r *objectReader) Size() int64 { return r.size }

func (r *objectReader) ReadAt(dst []byte, off int64) (int, error) {
	if r.closed.Load() {
		return 0, fs.ErrClosed
	}
	return r.file.ReadAt(dst, off)
}

func (r *objectReader) Read(dst []byte) (int, error) {
	n, err := r.ReadAt(dst, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *objectReader) Seek(off int64, whence int) (int64, error) {
	if r.closed.Load() {
		return 0, fs.ErrClosed
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = r.pos
	case io.SeekEnd:
		base = r.size
	default:
		return 0, errors.New("file: invalid seek whence")
	}
	if off < -base || off > math.MaxInt64-base {
		return 0, errors.New("file: invalid seek offset")
	}
	r.pos = base + off
	return r.pos, nil
}

func (r *objectReader) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	return r.file.Close()
}
