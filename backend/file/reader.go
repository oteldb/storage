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
	"github.com/oteldb/storage/internal/readerpool"
	"github.com/oteldb/storage/internal/vfs"
)

var (
	_ backend.ReadAppender = (*File)(nil)
	_ backend.ObjectOpener = (*File)(nil)
)

// ReadAtAppend appends a clamped range directly into caller-owned storage.
func (f *File) ReadAtAppend(ctx context.Context, dst []byte, key string, off, n int64) ([]byte, error) {
	if off < 0 || n < 0 {
		return dst, errors.New("file: negative read range")
	}
	if readerpool.Scoped(ctx) {
		if out, supported, err := f.readRetained(ctx, dst, key, off, n); supported || err != nil {
			return out, err
		}
	}

	if !readerpool.Valid(ctx) {
		return dst, errors.New("file: object changed before read")
	}
	root, file, size, err := f.openRead(key)
	if err != nil {
		return dst, err
	}
	defer func() { _ = root.Close() }()
	defer func() { _ = file.Close() }()
	if !readerpool.Valid(ctx) {
		return dst, errors.New("file: object changed while opening read")
	}
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
	if !readerpool.Valid(ctx) {
		return dst[:start], errors.New("file: object changed during read")
	}
	return dst, nil
}

func (f *File) openRead(key string) (vfs.FS, vfs.File, int64, error) {
	root, file, size, err := f.openReadOnce(key)
	if readerpool.RelievePressure(err) {
		return f.openReadOnce(key)
	}
	return root, file, size, err
}

func (f *File) openReadOnce(key string) (vfs.FS, vfs.File, int64, error) {
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
	name, err := f.rel(key)
	if err != nil {
		return nil, true, err
	}
	return readerpool.Open(ctx, f.readerNamespace(), name, func() (readerpool.Reader, error) { return f.openReaderObject(key) })
}

// RetainsImmutableReaders reports scoped file-generation support.
func (*File) RetainsImmutableReaders() bool { return true }

// ReaderMemory reserves file state and owned path strings; the handle has no internal byte buffer.
func (f *File) ReaderMemory(key string) (int64, bool) {
	return 3072 + 4*int64(len(f.dir)+len(key)), true
}

type objectReader struct {
	file   vfs.File
	size   int64
	pos    int64
	closed atomic.Bool
	memory int64
}

func (r *objectReader) Size() int64 { return r.size }

func (r *objectReader) MemoryBytes() int64 { return r.memory }

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

func (f *File) readRetained(ctx context.Context, dst []byte, key string, off, n int64) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return dst, true, err
	}
	name, err := f.rel(key)
	if err != nil {
		return dst, true, err
	}
	return readerpool.Append(ctx, f.readerNamespace(), name, dst, off, n, func() (readerpool.Reader, error) { return f.openReaderObject(key) })
}

func (f *File) openReaderObject(key string) (readerpool.Reader, error) {
	root, file, size, err := f.openReadOnce(key)
	if err != nil {
		return nil, err
	}
	if err := root.Close(); err != nil {
		_ = file.Close()
		return nil, errors.Wrap(err, "close reader root")
	}
	memory, _ := f.ReaderMemory(key)
	return &objectReader{file: file, size: size, memory: memory}, nil
}

func (f *File) readerNamespace() any {
	if f.dirs != nil {
		return f.dirs
	}
	return f
}
