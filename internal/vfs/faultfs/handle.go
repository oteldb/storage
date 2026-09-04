package faultfs

import (
	"io"
	"io/fs"
	"slices"
	"time"
)

// handle is an open file. Writes land in the node's live bytes and reach its synced bytes — the
// ones a crash keeps — only on Sync.
type handle struct {
	fs    *FS
	name  string
	node  *node
	write bool
	off   int64
}

// Write implements [io.Writer].
func (h *handle) Write(p []byte) (int, error) {
	if err := h.fs.enter(Call{Op: OpWrite, Name: h.name}); err != nil {
		return 0, err
	}

	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()

	h.node.data = append(h.node.data, p...)
	h.node.mod = time.Now()

	return len(p), nil
}

// Read implements [io.Reader].
func (h *handle) Read(p []byte) (int, error) {
	if err := h.fs.enter(Call{Op: OpRead, Name: h.name}); err != nil {
		return 0, err
	}

	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()

	if h.off >= int64(len(h.node.data)) {
		return 0, io.EOF
	}

	n := copy(p, h.node.data[h.off:])
	h.off += int64(n)

	return n, nil
}

// ReadAt implements [io.ReaderAt].
func (h *handle) ReadAt(p []byte, off int64) (int, error) {
	if err := h.fs.enter(Call{Op: OpRead, Name: h.name}); err != nil {
		return 0, err
	}

	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()

	if off < 0 || off > int64(len(h.node.data)) {
		return 0, &fs.PathError{Op: "readat", Path: h.name, Err: fs.ErrInvalid}
	}

	n := copy(p, h.node.data[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

// Sync implements [vfs.File]: it commits the bytes written so far. The directory entry naming them
// is a separate promise — see [FS.SyncDir].
func (h *handle) Sync() error {
	if err := h.fs.enter(Call{Op: OpSync, Name: h.name}); err != nil {
		return err
	}

	h.fs.mu.Lock()
	defer h.fs.mu.Unlock()

	h.node.synced = slices.Clone(h.node.data)

	// Once the name is durable, committing the bytes is immediately visible to a crash; before that
	// the bytes wait for the directory sync that publishes the name.
	if _, ok := h.fs.durable[h.name]; ok {
		h.fs.durable[h.name] = slices.Clone(h.node.data)
	}

	return nil
}

// Close implements [io.Closer]. Closing does not commit anything: an unsynced write is still lost
// by a crash, which is the mistake this fake exists to catch.
func (h *handle) Close() error { return nil }

// dirEntry satisfies [fs.DirEntry] and [fs.FileInfo] for the fake's listings.
type dirEntry struct {
	name string
	size int64
	mode fs.FileMode
	mod  time.Time
	dir  bool
}

func (e *dirEntry) Name() string               { return e.name }
func (e *dirEntry) Size() int64                { return e.size }
func (e *dirEntry) Mode() fs.FileMode          { return e.mode }
func (e *dirEntry) Type() fs.FileMode          { return e.mode.Type() }
func (e *dirEntry) ModTime() time.Time         { return e.mod }
func (e *dirEntry) IsDir() bool                { return e.dir }
func (e *dirEntry) Sys() any                   { return nil }
func (e *dirEntry) Info() (fs.FileInfo, error) { return e, nil }
