// Package vfs is the filesystem seam the durable local paths write through: the file backend's
// object store and the WAL's segments.
//
// It exists for two reasons. Durability is a property of the *order* of syscalls — data reaches the
// disk on fsync, a name reaches it only when its directory is synced — and no seam above this one
// can express that: a backend sees whole objects appear atomically, so a fault injected there
// cannot model a file whose bytes survived a power cut while the directory entry naming it did not.
// The second reason follows from the first: directory syncing needs one place to live, so
// [FS.SyncDir] is part of the contract rather than something each caller remembers.
//
// The interface is rooted, like [os.Root]: every name is relative to the directory the FS was
// opened on, and escaping it is refused. That keeps the production implementation a thin wrapper
// and matches how both consumers already work.
package vfs

import (
	"io"
	"io/fs"
)

// FS is the filesystem operations the durable local paths need — deliberately no more than that, so
// a fake has a small surface to be honest about.
type FS interface {
	// OpenFile opens name with the given flags, creating it when the flags say so. Reading is
	// OpenFile(name, os.O_RDONLY, 0).
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	// ReadFile returns name's contents.
	ReadFile(name string) ([]byte, error)
	// ReadDir lists the directory name, sorted by filename.
	ReadDir(name string) ([]fs.DirEntry, error)
	// Stat returns name's metadata.
	Stat(name string) (fs.FileInfo, error)
	// MkdirAll creates name and any missing parents.
	MkdirAll(name string, perm fs.FileMode) error
	// Rename atomically replaces newname with oldname. The new name is not durable until the
	// containing directory is synced ([FS.SyncDir]).
	Rename(oldname, newname string) error
	// Link creates newname as a hard link to oldname, failing when newname exists. As with Rename,
	// the new name is not durable until its directory is synced.
	Link(oldname, newname string) error
	// Remove deletes name.
	Remove(name string) error
	// SyncDir makes the directory name's own entries durable — the creations, renames, links and
	// removals performed in it. Syncing a *file* commits its bytes and says nothing about the name
	// that reaches them, so a publish sequence ending at Rename is not durable until this is called
	// on the destination's directory.
	//
	// It is a no-op where the platform gives no directory handle to sync (Windows, where the
	// atomic-replace primitive carries its own ordering).
	SyncDir(name string) error
	// Close releases the FS's own handle on the root. It does not close files opened through it.
	Close() error
}

// File is an open file. A write reaches the disk only on [File.Sync]; until then a power cut takes
// it, which is what makes Sync's placement testable.
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Closer

	// Sync commits the file's written bytes to stable storage. It does not commit the directory
	// entry naming the file — see [FS.SyncDir].
	Sync() error
}
