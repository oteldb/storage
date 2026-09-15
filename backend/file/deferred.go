package file

import (
	"context"
	"io/fs"
	"path"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/vfs"
)

var _ backend.DeferredSyncer = (*File)(nil)

// WriteDeferred is [File.Write] without the directory syncs: the bytes are fsynced, the name waits
// for [File.SyncPrefix]. Implements [backend.DeferredSyncer].
func (f *File) WriteDeferred(_ context.Context, key string, data []byte) error {
	return f.write(key, data, false)
}

// DeleteDeferred is [File.Delete] without the directory syncs: a power cut may bring the object
// back, as an orphan. Implements [backend.DeferredSyncer].
func (f *File) DeleteDeferred(_ context.Context, key string) error {
	return f.remove(key, false)
}

// SyncPrefix makes every deferred write and delete under the directory prefix names durable: each
// directory of its subtree children first, then the entries of the ancestors not known durable,
// innermost first — so no entry reaches the disk before what it names. It keeps no record of what
// the deferred operations touched, so an aborted part leaves nothing behind to leak. A prefix
// with no directory syncs nothing. Implements [backend.DeferredSyncer].
func (f *File) SyncPrefix(_ context.Context, prefix string) error {
	p, err := f.rel(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return err
	}

	root, err := f.openRoot()
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()

	epoch := f.dirs.snapshot()

	var synced []string

	if err := syncTree(root, p, &synced); err != nil {
		if errors.Is(err, fs.ErrNotExist) && len(synced) == 0 {
			return nil
		}

		return errors.Wrapf(err, "sync prefix %q", prefix)
	}

	for d := p; !f.dirs.durable(d); d = path.Dir(d) {
		if err := root.SyncDir(path.Dir(d)); err != nil {
			return errors.Wrapf(err, "sync dir %q", path.Dir(d))
		}

		synced = append(synced, d)
	}

	f.dirs.mark(synced, epoch)

	return nil
}

// syncTree syncs dir and every directory below it, children first, appending each to synced.
func syncTree(root vfs.FS, dir string, synced *[]string) error {
	entries, err := root.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.IsDir() {
			if err := syncTree(root, path.Join(dir, e.Name()), synced); err != nil {
				return err
			}
		}
	}

	if err := root.SyncDir(dir); err != nil {
		return err
	}

	*synced = append(*synced, dir)

	return nil
}
