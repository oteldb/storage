package file

import (
	"context"
	"hash/maphash"
	"io/fs"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// commitLocks serializes the read-compare-write of [File.CompareAndSwap] per key. A directory
// tree has no conditional-replace primitive to build on the way an object store has If-Match:
// rename(2) is atomic but unconditional, and the exclusive create behind [File.PutIfAbsent]
// claims a *new* name, not a transition of an existing one.
//
// The consequence is the honest limit of this backend's CAS: it is atomic against every other
// writer in this process, and not against a second process over the same directory. That matches
// what the backend already claims — [File.IsNodeLocal] reports the tree as private to its node —
// and the deployment CAS exists for (replicas of one shard over one shared store) is an object
// store, not a shared mount.
//
// The locks are a fixed shard array rather than a per-key map so a long-lived process does not
// accumulate one mutex per key ever committed; two keys colliding on a shard only serialize.
var commitLocks [64]sync.Mutex

var commitSeed = maphash.MakeSeed()

func lockFor(key string) *sync.Mutex {
	return &commitLocks[maphash.String(commitSeed, key)%uint64(len(commitLocks))]
}

// CompareAndSwap stores data under key if the file's current contents still hash to expected,
// writing through the same atomic temp+fsync+rename as [File.Write]. Implements
// [backend.Backend].
func (f *File) CompareAndSwap(
	ctx context.Context, key string, expected backend.Version, data []byte,
) (backend.Version, bool, error) {
	mu := lockFor(key)
	mu.Lock()
	defer mu.Unlock()

	current, err := f.versionOf(key)
	if err != nil {
		return backend.VersionAbsent, false, err
	}

	if current != expected {
		return backend.VersionAbsent, false, nil
	}

	if err := f.Write(ctx, key, data); err != nil {
		return backend.VersionAbsent, false, err
	}

	return backend.ContentVersion(data), true, nil
}

// ReadVersioned returns the value under key and the digest identifying it. Implements
// [backend.Backend].
func (f *File) ReadVersioned(ctx context.Context, key string) ([]byte, backend.Version, error) {
	data, err := f.Read(ctx, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil, backend.VersionAbsent, nil
		}

		return nil, backend.VersionAbsent, err
	}

	return data, backend.ContentVersion(data), nil
}

// versionOf returns the digest of key's stored contents, or [backend.VersionAbsent] if there is
// no file there.
func (f *File) versionOf(key string) (backend.Version, error) {
	p, err := f.rel(key)
	if err != nil {
		return backend.VersionAbsent, err
	}

	data, err := f.root.ReadFile(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return backend.VersionAbsent, nil
		}

		return backend.VersionAbsent, errors.Wrapf(err, "read %q", key)
	}

	return backend.ContentVersion(data), nil
}
