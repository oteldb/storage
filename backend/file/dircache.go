package file

import (
	"os"
	"sync"
)

// dirCache is the set of directories whose own entry, and every ancestor's, is known to be on the
// disk, so a publish stops climbing at the first one instead of syncing up to the root every time.
//
// A directory joins only after its parent's sync returned, and leaves before it is removed. The
// epoch closes the remaining gap: a publish that synced a directory a concurrent prune then removed
// would otherwise mark the name a later writer recreates, unsynced. Any removal during a publish
// discards that publish's marks, which costs a redundant sync later and nothing else.
type dirCache struct {
	mu    sync.Mutex
	epoch uint64
	known map[string]struct{}
}

func newDirCache() *dirCache { return &dirCache{known: map[string]struct{}{}} }

// dirCaches shares one cache per root directory across every [File] opened over it: a directory
// one of them prunes must stop being durable for all of them. Roots are matched by file identity
// ([os.SameFile]), not by path, so a symlink or a bind mount of the same tree shares its cache.
// Joining one clears it: a removed root's identity can be reused by a new directory, whose tree
// shares nothing with what the cache remembers, and a live tree only pays some syncs again.
var dirCaches struct {
	mu    sync.Mutex
	roots []cachedRoot
}

type cachedRoot struct {
	info  os.FileInfo
	cache *dirCache
}

func dirCacheFor(root os.FileInfo) *dirCache {
	dirCaches.mu.Lock()
	defer dirCaches.mu.Unlock()

	for _, r := range dirCaches.roots {
		if os.SameFile(r.info, root) {
			r.cache.reset()

			return r.cache
		}
	}

	c := newDirCache()
	dirCaches.roots = append(dirCaches.roots, cachedRoot{info: root, cache: c})

	return c
}

func (c *dirCache) durable(dir string) bool {
	if dir == "." {
		return true
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.known[dir]

	return ok
}

func (c *dirCache) snapshot() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.epoch
}

func (c *dirCache) mark(dirs []string, epoch uint64) {
	if len(dirs) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.epoch != epoch {
		return
	}

	for _, d := range dirs {
		c.known[d] = struct{}{}
	}
}

// forget drops dir alone, not its descendants: this package only ever removes an empty directory,
// and removes a subtree bottom-up.
func (c *dirCache) forget(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.epoch++

	delete(c.known, dir)
}

func (c *dirCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.epoch++

	clear(c.known)
}
