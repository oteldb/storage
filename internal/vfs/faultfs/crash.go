package faultfs

import (
	"path"
	"slices"
)

// Crash returns the filesystem a machine would come back with after losing power now: every name
// whose directory was synced, holding the bytes of that file's last [vfs.File.Sync]. Everything
// else — unsynced writes, and names created, renamed or removed without a [FS.SyncDir] — is gone.
//
// Where a real disk *may* have kept an unsynced write, this keeps none of it. That is the worst
// outcome POSIX permits, so passing here means passing on any filesystem; it also makes the result
// deterministic, which a test that samples the possible outcomes could not be.
//
// The returned FS is fresh: no rules, no recorded calls, nothing pending. The receiver is left
// alone, so one state can be crashed more than once (before and after a repair, say).
func (f *FS) Crash() *FS {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := New()

	for d := range f.durableDirs {
		out.dirs[d] = 0o750
		out.durableDirs[d] = struct{}{}
	}

	for name, data := range f.durable {
		// A file cannot outlive the directory naming it: if the parent never became durable, neither
		// did the path that reaches these bytes.
		if _, ok := out.dirs[path.Dir(name)]; !ok {
			continue
		}

		body := slices.Clone(data)
		out.live[name] = &node{data: body, synced: slices.Clone(body), perm: 0o600}
		out.durable[name] = slices.Clone(body)
	}

	return out
}

// Kill returns the filesystem a machine would come back with after the *process* died — a panic, a
// SIGKILL, an OOM — while the machine kept running. Nothing is lost: the writes are in the page
// cache and the kernel still owns them.
//
// It is here to keep the two failure modes apart. Code that survives Kill but not [FS.Crash] is
// crash-safe against a process failure and not against a power cut, and saying which one a
// durability claim covers is most of the claim.
func (f *FS) Kill() *FS {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := New()

	for d, perm := range f.dirs {
		out.dirs[d] = perm
		out.durableDirs[d] = struct{}{}
	}

	for name, n := range f.live {
		body := slices.Clone(n.data)
		out.live[name] = &node{data: body, synced: slices.Clone(body), perm: n.perm, mod: n.mod}
		out.durable[name] = slices.Clone(body)
	}

	return out
}

// Tear models a partial write reaching the platter: keep bytes of name's uncommitted tail survive
// the next [FS.Crash], the rest do not. A record framed across that boundary comes back truncated,
// which is the shape a torn append actually takes and the one a replayer has to tell apart from a
// clean end of file.
//
// keep is clamped to the uncommitted tail: 0 tears the whole tail away (the default a crash gives),
// and a keep past the tail's length commits all of it.
func (f *FS) Tear(name string, keep int) error {
	c, err := clean(name)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	n, ok := f.live[c]
	if !ok {
		return notExist("tear", name)
	}

	tail := len(n.data) - len(n.synced)
	if keep > tail {
		keep = tail
	}

	if keep < 0 {
		keep = 0
	}

	n.synced = slices.Clone(n.data[:len(n.synced)+keep])

	if _, durable := f.durable[c]; durable {
		f.durable[c] = slices.Clone(n.synced)
	}

	return nil
}

// Durable reports the bytes name would come back with after a [FS.Crash], and whether it would come
// back at all. It is the assertion the durability tests are written against.
func (f *FS) Durable(name string) ([]byte, bool) {
	c, err := clean(name)
	if err != nil {
		return nil, false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	data, ok := f.durable[c]
	if !ok {
		return nil, false
	}

	return slices.Clone(data), true
}
