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
func (f *FS) Crash() *FS { return f.CrashWith(CrashConfig{}) }

// CrashConfig asks [FS.CrashWith] for a crash that kept some of what was never synced. The zero
// value is [FS.Crash]: nothing unsynced survives.
type CrashConfig struct {
	// UnsyncedPercent is the chance, drawn per 4 KiB data block and per pending directory entry,
	// that a real disk had already written it out when the power went. 0 keeps exactly what was
	// synced; 100 keeps the whole pre-crash state. Values outside [0, 100] are clamped.
	UnsyncedPercent int
	// Seed fixes every draw. The same seed against the same filesystem gives the same result, so a
	// failure is replayable from the seed alone.
	Seed uint64
}

// CrashWith is [FS.Crash] with a disk that may have got ahead of the syncs.
//
// Writeback is per block, not per file: a device is free to have written block n and not block n-1,
// and the surviving file then comes back *longer* than its synced length with a zero-filled gap
// where the lost block was. A replayer that reads a run of zeros as the end of the log walks past
// live records; the deterministic [FS.Crash] cannot produce that state, because losing a prefix is
// all it can do. Directory entries are drawn the same way — an unsynced create or rename may or may
// not have landed, independently of the bytes it names.
//
// Synced data is never taken away: the draws only add. So a run over many seeds explores the space
// between [FS.Crash] (nothing extra) and [FS.Kill] (everything), and every point in it is a state
// the POSIX contract permits.
func (f *FS) CrashWith(cfg CrashConfig) *FS {
	f.mu.Lock()
	defer f.mu.Unlock()

	rng := crashRand{seed: cfg.Seed, pct: min(max(cfg.UnsyncedPercent, 0), 100)}

	dirs := make(map[string]struct{}, len(f.durableDirs))
	for d := range f.durableDirs {
		dirs[d] = struct{}{}
	}

	files := make(map[string][]byte, len(f.durable))
	for name, data := range f.durable {
		files[name] = slices.Clone(data)
	}

	f.applyPending(rng, dirs, files)

	out := New()

	for d := range dirs {
		out.dirs[d] = 0o750
		out.durableDirs[d] = struct{}{}
	}

	for name, body := range files {
		// A file cannot outlive the directory naming it: if the parent never became durable, neither
		// did the path that reaches these bytes.
		if _, ok := out.dirs[path.Dir(name)]; !ok {
			continue
		}

		if n, ok := f.live[name]; ok {
			body = writeback(name, body, n.data, rng)
		}

		out.live[name] = &node{data: body, synced: slices.Clone(body), perm: 0o600}
		out.durable[name] = slices.Clone(body)
	}

	return out
}

// applyPending folds the name changes still waiting for a directory sync into dirs and files, each
// drawn on its own. Removals go first, so a name unlinked and relinked without a sync in between
// ends up present, which is what the directory itself holds.
func (f *FS) applyPending(rng crashRand, dirs map[string]struct{}, files map[string][]byte) {
	if rng.pct == 0 {
		return
	}

	for _, names := range f.unlinks {
		for _, n := range names {
			if !rng.entrySurvives(n) {
				continue
			}

			delete(files, n)
			delete(dirs, n)
		}
	}

	for _, names := range f.links {
		for _, n := range names {
			if !rng.entrySurvives(n) {
				continue
			}

			if node, ok := f.live[n]; ok {
				if _, published := files[n]; !published {
					files[n] = slices.Clone(node.synced)
				}

				continue
			}

			if _, ok := f.dirs[n]; ok {
				dirs[n] = struct{}{}
			}
		}
	}
}

// crashBlock is the granularity writeback is drawn at — the page a filesystem hands the device.
const crashBlock = 4096

// writeback grows durable with whichever blocks of live the disk is drawn to have written. A block
// landing past the durable length grows the file with zeros, leaving the hole a lost earlier block
// makes; a block that did not land keeps whatever durable already held there, so nothing synced is
// lost.
func writeback(name string, durable, live []byte, rng crashRand) []byte {
	if rng.pct == 0 {
		return durable
	}

	r := rng.stream(domainData, name)

	for i := 0; i < len(live); i += crashBlock {
		if !rng.survives(r) {
			continue
		}

		block := live[i:min(i+crashBlock, len(live))]

		if grow := i + len(block) - len(durable); grow > 0 {
			durable = append(durable, make([]byte, grow)...)
		}

		copy(durable[i:], block)
	}

	return durable
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
