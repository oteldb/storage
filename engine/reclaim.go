package engine

import "context"

// The engine's `parts` slice is the only mutable handle onto its immutable parts; flush/merge replace
// it copy-on-write so a fetch that snapshotted the slice header under the lock keeps reading a stable
// backing array after it releases the lock. A retired part (one that has left the live set) is not
// deleted from the backend until the in-flight fetches that acquired it have drained — reference
// counting, so a lock-free reader never races a delete. Only the background maintenance task (never two
// at once) mutates `parts`, which keeps the swaps single-writer-simple.

// appendPart returns a new slice with p appended — copy-on-write, so concurrent fetch snapshots of the
// old slice are unaffected.
func appendPart(parts []*part, p *part) []*part {
	out := make([]*part, len(parts), len(parts)+1)
	copy(out, parts)

	return append(out, p)
}

// replaceParts returns a new slice with every part whose prefix is in removed dropped and the given
// parts appended (add may be empty — e.g. a retention merge that produced no part — or hold several
// when the merge split its output by MaxPartBytes; nil entries are skipped). Copy-on-write. It keeps
// any part not in removed, so a part added concurrently is never lost.
func replaceParts(parts []*part, removed map[string]struct{}, add ...*part) []*part {
	out := make([]*part, 0, len(parts)+len(add))
	for _, p := range parts {
		if _, drop := removed[p.prefix]; !drop {
			out = append(out, p)
		}
	}

	for _, p := range add {
		if p != nil {
			out = append(out, p)
		}
	}

	return out
}

// retireLocked moves parts onto the pending-deletion list. They have left the live set, so no new
// fetch can acquire them; their backend objects are deleted by [reclaimRetired] once their current
// readers drain. Caller holds e.mu.
func (e *Engine) retireLocked(parts []*part) {
	e.retiring = append(e.retiring, parts...)
}

// reclaimRetired deletes the backend objects of retired parts whose readers have all drained, doing the
// delete I/O outside e.mu. A part still being read stays pending for a later cycle. Best-effort: a
// failed delete is re-queued (it leaves an orphan object meanwhile, unreferenced by the bucket index
// and ignored on reload). No-op for a head-only engine.
func (e *Engine) reclaimRetired(ctx context.Context) {
	if e.cfg.Backend == nil {
		return
	}

	e.mu.Lock()

	var (
		deletable []*part
		kept      []*part
	)

	for _, p := range e.retiring {
		if p.refs.Load() == 0 {
			deletable = append(deletable, p)
		} else {
			kept = append(kept, p)
		}
	}

	e.retiring = kept
	e.mu.Unlock()

	if len(deletable) == 0 {
		return
	}

	// A reclaimed part will never be read again — drop its decoded blocks from the cache so they do
	// not linger as cold weight until LRU pressure evicts them.
	if e.blockCache != nil {
		for _, p := range deletable {
			e.blockCache.evictPrefix(p.prefix)
		}
	}

	var failed []*part

	for _, p := range deletable {
		if err := deletePart(ctx, e.cfg.Backend, p.prefix); err != nil {
			failed = append(failed, p)
		}
	}

	if len(failed) > 0 {
		e.mu.Lock()
		e.retiring = append(e.retiring, failed...)
		e.mu.Unlock()
	}
}
