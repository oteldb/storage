package engine

import (
	"context"
	"strings"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/internal/partid"
)

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

// partRows totals the rows of a part set (nil entries skipped, as [replaceParts] allows).
func partRows(parts []*part) int {
	n := 0

	for _, p := range parts {
		if p != nil {
			n += p.rows()
		}
	}

	return n
}

// retireLocked moves parts onto the pending-deletion list. They have left the live set, so no new
// fetch can acquire them; their backend objects are deleted by [reclaimRetired] once their current
// readers drain. Caller holds e.mu.
func (e *Engine) retireLocked(parts []*part) {
	e.retiring = append(e.retiring, parts...)
}

// sweepOrphansLocked deletes every object under the engine's prefix that belongs to a part the loaded
// bucket index does not name: the residue of a flush or merge that wrote a part's objects and then
// failed before committing it, or of a reclaim whose delete failed. Deletes are best-effort (a failure
// leaves the object for a later open); a failed List is fatal. Caller holds e.mu.
func (e *Engine) sweepOrphansLocked(ctx context.Context) error {
	root := e.cfg.Prefix + "/"

	keys, err := e.cfg.Backend.List(ctx, root)
	if err != nil {
		return errors.Wrap(err, "list objects")
	}

	live := make(map[string]struct{}, len(e.parts))
	for _, p := range e.parts {
		live[p.prefix] = struct{}{}
	}

	// A wanted part's objects are not orphans, they are what is left of it. Deleting the readable
	// remainder of a part repair is about to fetch destroys evidence and buys nothing: the want
	// bounds how long it is kept, and a discharged want puts the prefix back in the live set.
	for _, w := range e.wants {
		live[w.Prefix] = struct{}{}
	}

	for _, w := range e.pendingWants {
		live[w.Prefix] = struct{}{}
	}

	var orphans []string

	for _, k := range keys {
		dir, _, ok := strings.Cut(strings.TrimPrefix(k, root), "/")
		if !ok {
			continue // an engine-level object (bucket index, series index), not part of a part
		}

		if !partid.Valid(dir) {
			continue
		}

		if _, ok := live[root+dir]; ok {
			continue
		}

		orphans = append(orphans, k)
	}

	if len(orphans) == 0 {
		return nil
	}

	var failed int

	for _, k := range orphans {
		if err := e.cfg.Backend.Delete(ctx, k); err != nil && !errors.Is(err, backend.ErrNotExist) {
			failed++
		}
	}

	zctx.From(ctx).Debug("swept orphan part objects",
		zap.String("prefix", e.cfg.Prefix),
		zap.Int("objects", len(orphans)), zap.Int("failed", failed))

	return nil
}

// reclaimRetired deletes the backend objects of retired parts whose readers have all drained, doing the
// delete I/O outside e.mu. A part still being read stays pending for a later cycle. Best-effort: a
// failed delete is re-queued (it leaves an orphan object meanwhile, unreferenced by the bucket index
// and swept by the next [Engine.LoadParts]). No-op for a head-only engine.
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
