package recordengine

import (
	"cmp"
	"context"
	"iter"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// SideStore is an optional per-engine auxiliary store that rides the part lifecycle. A signal whose
// records reference content-addressed side data (e.g. the profiles symbol store: strings, functions,
// locations, stacks) supplies one via [Config.SideStore]; the engine then absorbs each batch's delta
// ([Batch.Side]) into a live accumulator, persists the accumulator as part sidecars on flush, and
// unions the sidecars of compacted parts on merge. The engine treats the data opaquely — only the
// signal package knows the table formats.
//
// Content-addressing is the load-bearing assumption: an entry's id is a hash of its content, so the
// same entry has the same id everywhere and [SideStore.Union] is a plain dedup with no id remap.
//
// All methods are called under the engine's lock, so an implementation need not be safe for
// concurrent use. [SideStore.Union] must be a pure function of its arguments and must not read or
// mutate the live accumulator (it merges already-flushed part data, independent of the head).
type SideStore interface {
	// Absorb merges one batch's encoded side delta ([Batch.Side]) into the live accumulator.
	Absorb(delta []byte) error
	// Encode serializes the accumulated side data into named sidecar payloads (name → bytes),
	// written as {prefix}/sym-{name}.bin at flush.
	Encode() map[string][]byte
	// Reset clears the live accumulator (after a flush drains the head).
	Reset()
	// RefColumn names the byte column whose cells are the ids the records reference in this store.
	RefColumn() string
	// Retain drops every accumulated entry that the refs (the [SideStore.RefColumn] cells of the
	// records left in the head) do not reach, directly or transitively.
	Retain(refs iter.Seq[[]byte])
	// Restore merges an [SideStore.Encode] snapshot back into the live accumulator. The engine calls
	// it when a flush fails after the snapshot+Reset: the records return to the head, so their side
	// data must too. Content-addressing makes the merge a plain dedup with whatever the accumulator
	// gained meanwhile.
	Restore(snapshot map[string][]byte) error
	// Names returns the sidecar names to read back for a part on merge (the keys [SideStore.Encode]
	// may produce). A part missing a named sidecar is skipped.
	Names() []string
	// Union merges the loaded sidecars of the compacted parts (one map per part) and returns the
	// named payloads to write under the new part: only the entries the refs (the [SideStore.RefColumn]
	// cells of that part) reach, directly or transitively. Pure; ignores the live accumulator.
	Union(parts []map[string][]byte, refs iter.Seq[[]byte]) (map[string][]byte, error)
	// Stored re-encodes named payloads in their on-disk form. The engine applies it to exactly what
	// it writes as sidecars, so [SideStore.Encode] and [SideStore.Union] can return a form that is
	// cheap to decode again in memory. Pure, like Union.
	Stored(tables map[string][]byte) (map[string][]byte, error)
}

// sidecarKey is the backend key of a side-store table sidecar under a part prefix (mirrors the
// per-column bloom sidecars, e.g. {prefix}/sym-stacks.bin).
func sidecarKey(prefix, name string) string { return prefix + "/sym-" + name + ".bin" }

// writeSidecars writes each named side-store payload under prefix.
func writeSidecars(ctx context.Context, b backend.Backend, prefix string, m map[string][]byte) error {
	for name, data := range m {
		if err := backend.WriteDeferred(ctx, b, sidecarKey(prefix, name), data); err != nil {
			return errors.Wrapf(err, "write sidecar %q", name)
		}
	}

	return nil
}

// loadSidecars reads the named side-store sidecars under prefix, skipping any that are absent.
func loadSidecars(ctx context.Context, b backend.Backend, prefix string, names []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(names))

	for _, name := range names {
		data, err := b.Read(ctx, sidecarKey(prefix, name))

		switch {
		case errors.Is(err, backend.ErrNotExist):
			continue
		case err != nil:
			return nil, errors.Wrapf(err, "read sidecar %q", name)
		}

		out[name] = data
	}

	return out, nil
}

// SidePart is one readable part's side data.
type SidePart struct {
	// Key is the part prefix. No other part reuses it and a part never changes, so it keys a cache
	// of the decoded side data.
	Key string

	be    backend.Backend
	names []string
}

// Load reads the part's sidecars, skipping any that are absent.
func (p SidePart) Load(ctx context.Context) (map[string][]byte, error) {
	return loadSidecars(ctx, p.be, p.Key, p.names)
}

// SideRead is the side data a read over a window needs, besides the live accumulator. Its parts stay
// readable until [SideRead.Release].
type SideRead struct {
	// Flushing is the side snapshot of the records an in-flight flush detached: they are fetchable
	// and in no part yet. nil when no flush is in flight.
	Flushing map[string][]byte
	// Parts are the readable parts overlapping the window, newest first.
	Parts []SidePart

	pinned []*part
}

// Release lets a merge or retention reclaim the parts. Idempotent.
func (r *SideRead) Release() {
	for _, p := range r.pinned {
		p.release()
	}

	r.pinned = nil
}

// ReadSide pins the side data a read over [start, end] needs (a zero start AND end selects every
// part) and calls head with the live side store under the engine's read lock, where the caller
// snapshots the unflushed side data; head must not retain it. The read is empty and head is not
// called when the engine has no side store. Safe for concurrent use.
func (e *Engine) ReadSide(start, end int64, head func(live SideStore)) *SideRead {
	e.mu.RLock()
	defer e.mu.RUnlock()

	r := &SideRead{}
	if e.cfg.SideStore == nil {
		return r
	}

	head(e.cfg.SideStore)
	r.Flushing = e.flushingSide

	for _, p := range e.readablePartsLocked() {
		if partInWindow(p, start, end) {
			p.acquire()
			r.pinned = append(r.pinned, p)
		}
	}

	slices.SortFunc(r.pinned, func(a, b *part) int {
		return cmp.Or(cmp.Compare(b.maxTime, a.maxTime), cmp.Compare(b.prefix, a.prefix))
	})

	names := e.cfg.SideStore.Names()
	r.Parts = make([]SidePart, len(r.pinned))

	for i, p := range r.pinned {
		r.Parts[i] = SidePart{Key: p.prefix, be: e.cfg.Backend, names: names}
	}

	return r
}
