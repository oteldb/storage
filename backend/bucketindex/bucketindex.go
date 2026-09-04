// Package bucketindex maintains a compact, incremental index of the immutable parts under a
// key prefix, so a stateless reader enumerates a tenant's parts (and prunes them by time)
// from a single object instead of a full, expensive bucket LIST (DESIGN.md §11, the
// object-store-native read path). The index is itself a backend object, rewritten as parts
// are added by flush/merge and removed by retention.
//
// The on-disk form is a small, versioned binary blob (see [Index.AppendBinary] /
// [Decode]); it is fuzzed for decode safety and golden-tested for format stability.
package bucketindex

import (
	"context"
	"encoding/binary"
	"math"
	"slices"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// Object is the conventional backend key, relative to a prefix, under which the index is
// stored. Callers join it with the tenant/signal prefix, e.g. "default/metrics/" + Object.
const Object = "bucket-index.bin"

// Entry describes one immutable part: its key prefix (the part's object-group root, e.g.
// "default/metrics/0000000001") and the inclusive unix-nanosecond time range of its samples,
// used to prune parts that cannot intersect a query window.
type Entry struct {
	Prefix  string
	MinTime int64
	MaxTime int64
	// Blocks is the interval of block numbers this part covers and Level its merge depth: a flush
	// writes [n, n] at level 0, a merge over parts spanning [a … b] writes [a, b] above it. The two
	// together make supersession decidable from identity alone — see [Entry.Supersedes]. Both are
	// unset in a part written before format v5, which carries neither. Added in format v5.
	Blocks Interval
	Level  uint32
	// Hole marks this entry as an acknowledged loss rather than a part: the writer owed a repair
	// for these blocks, no owner could supply them, and it committed this in their place so the
	// obligation stops blocking reads. It names no objects and holds no rows.
	//
	// The flag lives on the entry, not in a side list, because every reader already carries the
	// entry: a hole a read path forgot to join against would be indistinguishable from an empty
	// part, which turns acknowledged loss back into silent loss. Added in format v5.
	Hole bool
}

// Data reports whether the entry names a real part — anything a read may open, a merge may
// consume, a peer may copy. A hole is the one entry that does not.
func (e Entry) Data() bool { return !e.Hole }

// Index is the set of parts under a prefix, kept sorted by [Entry.Prefix]. The zero value is
// a valid empty index.
type Index struct {
	Entries []Entry
	// FlushedEpoch is the *anonymous* writer's WAL flush watermark: the highest flush generation
	// it has durably persisted into these parts (0 if unused). Read and written through
	// [Index.WriterEpoch] / [Index.SetWriterEpoch] with an empty writer id — a single-writer
	// engine, and every writer of a pre-v4 index, which had no other slot. Added in format v2.
	FlushedEpoch uint64
	// Generation orders index states — see [Generation], which is the whole of why it exists.
	// Zero in an index written before format v3, which is below every generation a writer
	// produces. Added in format v3.
	Generation Generation
	// Removed are the parts this writer deliberately took out, newest-bounded — see [Removal].
	// Kept sorted by prefix. Added in format v3.
	Removed []Removal
	// Epochs are the named writers' WAL flush watermarks, one slot each — see [WriterEpoch], which
	// is the whole of why the watermark cannot be a single number. Kept sorted by writer id.
	// Added in format v4.
	Epochs []WriterEpoch
	// Wanted are the parts this writer holds no readable copy of and owes a repair for — see
	// [Want]. Kept sorted by prefix. A part leaves [Index.Entries] only into [Index.Removed] or
	// into here. Added in format v5.
	Wanted []Want
	// LostParts counts the holes this shard's writers have ever committed — parts no owner could
	// supply, acknowledged as lost (see [Entry.Hole]). It only ever rises: a writer carries the
	// value it read forward and takes the maximum when it rebases on a rival's commit, so it is a
	// cluster-visible fact rather than a per-node level a restart resets. Added in format v5.
	LostParts uint64
}

// Add inserts e, replacing any existing entry with the same prefix, keeping the index sorted.
func (ix *Index) Add(e Entry) {
	i, found := slices.BinarySearchFunc(ix.Entries, e, func(a, b Entry) int {
		return strings.Compare(a.Prefix, b.Prefix)
	})
	if found {
		ix.Entries[i] = e

		return
	}

	ix.Entries = slices.Insert(ix.Entries, i, e)
}

// Remove deletes the entry with the given prefix, reporting whether one was removed.
func (ix *Index) Remove(prefix string) bool {
	i, found := slices.BinarySearchFunc(ix.Entries, Entry{Prefix: prefix}, func(a, b Entry) int {
		return strings.Compare(a.Prefix, b.Prefix)
	})
	if !found {
		return false
	}

	ix.Entries = slices.Delete(ix.Entries, i, i+1)

	return true
}

// Overlapping returns, in index order, the parts whose time range intersects the inclusive
// window [start, end]. It is the read-path prune: only these parts need to be opened.
func (ix *Index) Overlapping(start, end int64) []Entry {
	var out []Entry
	for _, e := range ix.Entries {
		if e.MinTime <= end && e.MaxTime >= start {
			out = append(out, e)
		}
	}

	return out
}

const (
	magic0, magic1 = 'B', 'I'

	// v5 carries the block interval and level on each entry and appends the wanted list; v4 (the
	// per-writer flush watermarks), v3 (Generation + Removed), v2 (the anonymous epoch only) and
	// v1 (neither) still decode.
	//
	// Reading is backward compatible; writing is not. [Decode] rejects any version above this one,
	// so a node on pre-v5 code cannot read an index this one writes: every node that reads a given
	// index must be upgraded together. See backend/ARCH.md for the blast radius per deployment.
	version = 5
)

// AppendBinary appends the versioned binary encoding of the index to dst (append-style for
// buffer reuse).
func (ix *Index) AppendBinary(dst []byte) []byte {
	dst = append(dst, magic0, magic1, version)
	dst = binary.AppendUvarint(dst, uint64(len(ix.Entries)))
	for i := range ix.Entries {
		e := &ix.Entries[i]
		dst = binary.AppendUvarint(dst, uint64(len(e.Prefix)))
		dst = append(dst, e.Prefix...)
		dst = binary.AppendVarint(dst, e.MinTime)
		dst = binary.AppendVarint(dst, e.MaxTime)
		dst = binary.AppendUvarint(dst, e.Blocks.Min)
		dst = binary.AppendUvarint(dst, e.Blocks.Max)
		dst = binary.AppendUvarint(dst, uint64(e.Level))
		dst = binary.AppendUvarint(dst, entryFlags(*e))
	}

	dst = binary.AppendUvarint(dst, ix.FlushedEpoch)
	dst = binary.AppendUvarint(dst, ix.Generation.Term)
	dst = binary.AppendUvarint(dst, ix.Generation.Counter)

	dst = binary.AppendUvarint(dst, uint64(len(ix.Removed)))
	for i := range ix.Removed {
		r := &ix.Removed[i]
		dst = binary.AppendUvarint(dst, uint64(len(r.Prefix)))
		dst = append(dst, r.Prefix...)
		dst = binary.AppendUvarint(dst, r.Generation.Term)
		dst = binary.AppendUvarint(dst, r.Generation.Counter)
	}

	dst = binary.AppendUvarint(dst, uint64(len(ix.Epochs)))
	for i := range ix.Epochs {
		w := &ix.Epochs[i]
		dst = binary.AppendUvarint(dst, uint64(len(w.Writer)))
		dst = append(dst, w.Writer...)
		dst = binary.AppendUvarint(dst, w.Epoch)
		dst = binary.AppendUvarint(dst, w.Generation.Term)
		dst = binary.AppendUvarint(dst, w.Generation.Counter)
	}

	dst = binary.AppendUvarint(dst, uint64(len(ix.Wanted)))
	for i := range ix.Wanted {
		w := &ix.Wanted[i]
		dst = binary.AppendUvarint(dst, uint64(len(w.Prefix)))
		dst = append(dst, w.Prefix...)
		dst = binary.AppendUvarint(dst, w.Blocks.Min)
		dst = binary.AppendUvarint(dst, w.Blocks.Max)
		dst = binary.AppendUvarint(dst, uint64(w.Level))
		dst = binary.AppendVarint(dst, w.MinTime)
		dst = binary.AppendVarint(dst, w.MaxTime)
		dst = binary.AppendUvarint(dst, w.Generation.Term)
		dst = binary.AppendUvarint(dst, w.Generation.Counter)
	}

	dst = binary.AppendUvarint(dst, ix.LostParts)

	return dst
}

// ErrCorrupt is returned (wrapped) by [Decode] for malformed input.
var ErrCorrupt = errors.New("bucketindex: corrupt index")

// Decode parses the binary encoding produced by [Index.AppendBinary]. It is defensive
// against truncated/malformed input (it is fuzzed).
func Decode(data []byte) (*Index, error) {
	if len(data) < 3 || data[0] != magic0 || data[1] != magic1 {
		return nil, errors.Wrap(ErrCorrupt, "bad magic")
	}

	ver := data[2]
	if ver < 1 || ver > version {
		return nil, errors.Wrapf(ErrCorrupt, "unsupported version %d", ver)
	}

	entries, buf, err := decodeEntries(data[3:], ver)
	if err != nil {
		return nil, err
	}

	ix := &Index{Entries: entries}

	// v2+ appends the flush-epoch watermark; v1 has none (it stays 0).
	if ver >= 2 {
		epoch, m := binary.Uvarint(buf)
		if m <= 0 {
			return nil, errors.Wrap(ErrCorrupt, "bad flushed epoch")
		}
		buf = buf[m:]

		ix.FlushedEpoch = epoch
	}

	// v3+ appends the commit generation; earlier versions leave it zero, which orders below
	// every generation a writer produces.
	if ver >= 3 {
		term, m := binary.Uvarint(buf)
		if m <= 0 {
			return nil, errors.Wrap(ErrCorrupt, "bad generation term")
		}
		buf = buf[m:]

		counter, m := binary.Uvarint(buf)
		if m <= 0 {
			return nil, errors.Wrap(ErrCorrupt, "bad generation counter")
		}

		buf = buf[m:]

		ix.Generation = Generation{Term: term, Counter: counter}

		removed, rest, err := decodeRemovals(buf)
		if err != nil {
			return nil, err
		}

		ix.Removed = removed
		buf = rest
	}

	// v4+ appends the named writers' watermarks; earlier versions carry only the anonymous slot.
	if ver >= 4 {
		epochs, rest, err := decodeWriterEpochs(buf)
		if err != nil {
			return nil, err
		}

		ix.Epochs = epochs
		buf = rest
	}

	// v5+ appends the outstanding repair obligations; earlier versions could not express one.
	if ver >= 5 {
		wanted, rest, err := decodeWants(buf)
		if err != nil {
			return nil, err
		}

		ix.Wanted = wanted

		lost, m := binary.Uvarint(rest)
		if m <= 0 {
			return nil, errors.Wrap(ErrCorrupt, "bad lost part count")
		}

		ix.LostParts = lost
	}

	return ix, nil
}

// decodeEntries parses the part list, bounding the count by what the buffer could hold as the
// removal and writer counts are.
func decodeEntries(buf []byte, ver uint8) ([]Entry, []byte, error) {
	n, m := binary.Uvarint(buf)
	if m <= 0 {
		return nil, nil, errors.Wrap(ErrCorrupt, "bad count")
	}
	buf = buf[m:]

	// Guard against a bogus count claiming more entries than the buffer could hold (each
	// entry is ≥ 3 bytes: a length, and two varint times).
	if n > uint64(len(buf)) {
		return nil, nil, errors.Wrap(ErrCorrupt, "count exceeds input")
	}

	if n == 0 {
		return nil, buf, nil // nil, not an empty slice, so encode∘decode is the identity
	}

	out := make([]Entry, 0, n)
	for range n {
		var (
			e  Entry
			ok bool
		)

		l, m := binary.Uvarint(buf)
		if m <= 0 || l > uint64(len(buf)-m) {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad prefix length")
		}
		buf = buf[m:]
		e.Prefix = string(buf[:l])
		buf = buf[l:]

		if e.MinTime, buf, ok = readVarint(buf); !ok {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad min time")
		}
		if e.MaxTime, buf, ok = readVarint(buf); !ok {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad max time")
		}

		// v5+ carries the block identity inline; earlier entries leave it unset, which takes part
		// in no containment (see [Interval.Valid]).
		if ver >= 5 {
			if buf, ok = decodeBlockIdentity(buf, &e); !ok {
				return nil, nil, errors.Wrap(ErrCorrupt, "bad block identity")
			}
		}

		out = append(out, e)
	}

	return out, buf, nil
}

// entryFlagHole is bit 0 of an entry's flag word, [Entry.Hole]. Every other bit is reserved, and
// [decodeBlockIdentity] rejects them: an unknown bit must never read as a data-bearing part.
const entryFlagHole = 1

func entryFlags(e Entry) uint64 {
	if e.Hole {
		return entryFlagHole
	}

	return 0
}

func decodeBlockIdentity(buf []byte, e *Entry) ([]byte, bool) {
	var ok bool
	if e.Blocks.Min, buf, ok = readUvarint(buf); !ok {
		return nil, false
	}
	if e.Blocks.Max, buf, ok = readUvarint(buf); !ok {
		return nil, false
	}

	level, buf, ok := readUvarint(buf)
	if !ok || level > math.MaxUint32 {
		return nil, false
	}

	e.Level = uint32(level)

	flags, buf, ok := readUvarint(buf)
	if !ok || flags & ^uint64(entryFlagHole) != 0 {
		return nil, false
	}

	e.Hole = flags&entryFlagHole != 0

	return buf, true
}

// decodeWants parses the wanted list, bounding the count by what the buffer could hold as the
// entry and removal counts are.
func decodeWants(buf []byte) ([]Want, []byte, error) {
	n, m := binary.Uvarint(buf)
	if m <= 0 {
		return nil, nil, errors.Wrap(ErrCorrupt, "bad want count")
	}
	buf = buf[m:]

	if n > uint64(len(buf)) {
		return nil, nil, errors.Wrap(ErrCorrupt, "want count exceeds input")
	}

	if n == 0 {
		return nil, buf, nil // nil, not an empty slice, so encode∘decode is the identity
	}

	out := make([]Want, 0, n)
	for range n {
		var (
			w  Want
			ok bool
		)

		l, m := binary.Uvarint(buf)
		if m <= 0 || l > uint64(len(buf)-m) {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad want prefix length")
		}
		buf = buf[m:]
		w.Prefix = string(buf[:l])
		buf = buf[l:]

		if w.Blocks.Min, buf, ok = readUvarint(buf); !ok {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad want block min")
		}
		if w.Blocks.Max, buf, ok = readUvarint(buf); !ok {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad want block max")
		}

		level, rest, lok := readUvarint(buf)
		if !lok || level > math.MaxUint32 {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad want level")
		}

		w.Level, buf = uint32(level), rest

		if w.MinTime, buf, ok = readVarint(buf); !ok {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad want min time")
		}
		if w.MaxTime, buf, ok = readVarint(buf); !ok {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad want max time")
		}
		if w.Generation.Term, buf, ok = readUvarint(buf); !ok {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad want term")
		}
		if w.Generation.Counter, buf, ok = readUvarint(buf); !ok {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad want counter")
		}

		out = append(out, w)
	}

	return out, buf, nil
}

// decodeWriterEpochs parses the per-writer watermark slots, bounding the count by what the buffer
// could hold as the entry and removal counts are.
func decodeWriterEpochs(buf []byte) ([]WriterEpoch, []byte, error) {
	n, m := binary.Uvarint(buf)
	if m <= 0 {
		return nil, nil, errors.Wrap(ErrCorrupt, "bad writer count")
	}
	buf = buf[m:]

	if n > uint64(len(buf)) {
		return nil, nil, errors.Wrap(ErrCorrupt, "writer count exceeds input")
	}

	if n == 0 {
		return nil, buf, nil // nil, not an empty slice, so encode∘decode is the identity
	}

	out := make([]WriterEpoch, 0, n)
	for range n {
		var w WriterEpoch

		l, m := binary.Uvarint(buf)
		if m <= 0 || l > uint64(len(buf)-m) {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad writer length")
		}
		buf = buf[m:]
		w.Writer = string(buf[:l])
		buf = buf[l:]

		if w.Epoch, m = binary.Uvarint(buf); m <= 0 {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad writer epoch")
		}
		buf = buf[m:]

		if w.Generation.Term, m = binary.Uvarint(buf); m <= 0 {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad writer term")
		}
		buf = buf[m:]

		if w.Generation.Counter, m = binary.Uvarint(buf); m <= 0 {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad writer counter")
		}
		buf = buf[m:]

		out = append(out, w)
	}

	return out, buf, nil
}

// decodeRemovals parses the tombstone list, defensively: the count is bounded by what the buffer
// could hold, as the entry count is.
func decodeRemovals(buf []byte) ([]Removal, []byte, error) {
	n, m := binary.Uvarint(buf)
	if m <= 0 {
		return nil, nil, errors.Wrap(ErrCorrupt, "bad removal count")
	}
	buf = buf[m:]

	if n > uint64(len(buf)) {
		return nil, nil, errors.Wrap(ErrCorrupt, "removal count exceeds input")
	}

	if n == 0 {
		return nil, buf, nil // nil, not an empty slice, so encode∘decode is the identity
	}

	out := make([]Removal, 0, n)
	for range n {
		var r Removal

		l, m := binary.Uvarint(buf)
		if m <= 0 || l > uint64(len(buf)-m) {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad removal prefix length")
		}
		buf = buf[m:]
		r.Prefix = string(buf[:l])
		buf = buf[l:]

		if r.Generation.Term, m = binary.Uvarint(buf); m <= 0 {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad removal term")
		}
		buf = buf[m:]

		if r.Generation.Counter, m = binary.Uvarint(buf); m <= 0 {
			return nil, nil, errors.Wrap(ErrCorrupt, "bad removal counter")
		}
		buf = buf[m:]

		out = append(out, r)
	}

	return out, buf, nil
}

func readUvarint(buf []byte) (uint64, []byte, bool) {
	v, m := binary.Uvarint(buf)
	if m <= 0 {
		return 0, buf, false
	}

	return v, buf[m:], true
}

func readVarint(buf []byte) (int64, []byte, bool) {
	v, m := binary.Varint(buf)
	if m <= 0 {
		return 0, buf, false
	}

	return v, buf[m:], true
}

// Load reads the index stored under key from b. A missing object is reported as an empty
// index (the read path starts before any flush has written one). Use [LoadVersioned] when the
// index will be written back: a commit needs the version it was read at.
func Load(ctx context.Context, b backend.Backend, key string) (*Index, error) {
	// Uncached, like [LoadVersioned]: the index is rewritten as parts come and go, and over a
	// shared store by writers this process cannot see. A resident copy proves only what this
	// process last read, and serving one here would hide a peer's commit behind a stale part set.
	data, err := backend.ReadUncached(ctx, b, key)
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return &Index{}, nil
		}

		return nil, errors.Wrapf(err, "read index %q", key)
	}

	ix, err := Decode(data)
	if err != nil {
		return nil, errors.Wrapf(err, "decode index %q", key)
	}

	return ix, nil
}

// LoadVersioned reads the index and the backend version it was read at — the token
// [Index.Save] commits against. A missing object yields an empty index at
// [backend.VersionAbsent], which is the version a first commit expects.
func LoadVersioned(ctx context.Context, b backend.Backend, key string) (*Index, backend.Version, error) {
	data, version, err := b.ReadVersioned(ctx, key)
	if err != nil {
		return nil, backend.VersionAbsent, errors.Wrapf(err, "read index %q", key)
	}

	if version == backend.VersionAbsent {
		return &Index{}, backend.VersionAbsent, nil
	}

	ix, err := Decode(data)
	if err != nil {
		return nil, backend.VersionAbsent, errors.Wrapf(err, "decode index %q", key)
	}

	return ix, version, nil
}

// ErrConflict is returned (wrapped) by [Index.Save] when the stored index is no longer the one
// the committer read: another writer committed in between, and this commit did not land.
//
// It is an error *here*, unlike at the backend seam where a lost race is a plain false, because
// the caller of a commit has a part in the store that nothing yet references. Anything short of
// an error would let a flush report success while its entry was dropped — the failure #392 is
// about. The caller reloads and retries; only exhausting its retries is a real failure.
var ErrConflict = errors.New("bucketindex: index changed since it was loaded")

// Save commits the index under key, replacing the version the caller loaded and nothing else.
// It returns the version the committed index now has, which the committer holds for its next
// commit without re-reading the object. Pass [backend.VersionAbsent] as expected to commit an
// index into a prefix that has none yet.
//
// A commit that loses the race wraps [ErrConflict] and has written nothing: reload with
// [LoadVersioned], rebuild on top of what is there, and try again.
func (ix *Index) Save(
	ctx context.Context, b backend.Backend, key string, expected backend.Version,
) (backend.Version, error) {
	version, ok, err := b.CompareAndSwap(ctx, key, expected, ix.AppendBinary(nil))
	if err != nil {
		return backend.VersionAbsent, errors.Wrapf(err, "commit index %q", key)
	}

	if !ok {
		return backend.VersionAbsent, errors.Wrapf(ErrConflict, "commit index %q", key)
	}

	return version, nil
}
