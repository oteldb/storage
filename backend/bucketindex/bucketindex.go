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
}

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

	// v4 appends the per-writer flush watermarks; v3 (Generation + Removed), v2 (the anonymous
	// epoch only) and v1 (neither) still decode.
	version = 4
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

	buf := data[3:]

	n, m := binary.Uvarint(buf)
	if m <= 0 {
		return nil, errors.Wrap(ErrCorrupt, "bad count")
	}
	buf = buf[m:]

	// Guard against a bogus count claiming more entries than the buffer could hold (each
	// entry is ≥ 3 bytes: a length, and two varint times).
	if n > uint64(len(buf)) {
		return nil, errors.Wrap(ErrCorrupt, "count exceeds input")
	}

	ix := &Index{Entries: make([]Entry, 0, n)}
	for range n {
		var e Entry

		l, m := binary.Uvarint(buf)
		if m <= 0 || l > uint64(len(buf)-m) {
			return nil, errors.Wrap(ErrCorrupt, "bad prefix length")
		}
		buf = buf[m:]
		e.Prefix = string(buf[:l])
		buf = buf[l:]

		var ok bool
		if e.MinTime, buf, ok = readVarint(buf); !ok {
			return nil, errors.Wrap(ErrCorrupt, "bad min time")
		}
		if e.MaxTime, buf, ok = readVarint(buf); !ok {
			return nil, errors.Wrap(ErrCorrupt, "bad max time")
		}

		ix.Entries = append(ix.Entries, e)
	}

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
		epochs, err := decodeWriterEpochs(buf)
		if err != nil {
			return nil, err
		}

		ix.Epochs = epochs
	}

	return ix, nil
}

// decodeWriterEpochs parses the per-writer watermark slots, bounding the count by what the buffer
// could hold as the entry and removal counts are.
func decodeWriterEpochs(buf []byte) ([]WriterEpoch, error) {
	n, m := binary.Uvarint(buf)
	if m <= 0 {
		return nil, errors.Wrap(ErrCorrupt, "bad writer count")
	}
	buf = buf[m:]

	if n > uint64(len(buf)) {
		return nil, errors.Wrap(ErrCorrupt, "writer count exceeds input")
	}

	if n == 0 {
		return nil, nil // nil, not an empty slice, so encode∘decode is the identity
	}

	out := make([]WriterEpoch, 0, n)
	for range n {
		var w WriterEpoch

		l, m := binary.Uvarint(buf)
		if m <= 0 || l > uint64(len(buf)-m) {
			return nil, errors.Wrap(ErrCorrupt, "bad writer length")
		}
		buf = buf[m:]
		w.Writer = string(buf[:l])
		buf = buf[l:]

		if w.Epoch, m = binary.Uvarint(buf); m <= 0 {
			return nil, errors.Wrap(ErrCorrupt, "bad writer epoch")
		}
		buf = buf[m:]

		if w.Generation.Term, m = binary.Uvarint(buf); m <= 0 {
			return nil, errors.Wrap(ErrCorrupt, "bad writer term")
		}
		buf = buf[m:]

		if w.Generation.Counter, m = binary.Uvarint(buf); m <= 0 {
			return nil, errors.Wrap(ErrCorrupt, "bad writer counter")
		}
		buf = buf[m:]

		out = append(out, w)
	}

	return out, nil
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
