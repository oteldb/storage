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
	// FlushedEpoch is the highest WAL flush generation durably persisted in these parts (0 if
	// unused). It is the watermark a record engine reads on recovery to skip WAL records a part
	// already holds — advanced atomically with the part list, so exactly-once replay survives a
	// crash between a flush committing and its WAL being truncated. Added in format v2.
	FlushedEpoch uint64
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
	version        = 2 // v2 appends FlushedEpoch; v1 (no epoch) still decodes.
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

	return binary.AppendUvarint(dst, ix.FlushedEpoch)
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

		ix.FlushedEpoch = epoch
	}

	return ix, nil
}

func readVarint(buf []byte) (int64, []byte, bool) {
	v, m := binary.Varint(buf)
	if m <= 0 {
		return 0, buf, false
	}

	return v, buf[m:], true
}

// Load reads the index stored under key from b. A missing object is reported as an empty
// index (the read path starts before any flush has written one).
func Load(ctx context.Context, b backend.Backend, key string) (*Index, error) {
	data, err := b.Read(ctx, key)
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

// Save writes the index under key in b (overwriting the previous version atomically per the
// backend's per-object write atomicity).
func (ix *Index) Save(ctx context.Context, b backend.Backend, key string) error {
	if err := b.Write(ctx, key, ix.AppendBinary(nil)); err != nil {
		return errors.Wrapf(err, "write index %q", key)
	}

	return nil
}
