package seriesindex

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/series"
)

// WriteContiguous merges postings from disjoint, consecutive ranges of the same
// sorted view. A source's output ordinal is its original ordinal plus its range offset.
func WriteContiguous(ctx context.Context, be backend.Backend, key string, digest [32]byte,
	view series.View, refs []uint32, inputs []*Index,
) error {
	if uint64(len(refs)) > math.MaxUint32 {
		return errors.New("seriesindex: too many series")
	}
	merge := NewPostingMerge(ctx, inputs)
	merge.contiguous = true
	var offset uint64
	for i := range merge.sources {
		s := &merge.sources[i]
		s.contiguous = true
		s.offset = uint32(offset)
		offset += uint64(s.index.count)
	}
	if offset != uint64(len(refs)) {
		return errors.New("seriesindex: postings spill count mismatch")
	}
	w, err := backend.CreateObjectDeferred(ctx, be, key)
	if err != nil {
		return err
	}
	defer w.Abort()

	table := NewTableWriter(w)
	table.seriesCount, table.parentDigest = uint32(len(refs)), digest
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], table.seriesCount)
	if err := table.Append([]byte{'c'}, count[:]); err != nil {
		return err
	}

	b := Builder{mergedPostings: merge}
	if err := b.writeMergedPostings(table, 'h'); err != nil {
		return err
	}
	var keyID [17]byte
	keyID[0] = 'i'
	var value [13]byte
	for i, ref := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := view.ID(int(ref))
		binary.BigEndian.PutUint64(keyID[1:], id.Hi)
		binary.BigEndian.PutUint64(keyID[9:], id.Lo)
		binary.BigEndian.PutUint32(value[:], uint32(i))
		if err := table.Append(keyID[:], value[:]); err != nil {
			return err
		}
	}
	for _, prefix := range []byte{'n', 'p'} {
		if err := b.writeMergedPostings(table, prefix); err != nil {
			return err
		}
	}
	var keyRef [5]byte
	keyRef[0] = 's'
	var raw []byte
	for i, ref := range refs {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := view.ID(int(ref))
		raw = id.AppendBinary(raw[:0])
		raw = appendMetadata(raw, Metadata{})
		raw = view.AppendCanonical(raw, int(ref))
		binary.BigEndian.PutUint32(keyRef[1:], uint32(i))
		if err := table.Append(keyRef[:], raw); err != nil {
			return err
		}
	}
	if err := table.Finish(); err != nil {
		return err
	}
	return w.Commit(ctx)
}
