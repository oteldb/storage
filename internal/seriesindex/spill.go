package seriesindex

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

// WriteContiguous merges postings from disjoint, consecutive ranges of the same
// sorted view and returns the largest encoded descriptor size.
func WriteContiguous(ctx context.Context, be backend.Backend, key string, digest [32]byte,
	view series.View, refs []uint32, inputs []*Index,
) (int, error) {
	if uint64(len(refs)) > math.MaxUint32 {
		return 0, errors.New("seriesindex: too many series")
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
		return 0, errors.New("seriesindex: postings spill count mismatch")
	}
	var scratch []byte
	return writeCanonical(ctx, be, key, digest, len(refs), merge,
		func(i int) signal.SeriesID { return view.ID(int(refs[i])) },
		func(i int) ([]byte, error) {
			scratch = view.AppendCanonical(scratch[:0], int(refs[i]))
			return scratch, nil
		})
}

// WriteOrdinalMerge streams borrowed canonical records and remapped postings
// from validated immutable sources and returns the largest encoded descriptor size.
func WriteOrdinalMerge(ctx context.Context, be backend.Backend, key string, digest [32]byte,
	count int, merge *PostingMerge, idAt func(int) signal.SeriesID, rawAt func(int) ([]byte, error),
) (int, error) {
	merge.selectCovering(count)
	return writeCanonical(ctx, be, key, digest, count, merge, idAt, rawAt)
}

func writeCanonical(ctx context.Context, be backend.Backend, key string, digest [32]byte, count int,
	merge *PostingMerge, idAt func(int) signal.SeriesID, rawAt func(int) ([]byte, error),
) (int, error) {
	if count <= 0 || uint64(count) > math.MaxUint32 {
		return 0, errors.New("seriesindex: invalid series count")
	}
	w, err := backend.CreateObjectDeferred(ctx, be, key)
	if err != nil {
		return 0, err
	}
	defer w.Abort()

	table := NewTableWriter(w)
	table.seriesCount, table.parentDigest = uint32(count), digest
	var countBytes [4]byte
	binary.BigEndian.PutUint32(countBytes[:], table.seriesCount)
	if err := table.Append([]byte{'c'}, countBytes[:]); err != nil {
		return 0, err
	}

	b := Builder{mergedPostings: merge}
	if err := b.writeMergedPostings(table, 'h'); err != nil {
		return 0, err
	}
	var keyID [17]byte
	keyID[0] = 'i'
	var value [13]byte
	for i := range count {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		id := idAt(i)
		binary.BigEndian.PutUint64(keyID[1:], id.Hi)
		binary.BigEndian.PutUint64(keyID[9:], id.Lo)
		binary.BigEndian.PutUint32(value[:], uint32(i))
		if err := table.Append(keyID[:], value[:]); err != nil {
			return 0, err
		}
	}
	for _, prefix := range []byte{'n', 'p'} {
		if err := b.writeMergedPostings(table, prefix); err != nil {
			return 0, err
		}
	}
	var keyRef [5]byte
	keyRef[0] = 's'
	var record []byte
	var maxCanonicalBytes int
	for i := range count {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		id := idAt(i)
		raw, err := rawAt(i)
		if err != nil {
			return 0, err
		}
		record = id.AppendBinary(record[:0])
		record = appendMetadata(record, Metadata{})
		record = append(record, raw...)
		maxCanonicalBytes = max(maxCanonicalBytes, len(raw))
		binary.BigEndian.PutUint32(keyRef[1:], uint32(i))
		if err := table.Append(keyRef[:], record); err != nil {
			return 0, err
		}
	}
	if err := table.Finish(); err != nil {
		return 0, err
	}
	if err := w.Commit(ctx); err != nil {
		return 0, err
	}
	return maxCanonicalBytes, nil
}
