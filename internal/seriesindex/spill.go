package seriesindex

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/signal"
)

// The digest marks private postings data that must never open as a complete series index.
var postingsSpillDigest = [32]byte{'P', 'O', 'S', 'T', 'I', 'N', 'G', 'S', 1}

// PostingsSpill is a private, immutable table of postings for a contiguous ordinal range.
type PostingsSpill struct{ table *Table }

// MaxPageBytes bounds one spill cursor's workspace.
func (s *PostingsSpill) MaxPageBytes() int64 { return s.table.MaxPageBytes() }

// OpenPostingsSpill opens a private postings table without borrowing a descriptor index.
func OpenPostingsSpill(ctx context.Context, be backend.Backend, key string) (*PostingsSpill, error) {
	t, err := OpenTable(ctx, be, key)
	if err != nil {
		return nil, err
	}
	if t.parentDigest != postingsSpillDigest || t.seriesCount == 0 || t.count == 0 {
		return nil, errors.Wrap(ErrCorrupt, "postings spill header")
	}
	return &PostingsSpill{table: t}, nil
}

// WritePostingsSpill writes only label keys and postings with the local series count.
func (b *Builder) WritePostingsSpill(ctx context.Context, be backend.Backend, key string) error {
	w, err := backend.CreateObjectDeferred(ctx, be, key)
	if err != nil {
		return err
	}
	defer w.Abort()
	if err := b.write(w, true); err != nil {
		return err
	}
	return w.Commit(ctx)
}

func contiguousPostingMerge(ctx context.Context, count int, inputs []*PostingsSpill) (*PostingMerge, error) {
	if count <= 0 || uint64(count) > math.MaxUint32 {
		return nil, errors.New("seriesindex: invalid postings spill count")
	}
	m := &PostingMerge{
		ctx: ctx, sources: make([]mergePostingSource, len(inputs)),
		selectedSource: -1, contiguous: true,
	}
	for i, input := range inputs {
		t := input.table
		m.sources[i] = mergePostingSource{table: t, count: t.seriesCount, contiguous: true, offset: uint32(m.count)}
		m.count += uint64(t.seriesCount)
		m.postingRecords = buildbudget.Add(m.postingRecords, int64(t.count)-1)
	}
	if m.count != uint64(count) {
		return nil, errors.New("seriesindex: postings spill count mismatch")
	}
	return m, nil
}

// MergePostingsSpills writes the union of consecutive spills without encoding descriptors.
func MergePostingsSpills(ctx context.Context, be backend.Backend, key string, count int, inputs []*PostingsSpill) error {
	merge, err := contiguousPostingMerge(ctx, count, inputs)
	if err != nil {
		return err
	}
	w, err := backend.CreateObjectDeferred(ctx, be, key)
	if err != nil {
		return err
	}
	defer w.Abort()
	table := NewTableWriter(w)
	table.seriesCount, table.parentDigest = uint32(count), postingsSpillDigest
	var countBytes [4]byte
	binary.BigEndian.PutUint32(countBytes[:], table.seriesCount)
	if err := table.Append([]byte{'c'}, countBytes[:]); err != nil {
		return err
	}
	b := Builder{mergedPostings: merge}
	for _, prefix := range []byte{'h', 'n', 'p'} {
		if err := b.writeMergedPostings(table, prefix); err != nil {
			return err
		}
	}
	if err := table.Finish(); err != nil {
		return err
	}
	return w.Commit(ctx)
}

// WriteContiguous merges consecutive spills into one complete index of the sorted view.
func WriteContiguous(ctx context.Context, be backend.Backend, key string, digest [32]byte,
	view series.View, refs []uint32, inputs []*PostingsSpill,
) (int, error) {
	merge, err := contiguousPostingMerge(ctx, len(refs), inputs)
	if err != nil {
		return 0, err
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
