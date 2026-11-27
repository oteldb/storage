// Package seriestable stores immutable identity-only runs under the engine's catalog.
package seriestable

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/signal"
)

// IDColumn is the sole physical column in a metadata run.
const IDColumn = "series"

// Builder owns sorted, distinct canonical identities; it carries no sample metadata.
type Builder struct{ index seriesindex.Builder }

// NewBorrowedBuilder builds postings while borrowing descriptors from a pinned view.
func NewBorrowedBuilder(view series.View) Builder {
	return Builder{index: seriesindex.NewBorrowedBuilder(view)}
}

// AddBorrowed appends an ordered descriptor reference from the pinned view.
func (b *Builder) AddBorrowed(ref int) error {
	return b.index.AddBorrowed(ref, seriesindex.Metadata{})
}

// Add copies a checked identity in strictly increasing canonical-ID order.
func (b *Builder) Add(id signal.SeriesID, s signal.Series) error {
	return b.index.Add(id, s, seriesindex.Metadata{})
}

// AddView copies a descriptor from a validated registry in canonical-ID order.
func (b *Builder) AddView(view series.View, i int) error {
	if i < 0 || i >= view.Len() {
		return errors.New("seriestable: identity reference outside view")
	}
	return b.index.AddView(view, i, seriesindex.Metadata{})
}

// Count returns the number of descriptors.
func (b *Builder) Count() int { return b.index.Count() }

// Bytes bounds builder and ID-column construction workspace.
func (b *Builder) Bytes() int64 { return b.index.Bytes() + int64(b.Count())*128 }

// StreamingDirectoryBytes bounds the index directory when pages are streamed.
func (b *Builder) StreamingDirectoryBytes() int64 { return b.index.StreamingDirectoryBytes() }

// WritePostingsSpill writes the postings for a private, contiguous spill.
func (b *Builder) WritePostingsSpill(ctx context.Context, be backend.Backend, key string) error {
	return b.index.WritePostingsSpill(ctx, be, key)
}

// EncodedBound bounds buffered output before compression.
func (b *Builder) EncodedBound() int64 { return b.index.EncodedBound() + int64(b.Count())*32 + 4096 }

// Write writes and syncs an unused, caller-owned prefix; it does not publish a catalog.
func Write(ctx context.Context, be backend.Backend, prefix string, b *Builder) (*Run, error) {
	return WriteWithIDCache(ctx, be, prefix, b, nil)
}

// WriteWithIDCache writes a run and retains its IDs within cache's memory budget.
func WriteWithIDCache(ctx context.Context, be backend.Backend, prefix string, b *Builder, cache *IDCache) (*Run, error) {
	if b == nil || b.Count() == 0 {
		return nil, errors.New("seriestable: empty run")
	}
	return writeRun(ctx, be, prefix, b.Count(), b.index.ID, func(key string, digest [32]byte) (int, error) {
		b.index.BindParent(digest)
		if err := b.index.WriteObject(ctx, be, key); err != nil {
			return 0, err
		}
		return b.index.MaxCanonicalBytes(), nil
	}, cache)
}

// WriteContiguous writes one immutable run from bounded, private postings spills.
func WriteContiguous(ctx context.Context, be backend.Backend, prefix string, view series.View, refs []uint32,
	inputs []*seriesindex.PostingsSpill, cache *IDCache,
) (*Run, error) {
	return writeRun(ctx, be, prefix, len(refs), func(i int) signal.SeriesID { return view.ID(int(refs[i])) },
		func(key string, digest [32]byte) (int, error) {
			return seriesindex.WriteContiguous(ctx, be, key, digest, view, refs, inputs)
		}, cache)
}

// WriteOrdinalMerge writes a descriptor run from pinned source ordinals.
func WriteOrdinalMerge(ctx context.Context, be backend.Backend, prefix string, count int,
	merge *seriesindex.PostingMerge, idAt func(int) signal.SeriesID, rawAt func(int) ([]byte, error), cache *IDCache,
) (*Run, error) {
	return writeRun(ctx, be, prefix, count, idAt, func(key string, digest [32]byte) (int, error) {
		return seriesindex.WriteOrdinalMerge(ctx, be, key, digest, count, merge, idAt, rawAt)
	}, cache)
}

func writeRun(ctx context.Context, be backend.Backend, prefix string, count int, idAt func(int) signal.SeriesID,
	writeIndex func(string, [32]byte) (int, error), cache *IDCache,
) (*Run, error) {
	w := block.NewStreamWriterTo(ctx, be, prefix, block.WithSizingStats())
	defer w.Abort()
	if err := w.AddColumn(block.Column{Name: IDColumn, Kind: block.KindInt128}); err != nil {
		return nil, err
	}
	for i := range count {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := idAt(i)
		if err := w.AppendU128Run(0, chunk.U128(id), 1); err != nil {
			return nil, err
		}
	}
	if err := block.WriteStreamPart(ctx, be, prefix, w); err != nil {
		return nil, err
	}
	digest, err := block.BindManifest(ctx, be, prefix)
	if err != nil {
		return nil, err
	}
	maxCanonicalBytes, err := writeIndex(prefix+"/series-index", digest)
	if err != nil {
		return nil, err
	}
	if err := backend.SyncPrefix(ctx, be, prefix); err != nil {
		return nil, err
	}
	return openRun(ctx, be, prefix, cache, int64(maxCanonicalBytes), digest)
}
