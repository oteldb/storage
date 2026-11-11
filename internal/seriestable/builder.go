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

// EncodedBound bounds buffered output before compression.
func (b *Builder) EncodedBound() int64 { return b.index.EncodedBound() + int64(b.Count())*32 + 4096 }

// Write writes and syncs an unused, caller-owned prefix; it does not publish a catalog.
func Write(ctx context.Context, be backend.Backend, prefix string, b *Builder) (*Run, error) {
	if b == nil || b.Count() == 0 {
		return nil, errors.New("seriestable: empty run")
	}
	w := block.NewStreamWriterTo(ctx, be, prefix, block.WithSizingStats())
	defer w.Abort()
	if err := w.AddColumn(block.Column{Name: IDColumn, Kind: block.KindInt128}); err != nil {
		return nil, err
	}
	view := b.index.View()
	for i := range b.Count() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := view.ID(i)
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
	b.index.BindParent(digest)
	if err := b.index.WriteObject(ctx, be, prefix+"/series-index"); err != nil {
		return nil, err
	}
	if err := backend.SyncPrefix(ctx, be, prefix); err != nil {
		return nil, err
	}
	return Open(ctx, be, prefix, digest)
}
