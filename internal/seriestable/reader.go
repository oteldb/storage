package seriestable

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
)

// Reader owns bounded page scratch for one operation. It is not concurrent-safe.
type Reader struct {
	run    *Run
	ctx    context.Context //nolint:containedctx // Reader owns operation-local I/O and scratch.
	raw    *seriesindex.RawReader
	budget *readbudget.Budget
	bytes  int64
}

// NewReader reserves scratch and pins the run until Close.
func (r *Run) NewReader(ctx context.Context) (*Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n := 2*r.MaxPageBytes() + 4096
	budget := readbudget.From(ctx)
	if err := budget.Reserve(n); err != nil {
		return nil, err
	}
	if err := r.acquire(); err != nil {
		budget.Release(n)
		return nil, err
	}
	ctx = r.ReaderContext(ctx)
	return &Reader{run: r, ctx: ctx, raw: r.index.RawReader(ctx), budget: budget, bytes: n}, nil
}

// LookupRaw returns a borrowed descriptor, valid until the next lookup or Close.
func (r *Reader) LookupRaw(id signal.SeriesID) ([]byte, bool, error) {
	if r.run == nil {
		return nil, false, ErrClosed
	}
	if err := r.ctx.Err(); err != nil {
		return nil, false, err
	}
	raw, meta, found, err := r.raw.Lookup(id)
	if err != nil {
		return nil, false, err
	}
	if found && meta != (seriesindex.Metadata{}) {
		return nil, false, errors.Wrap(ErrCorrupt, "descriptor sample metadata")
	}
	return raw, found, nil
}

// Close releases owned scratch and its run pin.
func (r *Reader) Close() error {
	if r == nil || r.run == nil {
		return nil
	}
	r.run.release()
	r.budget.Release(r.bytes)
	r.run, r.raw, r.ctx = nil, nil, nil
	return nil
}

// Iterator yields sorted canonical IDs and borrowed descriptors until Close.
type Iterator struct {
	run    *Run
	refs   postings.Postings
	cursor *seriesindex.SeriesCursor
	budget *readbudget.Budget
	bytes  int64
	id     signal.SeriesID
	raw    []byte
	err    error
	ctx    context.Context //nolint:containedctx // Iterator owns operation-local I/O and scratch.
}

// Iterate walks every descriptor in canonical-ID order.
func (r *Run) Iterate(ctx context.Context) (*Iterator, error) { return r.Resolve(ctx, nil) }

// Resolve yields canonical identities selected by typed label predicates.
func (r *Run) Resolve(ctx context.Context, matchers []fetch.Matcher) (*Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	count := int64(max(len(matchers), 1))
	n := (count+3)*r.MaxPageBytes() + count*((int64(r.Count())+63)/64)*8 + 4096
	budget := readbudget.From(ctx)
	if err := budget.Reserve(n); err != nil {
		return nil, err
	}
	if err := r.acquire(); err != nil {
		budget.Release(n)
		return nil, err
	}
	ctx = r.ReaderContext(ctx)
	refs, err := r.index.Resolve(ctx, matchers)
	if err != nil {
		r.release()
		budget.Release(n)
		return nil, err
	}
	return &Iterator{run: r, refs: refs, cursor: r.index.SeriesCursor(ctx), budget: budget, bytes: n, ctx: ctx}, nil
}

// Resolve creates an independently owned iterator using this operation's context.
func (r *Reader) Resolve(matchers []fetch.Matcher) (*Iterator, error) {
	if r.run == nil {
		return nil, ErrClosed
	}
	return r.run.Resolve(r.ctx, matchers)
}

// Next advances the cursor; Raw expires at every call.
func (it *Iterator) Next() bool {
	it.raw = nil
	if it.run == nil || it.err != nil {
		return false
	}
	if it.err = it.ctx.Err(); it.err != nil {
		return false
	}
	if !it.refs.Next() {
		it.err = it.refs.Err()
		return false
	}
	ref := it.refs.At()
	if ref.Hi != 0 || ref.Lo >= uint64(it.run.Count()) {
		it.err = errors.Wrap(ErrCorrupt, "descriptor reference")
		return false
	}
	var meta seriesindex.Metadata
	it.id, it.raw, meta, it.err = it.cursor.At(uint32(ref.Lo))
	if it.err == nil && meta != (seriesindex.Metadata{}) {
		it.err = errors.Wrap(ErrCorrupt, "descriptor sample metadata")
	}
	return it.err == nil
}

// ID returns the current canonical identity hash.
func (it *Iterator) ID() signal.SeriesID { return it.id }

// Raw returns a descriptor borrowed until the next Next or Close.
func (it *Iterator) Raw() []byte { return it.raw }

// Err reports the iteration error.
func (it *Iterator) Err() error { return it.err }

// Close releases scratch and the run pin.
func (it *Iterator) Close() error {
	if it == nil || it.run == nil {
		return nil
	}
	it.run.release()
	it.budget.Release(it.bytes)
	it.run, it.refs, it.cursor, it.raw, it.ctx = nil, nil, nil, nil, nil
	return nil
}
