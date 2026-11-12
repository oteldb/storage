package seriestable

import (
	"context"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/readbudget"
)

// ErrCorrupt identifies an invalid metadata run.
var ErrCorrupt = errors.Wrap(seriesindex.ErrCorrupt, "series table")

// ErrClosed rejects new operations after a run is retired.
var ErrClosed = errors.New("seriestable: closed run")

// Run retains immutable metadata only. The engine owns catalog and object reclamation.
type Run struct {
	idCache *IDCache
	part    *block.PartReader
	index   *seriesindex.Index
	mu      sync.Mutex
	refs    int
	closed  bool
}

// Open validates a run and optionally checks its catalog-bound manifest digest.
func Open(ctx context.Context, be backend.Backend, prefix string, expected ...[32]byte) (*Run, error) {
	return OpenWithIDCache(ctx, be, prefix, nil, expected...)
}

// OpenWithIDCache retains validated IDs within cache's separate memory budget.
func OpenWithIDCache(ctx context.Context, be backend.Backend, prefix string, cache *IDCache, expected ...[32]byte) (_ *Run, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	budget := readbudget.From(ctx)
	if err := budget.Reserve(512 << 10); err != nil {
		return nil, err
	}
	defer budget.Release(512 << 10)
	if len(expected) > 1 {
		return nil, errors.New("seriestable: multiple expected digests")
	}
	if _, err := requireObject(ctx, be, prefix+"/manifest", 4<<10); err != nil {
		return nil, err
	}
	p, err := block.OpenPart(ctx, be, prefix)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = p.Close()
		}
	}()
	m := p.Manifest()
	if len(m.Columns) != 1 || m.RowCount <= 0 || m.MinTime != 0 || m.MaxTime != 0 || m.Rollup != nil || m.BindingNonce == ([16]byte{}) {
		return nil, errors.Wrap(ErrCorrupt, "metadata manifest schema")
	}
	c := m.Columns[0]
	if c.Name != IDColumn || c.Kind != block.KindInt128 || c.Codec != chunk.CodecID128 || c.Compress != compress.AlgorithmNone ||
		c.Const || c.Blocked || !c.Checked || !c.HasSizing {
		return nil, errors.Wrap(ErrCorrupt, "metadata ID column schema")
	}
	if len(expected) == 1 && expected[0] != p.ManifestDigest() {
		return nil, errors.Wrap(ErrCorrupt, "catalog manifest digest")
	}
	if _, err := requireObject(ctx, be, prefix+"/series-index", 0); err != nil {
		return nil, err
	}
	idx, err := seriesindex.Open(p.ReaderContext(ctx), be, prefix+"/series-index")
	if err != nil {
		return nil, err
	}
	if uint64(idx.Count()) != uint64(m.RowCount) || idx.ParentDigest() != p.ManifestDigest() {
		return nil, errors.Wrap(ErrCorrupt, "index manifest binding")
	}
	r := &Run{part: p, index: idx, idCache: cache}
	if err := r.validate(ctx, be, prefix+"/c/0", c); err != nil {
		return nil, err
	}
	return r, nil
}

func requireObject(ctx context.Context, be backend.Backend, key string, limit int64) (int64, error) {
	n, known, err := backend.NativeSizeOf(ctx, be, key)
	if err != nil {
		return 0, err
	}
	if !known || !backend.RangesNatively(ctx, be, key) {
		return 0, errors.Wrap(readbudget.ErrExceeded, "series table requires native sizes and ranged reads")
	}
	if n <= 0 || limit > 0 && n > limit {
		return 0, errors.Wrap(ErrCorrupt, "object size")
	}
	return n, nil
}

// Count is descriptor cardinality, not active sample cardinality.
func (r *Run) Count() uint32 { return r.index.Count() }

// Digest is the catalog binding for this run.
func (r *Run) Digest() [32]byte { return r.part.ManifestDigest() }

// MaxPageBytes bounds one index cursor's scratch.
func (r *Run) MaxPageBytes() int64 { return r.index.MaxPageBytes() }

// ReaderContext binds reads to this run's retained immutable-file generation.
func (r *Run) ReaderContext(ctx context.Context) context.Context { return r.part.ReaderContext(ctx) }

// Close refuses new readers and retires retained handles after active operations finish.
func (r *Run) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	closePart := r.refs == 0
	r.mu.Unlock()
	if closePart {
		r.idCache.remove(r)
		return r.part.Close()
	}
	return nil
}

func (r *Run) acquire() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	r.refs++
	r.part.AcquireReaders()
	return nil
}

func (r *Run) release() {
	r.part.ReleaseReaders()
	r.mu.Lock()
	r.refs--
	closePart := r.closed && r.refs == 0
	r.mu.Unlock()
	if closePart {
		r.idCache.remove(r)
		_ = r.part.Close()
	}
}
