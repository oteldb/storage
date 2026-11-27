package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/internal/partid"
	"github.com/oteldb/storage/internal/seriestable"
	"github.com/oteldb/storage/signal"
)

func seriesReferenceKey(prefix string) string { return prefix + "/series-ref" }

// writeSeriesReference binds the numeric-only marker to the immutable sample manifest.
func writeSeriesReference(ctx context.Context, e *Engine, prefix string) error {
	digest, err := block.BindManifest(ctx, e.cfg.Backend, prefix)
	if err != nil {
		return err
	}
	return backend.WriteDeferred(ctx, e.cfg.Backend, seriesReferenceKey(prefix), encodeSeriesReference(digest))
}

// attachSharedSeries pins the current snapshot for a private part built off-lock.
func (e *Engine) attachSharedSeries(p *part) error {
	if !e.cfg.SeparateSeries {
		return errors.New("engine: numeric part requires shared series roots")
	}
	if p.sharedSeries != nil {
		return nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return attachSharedSeriesFrom(p, e.seriesTable)
}

func attachSharedSeriesFrom(p *part, s *seriesSnapshot) error {
	if p.sharedSeries != nil {
		return nil
	}
	if s == nil {
		return errors.New("engine: missing shared series roots")
	}
	p.sharedSeries = newSeriesBinding(s)
	return nil
}

func (e *Engine) prepareSharedPart(ctx context.Context, p *part) error {
	raw, err := backend.ReadUncached(p.reader.ReaderContext(ctx), p.be, seriesReferenceKey(p.prefix))
	if err != nil {
		return errors.Wrap(err, "read numeric series reference")
	}
	digest := p.reader.ManifestDigest()
	if !validSeriesReference(raw, digest) {
		return errors.New("engine: invalid numeric series reference")
	}

	if err := e.attachSharedSeries(p); err != nil {
		return err
	}
	return p.validateDiskSeriesIDs(ctx)
}

func (e *Engine) ensureSeriesIdentities(ctx context.Context, entries identitySnapshot, reserve func(int64) error) error {
	for {
		err := e.writeSeriesDelta(ctx, entries, reserve)
		if !errors.Is(err, errSeriesSnapshotChanged) {
			return err
		}
		e.cfg.Obs.Flush.DescriptorPlanRetry(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

var errSeriesSnapshotChanged = errors.New("engine: shared series snapshot changed during planning")

func (e *Engine) writeSeriesDelta(ctx context.Context, entries identitySnapshot, reserve func(int64) error) error {
	ctx = obs.WithWritePhase(ctx, "descriptor_delta")
	e.mu.RLock()
	snapshot := e.seriesTable
	snapshot.acquire()
	e.mu.RUnlock()
	defer snapshot.release()
	base := buildbudget.Add(16<<20, int64(len(entries.refs)))
	if snapshot != nil {
		base = buildbudget.Add(base, snapshot.workspace(0))
	}
	if reserve != nil {
		if err := reserve(base); err != nil {
			return err
		}
	}
	beforeVerification := e.VerifiedIdentityCacheStats()
	known, err := snapshot.knownIdentities(ctx, entries, e.verifiedIdentities)
	afterVerification := e.VerifiedIdentityCacheStats()
	e.cfg.Obs.Flush.VerificationCache(ctx, int64(afterVerification.Hits-beforeVerification.Hits),
		int64(afterVerification.Misses-beforeVerification.Misses))
	if err != nil {
		return err
	}

	if backend.StreamsWrites(e.cfg.Backend) {
		return e.writeNativeSeriesDelta(ctx, entries, known, snapshot, base, reserve)
	}
	builder := seriestable.NewBorrowedBuilder(entries.view)
	var largest int64
	for i, ref := range entries.refs {
		if known[i] {
			continue
		}
		single := buildbudget.Add(entries.view.IdentityWorkspace(int(ref)), entries.view.EncodingBytes(int(ref)))
		largest = max(largest, single)
		prospective := buildbudget.Add(base, buildbudget.Add(builder.Bytes(), buildbudget.Mul(single, 32)))
		prospective = buildbudget.Add(prospective, buildbudget.Mul(max(64<<10, largest), 160))
		prospective = buildbudget.Add(prospective, buildbudget.Mul(builder.EncodedBound(), 4))
		if reserve != nil {
			if err := reserve(prospective); err != nil {
				return err
			}
		}
		if err := builder.AddBorrowed(int(ref)); err != nil {
			return err
		}
	}

	if builder.Count() == 0 {
		return nil
	}
	prefix := e.cfg.Prefix + "/s-" + partid.New().String()
	writerBytes, _ := backend.WriterMemory(e.cfg.Backend, prefix+"/series-index")
	needed := buildbudget.Add(base, buildbudget.Add(builder.Bytes(), buildbudget.Mul(builder.EncodedBound(), 4)))
	needed = buildbudget.Add(needed, buildbudget.Add(buildbudget.Mul(writerBytes, 4), buildbudget.Mul(max(64<<10, largest), 160)))
	if reserve != nil {
		if err := reserve(needed); err != nil {
			return err
		}
	}
	return e.installSeriesDelta(entries, snapshot, known, prefix, func() (*seriestable.Run, error) {
		return seriestable.WriteWithIDCache(ctx, e.cfg.Backend, prefix, &builder, e.seriesIDs)
	})
}

func (e *Engine) installSeriesDelta(
	entries identitySnapshot, snapshot *seriesSnapshot, known []bool, prefix string, write func() (*seriestable.Run, error),
) error {
	// No workspace acquisition while owning the writer. A sibling may have added the
	// same descriptors while we planned off-lock.
	e.seriesBuildMu.Lock()
	defer e.seriesBuildMu.Unlock()
	e.mu.RLock()
	stale := snapshot != e.seriesTable
	e.mu.RUnlock()
	if stale {
		return errSeriesSnapshotChanged
	}
	run, err := write()
	if err != nil {
		return errors.Wrap(err, "write shared series delta")
	}
	binding := bucketindex.SeriesRoot{Prefix: prefix, ManifestDigest: run.Digest()}
	e.mu.Lock()
	roots := make([]*seriesRootHandle, 0)
	if e.seriesTable != nil {
		roots = append(roots, e.seriesTable.roots...)
	}
	root := &seriesRootHandle{binding: binding, run: run}
	roots = append(roots, root)
	next := newSeriesSnapshot(roots)
	next.retired = len(e.seriesRetired)
	old := e.seriesTable
	e.seriesTable = next
	old.release()
	e.mu.Unlock()
	e.verifiedIdentities.seed(root.token.Load(), entries, known)
	return nil
}

func (s *seriesSnapshot) knownIdentities(ctx context.Context, entries identitySnapshot, caches ...*verifiedIdentityCache) ([]bool, error) {
	known := make([]bool, len(entries.refs))
	if s == nil {
		return known, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var cache *verifiedIdentityCache
	if len(caches) > 0 {
		cache = caches[0]
	}
	hits := cache.match(s, entries, known)
	remaining := len(known) - hits
	for _, h := range s.roots {
		if remaining == 0 {
			break
		}
		found, err := checkRootIdentitiesCached(ctx, h, entries, known, cache)
		if err != nil {
			return nil, err
		}
		remaining -= found
	}
	return known, nil
}

func checkRootIdentitiesCached(ctx context.Context, root *seriesRootHandle, entries identitySnapshot,
	known []bool, cache *verifiedIdentityCache,
) (found int, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var r *seriestable.Reader
	defer func() {
		if r != nil {
			_ = r.Close()
		}
	}()
	token := root.token.Load()
	var pending [128]signal.SeriesID
	batch := pending[:0]
	for i, ref := range entries.refs {
		if known[i] {
			continue
		}
		if r == nil {
			var err error
			r, err = root.run.NewReader(ctx)
			if err != nil {
				return 0, err
			}
			r.StreamPages()
		}
		id := entries.view.ID(int(ref))
		ok, err := r.HasID(id)
		if err != nil {
			return 0, err
		}
		if !ok {
			continue
		}
		known[i] = true
		found++
		if cache != nil {
			batch = append(batch, id)
			if len(batch) == cap(batch) {
				cache.putIDs(token, batch)
				batch = batch[:0]
			}
		}
	}
	cache.putIDs(token, batch)
	return found, nil
}

func encodeSeriesReference(digest [32]byte) []byte {
	var raw [40]byte
	copy(raw[:4], "SREF")
	copy(raw[4:36], digest[:])
	binary.LittleEndian.PutUint32(raw[36:], crc32.ChecksumIEEE(raw[:36]))
	return raw[:]
}

func validSeriesReference(raw []byte, digest [32]byte) bool {
	return len(raw) == 40 && string(raw[:4]) == "SREF" && bytes.Equal(raw[4:36], digest[:]) &&
		binary.LittleEndian.Uint32(raw[36:]) == crc32.ChecksumIEEE(raw[:36])
}
