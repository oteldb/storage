package engine

import (
	"context"
	"encoding/binary"
	"path"
	"strconv"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/signal"
)

// The engine maintains a [bucketindex] alongside its parts so the part set is durable and a
// node can reconstruct it from the object store without local state (the object-store-native
// read path). The index is rewritten on every flush and merge; a single-node engine
// overwrites it (multi-writer commits would use backend CAS, a later milestone).

// indexKey is the backend key of this engine's bucket index.
func (e *Engine) indexKey() string {
	return e.cfg.Prefix + "/" + bucketindex.Object
}

// seriesKey is the backend key of this engine's persisted identity index (the series
// labels). Parts store only series ids; the labels needed to resolve matchers and to label
// fetched batches live here, so a stateless reader can rebuild the postings/series index
// without the (local) WAL.
func (e *Engine) seriesKey() string { return e.cfg.Prefix + "/series.bin" }

// updateIndexLocked rewrites the bucket index to match the engine's current parts. It is a
// no-op for a head-only engine (no backend). Caller holds e.mu.
func (e *Engine) updateIndexLocked(ctx context.Context) error {
	if e.cfg.Backend == nil {
		return nil
	}

	ix := &bucketindex.Index{}
	for _, p := range e.parts {
		ix.Add(bucketindex.Entry{Prefix: p.prefix, MinTime: p.minTime, MaxTime: p.maxTime})
	}

	if err := ix.Save(ctx, e.cfg.Backend, e.indexKey()); err != nil {
		return errors.Wrap(err, "save bucket index")
	}

	return nil
}

// LoadParts reconstructs the engine's durable state from the object store: the part set from
// the bucket index, and the series identity index (postings + labels) from the persisted
// identity object. It is how a fresh engine over an existing prefix serves reads with no
// in-memory state carried over from the writer (the stateless read path); typically called
// once after [New] during recovery. WAL [Engine.Replay] is complementary — it restores the
// unflushed head samples — but is not required to query flushed data.
//
// It replaces any current parts and advances the part sequence past the highest existing
// part. A head-only engine (no backend) is a no-op. It assumes this node owns the prefix — it
// sweeps the part objects the index does not name (see [Engine.sweepOrphansLocked]).
func (e *Engine) LoadParts(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.loadPartsLocked(ctx, true)
}

// loadPartsLocked is [Engine.LoadParts] with the orphan sweep made optional: a replica shares the
// prefix with the owner, whose in-flight (not yet committed) part must not be deleted underneath it.
// Caller holds e.mu.
func (e *Engine) loadPartsLocked(ctx context.Context, sweep bool) error {
	if e.cfg.Backend == nil {
		return nil
	}

	ix, err := bucketindex.Load(ctx, e.cfg.Backend, e.indexKey())
	if err != nil {
		return errors.Wrap(err, "load bucket index")
	}

	parts := make([]*part, 0, len(ix.Entries))
	maxSeq := -1

	for _, ent := range ix.Entries {
		p, err := openPart(ctx, e.cfg.Backend, ent.Prefix)
		if err != nil {
			return errors.Wrapf(err, "open part %q", ent.Prefix)
		}

		p.minTime, p.maxTime = ent.MinTime, ent.MaxTime
		parts = append(parts, p)

		if s := seqOfPrefix(ent.Prefix); s > maxSeq {
			maxSeq = s
		}
	}

	e.parts = parts
	e.nextSeq = maxSeq + 1

	if sweep {
		next, err := e.sweepOrphansLocked(ctx, maxSeq)
		if err != nil {
			return err
		}

		e.nextSeq = next
	}

	return e.loadSeriesIndexLocked(ctx)
}

// RefreshReplica brings a replica node's view up to date with the shared object store: it
// reconstructs the flushed parts from the bucket index and trims its head to the
// still-unflushed window — samples a primary has already flushed (covered by a part) are
// dropped, bounding replica memory. With no shared store (this node cannot see the parts), it
// is a safe no-op: nothing loads, so nothing is trimmed.
func (e *Engine) RefreshReplica(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.loadPartsLocked(ctx, false); err != nil {
		return err
	}

	if len(e.parts) == 0 {
		return nil
	}

	maxT := minInt64
	covered := make(map[signal.SeriesID]struct{})

	for _, p := range e.parts {
		if p.maxTime > maxT {
			maxT = p.maxTime
		}

		// The trim must only touch series the parts actually hold: a head series that is not
		// flushed anywhere (a late-registered series whose timestamps overlap already-flushed
		// data — ordinary backfill) would otherwise lose its only replicated copies, leaving the
		// primary as the sole holder of quorum-acked samples until its next flush.
		if err := p.index.forEachID(ctx, func(id signal.SeriesID) { covered[id] = struct{}{} }); err != nil {
			return errors.Wrapf(err, "enumerate series of part %q", p.prefix)
		}
	}

	e.head.trimBelowCovered(maxT, covered)

	return nil
}

// writeSeriesIndexLocked persists the head's full series identity set so a stateless reader
// can rebuild the postings/series index. Written on flush; identities outlive a flush, so a full
// rewrite is correct. Caller holds e.mu.
//
// Between prunes the set only grows, so an unchanged identity count means the persisted object is
// already byte-identical and the write is skipped: a steady state that registers no new series costs
// nothing, instead of re-serializing and rewriting every identity every flush interval. An identity
// prune ([Engine.PruneIdentities]) is the one path that removes entries, and a set that shrank and
// regrew to the same count is not the same set — so it forces the next write by clearing
// seriesWritten rather than leaving the count to imply it. The object is written uncached — it is
// rewritten at flush frequency and read only on recovery, so caching it only evicts part data.
//
// TODO: this rewrites every identity when the set does change — fine single-node, but a per-part
// identity object (incremental) is the scale-out form.
func (e *Engine) writeSeriesIndexLocked(ctx context.Context) error {
	if e.cfg.Backend == nil {
		return nil
	}

	n := e.head.series.Len()
	if n == e.seriesWritten {
		return nil
	}

	buf := encodeSeriesSet(make([]byte, 0, seriesSetSizeHint(e.seriesBytes, e.seriesWritten, n)), e.head.series)
	if err := backend.WriteUncached(ctx, e.cfg.Backend, e.seriesKey(), buf); err != nil {
		return errors.Wrap(err, "write series index")
	}

	e.seriesWritten, e.seriesBytes = n, len(buf)

	return nil
}

// seriesSetSizeHint estimates the encoded size of an identity set of n series from the previous
// encode (prevBytes over prevCount), with a margin so the encode does not reallocate near the end.
// Without it the buffer reaches its final size by append-doubling, whose transient peak is a
// multiple of a result that is hundreds of MiB at real cardinality.
func seriesSetSizeHint(prevBytes, prevCount, n int) int {
	if prevBytes <= 0 || prevCount <= 0 {
		return 0
	}

	est := prevBytes / prevCount * n

	return est + est/16 + 64
}

// loadSeriesIndexLocked rebuilds the head's series/postings index from the persisted identity
// object, registering each identity. A missing object (nothing flushed yet) is a no-op.
// Caller holds e.mu.
func (e *Engine) loadSeriesIndexLocked(ctx context.Context) error {
	data, err := backend.ReadUncached(ctx, e.cfg.Backend, e.seriesKey())
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil
		}

		return errors.Wrap(err, "read series index")
	}

	var count int

	if err := decodeSeriesSet(data, func(s signal.Series) {
		e.head.registerSeries(s)
		count++
	}); err != nil {
		return errors.Wrap(err, "decode series index")
	}

	// Only when the head holds exactly what the object holds is the object current — a replica
	// refreshing mid-ingest already has identities of its own, which still need persisting.
	if e.head.series.Len() == count {
		e.seriesWritten, e.seriesBytes = count, len(data)
	}

	return nil
}

// encodeSeriesSet appends to dst every identity in ix as a count followed by length-delimited
// [signal.Series.AppendHashInput] records (the reversible wire form read by
// [signal.DecodeSeries]). One scratch buffer is reused across identities: encoding each into a
// fresh one made the whole set's worth of garbage per call.
func encodeSeriesSet(dst []byte, ix *series.Index) []byte {
	buf := binary.AppendUvarint(dst, uint64(ix.Len()))

	var enc []byte

	ix.ForEach(func(_ signal.SeriesID, s signal.Series) {
		enc = s.AppendHashInput(enc[:0])
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)
	})

	return buf
}

// decodeSeriesSet parses encodeSeriesSet output, calling fn for each identity. It is
// defensive against truncated input.
func decodeSeriesSet(data []byte, fn func(signal.Series)) error {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.Wrap(signal.ErrMalformed, "series count")
	}
	data = data[n:]

	for range count {
		l, n := binary.Uvarint(data)
		if n <= 0 || l > uint64(len(data)-n) {
			return errors.Wrap(signal.ErrMalformed, "series length")
		}
		data = data[n:]

		s, _, err := signal.DecodeSeries(data[:l])
		if err != nil {
			return errors.Wrap(err, "decode series")
		}

		fn(s)
		data = data[l:]
	}

	return nil
}

// seqOfPrefix parses the trailing sequence number of a part prefix
// ("{enginePrefix}/{seq}"), or -1 if it is not numeric.
func seqOfPrefix(prefix string) int {
	n, err := strconv.Atoi(path.Base(prefix))
	if err != nil {
		return -1
	}

	return n
}

// colsTimeRange returns the inclusive min/max timestamp across cols (which has ≥ 1 sample
// when a part is written).
func colsTimeRange(cols *flushColumns) (minTime, maxTime int64) {
	minTime, maxTime = maxInt64, minInt64
	for _, t := range cols.ts {
		if t < minTime {
			minTime = t
		}

		if t > maxTime {
			maxTime = t
		}
	}

	return minTime, maxTime
}
