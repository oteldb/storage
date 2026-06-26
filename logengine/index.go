package logengine

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

// The logs engine, like the metrics engine, keeps a [bucketindex] of its parts and a persisted
// stream-identity object alongside them, so a stateless node reconstructs both the part set and
// the postings/labels from the object store with no local state (the object-store-native read
// path). Both are rewritten on every flush and merge.

func (e *Engine) indexKey() string  { return e.cfg.Prefix + "/" + bucketindex.Object }
func (e *Engine) streamKey() string { return e.cfg.Prefix + "/streams.bin" }

// updateIndexLocked rewrites the bucket index to match the engine's current parts. No-op for a
// head-only engine. Caller holds e.mu.
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

// LoadParts reconstructs the engine's durable state from the object store: the part set from the
// bucket index and the stream identity index (postings + labels) from the persisted object. A
// head-only engine (no backend) is a no-op. Replaces current parts and advances the sequence.
func (e *Engine) LoadParts(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

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

	return e.loadStreamIndexLocked(ctx)
}

// RefreshReplica brings a replica node's view up to date with the shared object store: it
// reconstructs the flushed parts and trims its head to the still-unflushed window. With no shared
// store, it is a safe no-op.
func (e *Engine) RefreshReplica(ctx context.Context) error {
	if err := e.LoadParts(ctx); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.parts) == 0 {
		return nil
	}

	maxT := minInt64
	for _, p := range e.parts {
		if p.maxTime > maxT {
			maxT = p.maxTime
		}
	}

	e.head.trimBelow(maxT)

	return nil
}

// writeStreamIndexLocked persists the head's full stream identity set so a stateless reader can
// rebuild the postings/series index. Written on flush; the set only grows. Caller holds e.mu.
func (e *Engine) writeStreamIndexLocked(ctx context.Context) error {
	if e.cfg.Backend == nil {
		return nil
	}

	if err := e.cfg.Backend.Write(ctx, e.streamKey(), encodeSeriesSet(e.head.series)); err != nil {
		return errors.Wrap(err, "write stream index")
	}

	return nil
}

// loadStreamIndexLocked rebuilds the head's series/postings index from the persisted object. A
// missing object (nothing flushed yet) is a no-op. Caller holds e.mu.
func (e *Engine) loadStreamIndexLocked(ctx context.Context) error {
	data, err := e.cfg.Backend.Read(ctx, e.streamKey())
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return nil
		}

		return errors.Wrap(err, "read stream index")
	}

	if err := decodeSeriesSet(data, e.head.registerStream); err != nil {
		return errors.Wrap(err, "decode stream index")
	}

	return nil
}

// encodeSeriesSet serializes every identity in ix as a count followed by length-delimited
// reversible [signal.Series] records (read back by [signal.DecodeSeries]).
func encodeSeriesSet(ix *series.Index) []byte {
	buf := binary.AppendUvarint(nil, uint64(ix.Len()))
	ix.ForEach(func(_ signal.SeriesID, s signal.Series) {
		enc := s.AppendHashInput(nil)
		buf = binary.AppendUvarint(buf, uint64(len(enc)))
		buf = append(buf, enc...)
	})

	return buf
}

// decodeSeriesSet parses encodeSeriesSet output, calling fn for each identity. Defensive against
// truncated input.
func decodeSeriesSet(data []byte, fn func(signal.Series)) error {
	count, n := binary.Uvarint(data)
	if n <= 0 {
		return errors.Wrap(signal.ErrMalformed, "stream count")
	}

	data = data[n:]

	for range count {
		l, n := binary.Uvarint(data)
		if n <= 0 || l > uint64(len(data)-n) {
			return errors.Wrap(signal.ErrMalformed, "stream length")
		}

		data = data[n:]

		s, _, err := signal.DecodeSeries(data[:l])
		if err != nil {
			return errors.Wrap(err, "decode stream")
		}

		fn(s)
		data = data[l:]
	}

	return nil
}

// seqOfPrefix parses the trailing sequence number of a part prefix, or -1 if not numeric.
func seqOfPrefix(prefix string) int {
	n, err := strconv.Atoi(path.Base(prefix))
	if err != nil {
		return -1
	}

	return n
}
