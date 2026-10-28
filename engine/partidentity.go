package engine

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/index/identity"
	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/obs"
	"github.com/oteldb/storage/signal"
)

// Identity is **part-scoped**: each part carries the identities of the series it holds, in an
// `{partPrefix}/identity` object written with the part's other objects and deleted with them. The
// engine's resident index is then the union over its live parts (plus the unflushed head), so
// retention is self-cleaning — dropping a part drops its identities — and a flush costs the
// identities it wrote rather than every identity the tenant has ever had.
//
// The whole-set `series.bin` this replaces had neither property: it was rewritten from the whole index
// whenever the set changed and never shrank with the data. It is still *read* when present, so a
// prefix written by an older build keeps resolving, and deleted once every live part carries its
// own identities.

// identityKey is the backend key of a part's identity object (deleted with the part, since
// deletePart lists and removes everything under the prefix).
func identityKey(prefix string) string { return prefix + "/identity" }

// identitySet is a private snapshot of id → identity, taken under the engine lock and handed to the
// off-lock part write. A map rather than the index itself: the index keeps being mutated by ingest
// while a flush or merge is writing.
type identitySet struct {
	view series.View
	refs map[signal.SeriesID]uint32
}

type identitySnapshot struct {
	view series.View
	refs []uint32
}

// entriesFor returns the identity records for the distinct series of a part's (sorted) series
// column. An id the snapshot does not hold is skipped — it cannot be encoded, and inventing one
// would write an identity that hashes to something else.
func (s identitySet) entriesFor(col []chunk.U128) identitySnapshot {
	out := identitySnapshot{view: s.view, refs: make([]uint32, 0, len(s.refs))}
	for i := range col {
		if i > 0 && col[i] == col[i-1] {
			continue
		}
		if ref, ok := s.refs[signal.SeriesID(col[i])]; ok {
			out.refs = append(out.refs, ref)
		}
	}
	return out
}

// writeIdentity writes a part's identity object. It is written before the part becomes visible (the
// bucket index is the commit point), so a part is never readable without the identities naming its
// rows.
func writeIdentity(ctx context.Context, b backend.Backend, prefix string, entries identitySnapshot) error {
	// Written uncached: read on recovery and by a replica adopting the part, never on the query
	// path, so a cache entry would only evict part data.
	data := identity.EncodeSorted(nil, len(entries.refs), func(i int, dst signal.Attributes) series.Entry {
		ref := int(entries.refs[i])
		return series.Entry{ID: entries.view.ID(ref), Series: entries.view.At(ref, dst)}
	})
	if err := backend.WriteUncachedDeferred(ctx, b, identityKey(prefix), data); err != nil {
		return errors.Wrapf(err, "write identity object %q", prefix)
	}

	return nil
}

// identitiesOf snapshots the identities of the given series ids from the resident index. Caller
// holds e.mu (at least for reading).
func (h *head) identitiesOf(ids map[signal.SeriesID]*sampleBuf) identitySet {
	out := identitySet{view: h.series.View(), refs: make(map[signal.SeriesID]uint32, len(ids))}

	for id := range ids {
		if ref, ok := h.series.Ref(id); ok {
			out.refs[id] = ref
		}
	}

	return out
}

// identitiesForColumn snapshots, under the read lock, the identities of the distinct series in a
// part's sorted series column — what a merge needs to write its output part's identity object.
func (e *Engine) identitiesForColumn(col []chunk.U128) identitySet {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := identitySet{view: e.head.series.View(), refs: make(map[signal.SeriesID]uint32)}

	for i := range col {
		if i > 0 && col[i] == col[i-1] {
			continue
		}

		id := signal.SeriesID(col[i])
		if ref, ok := e.head.series.Ref(id); ok {
			out.refs[id] = ref
		}
	}

	return out
}

// loadIdentitiesLocked rebuilds the resident identity index from the durable state: each live
// part's identity object, plus the legacy whole-set object when the prefix still has one. It
// returns whether every part carried its own identities. Caller holds e.mu.
func (e *Engine) loadIdentitiesLocked(ctx context.Context, parts []*part) (bool, error) {
	// The legacy object first: on a prefix written by an older build it is the only place a part's
	// identities exist. It is a superset (it never shrank), so loading it before the parts costs
	// nothing but dead entries, which the identity prune reclaims.
	if err := e.loadSeriesIndexLocked(ctx); err != nil {
		return false, err
	}

	complete := true

	for _, p := range parts {
		// A reused handle already registered its identities, and the object is immutable, so the
		// uncached read is not repeated: the prune only ever drops identities no live part holds,
		// so nothing it did can have unregistered these.
		if p.identityLoaded {
			complete = complete && p.identityPresent

			continue
		}

		ok, err := e.registerPartIdentitiesLocked(ctx, p.prefix)
		if err != nil {
			return false, err
		}

		p.identityLoaded, p.identityPresent = true, ok

		if !ok {
			complete = false
		}
	}

	return complete, nil
}

// registerPartIdentitiesLocked registers the series of the part at prefix into the head's identity
// index, so a matcher can resolve the rows it holds. It reports whether the part carried its own
// identity object: a missing one is a part written before identity was part-scoped (its identities
// come from the legacy whole-set object), a corrupt one leaves its rows unresolvable until a merge
// rewrites the part — neither is fatal. Caller holds e.mu.
func (e *Engine) registerPartIdentitiesLocked(ctx context.Context, prefix string) (bool, error) {
	data, err := backend.ReadUncached(ctx, e.cfg.Backend, identityKey(prefix))
	if err != nil {
		if errors.Is(err, backend.ErrNotExist) {
			return false, nil
		}

		return false, errors.Wrapf(err, "read identity object %q", prefix)
	}

	if err := identity.Decode(data, func(_ signal.SeriesID, s signal.Series) error {
		// registerSeries interns the identity, so the aliased decode buffer is not retained.
		e.head.registerSeries(s)

		return nil
	}); err != nil {
		zctx.From(ctx).Error("corrupt part identity object",
			zap.String("part", prefix), zap.Error(err))
		e.cfg.Obs.Corruption.Detected(ctx, "part_identity", obs.CorruptTolerated)

		return false, nil
	}

	return true, nil
}
