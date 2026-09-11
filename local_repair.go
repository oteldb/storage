package storage

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/engine"
)

// soleOwnerRepairer is the repair seam of a writable store opened without a cluster layer. It copies
// nothing, since there is no peer; it answers the question repair asks an owner set — is the part
// gone everywhere? — for an owner set of one, whose backend holds the only copy. The engine's
// evidence rule (definitive absence, repeated over consecutive passes) is unchanged; see
// engine/ARCH.md, "A store without a cluster layer is its own complete owner set".
type soleOwnerRepairer struct {
	backend backend.Backend
	prefix  string
}

// FetchWants implements the engines' PartFetcher.
func (r soleOwnerRepairer) FetchWants(ctx context.Context, wants []bucketindex.Want) []engine.FetchResult {
	out := make([]engine.FetchResult, len(wants))

	ix, err := bucketindex.Load(ctx, r.backend, r.prefix+"/"+bucketindex.Object)
	if err != nil {
		for i := range out {
			out[i].Err = err
		}

		return out
	}

	for i := range wants {
		out[i] = r.probe(ctx, ix, wants[i])
	}

	return out
}

// probe concludes absence only when the committed index still states the loss and the backend says
// the part's manifest does not exist. Anything else is either no evidence or a transient failure.
func (r soleOwnerRepairer) probe(ctx context.Context, ix *bucketindex.Index, w bucketindex.Want) engine.FetchResult {
	if !committedLoss(ix, w) {
		return engine.FetchResult{Outcome: bucketindex.WantIncomplete}
	}

	present, err := block.PartPresent(ctx, r.backend, w.Prefix)
	switch {
	case err != nil:
		return engine.FetchResult{Err: err}
	case present:
		return engine.FetchResult{Err: errors.Errorf("part %q is present but does not open", w.Prefix)}
	default:
		return engine.FetchResult{Outcome: bucketindex.WantAbsent}
	}
}

// committedLoss reports whether the index on the backend still records w as owed — a want or a hole
// at its prefix, not tombstoned and not contained in a committed part. A writer this engine has not
// rebased onto can only show up here, and every way it could have explained the absence (merging
// the part into a successor, expiring it) makes this false.
func committedLoss(ix *bucketindex.Index, w bucketindex.Want) bool {
	if _, ok := ix.Satisfying(w); ok {
		return false
	}

	if _, ok := ix.Removals()[w.Prefix]; ok {
		return false
	}

	if _, ok := ix.Wants()[w.Prefix]; ok {
		return true
	}

	return slices.ContainsFunc(ix.Holes(), func(h bucketindex.Entry) bool { return h.Prefix == w.Prefix })
}
