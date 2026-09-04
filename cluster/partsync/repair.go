package partsync

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend/bucketindex"
)

// FetchWant makes local a part that discharges w — the part itself, or the largest part containing
// every block it covered at a higher level — by pulling its objects from whichever peer holds it,
// and returns that part's index entry.
//
// ok is false with a nil error only when every peer *given to it* answered and none of them named
// a satisfying part. A peer that could not be reached is a transient failure and is reported as an
// error, because the difference decides whether the owner may ever conclude the data is gone.
//
// That is absence over the peers asked, not over the shard's owners, and the two coincide only
// when the caller says so: only the caller knows the expected owner set (see
// [bucketindex.WantIncomplete]). Acknowledging a loss on this answer alone would fabricate a hole
// over an owner that is merely restarting.
//
// A peer's own hole never counts as a satisfying part — [bucketindex.Index.Satisfying] admits only
// data-bearing entries — so an acknowledged loss cannot propagate from one owner to the next.
//
// The index is deliberately not installed: the caller commits the entry through its own index
// commit, which is what discharges the want.
func (s *Syncer) FetchWant(
	ctx context.Context, enginePrefix string, peers []string, w bucketindex.Want,
) (bucketindex.Entry, bool, error) {
	if enginePrefix == "" || !ValidKey(enginePrefix) {
		return bucketindex.Entry{}, false, errors.Errorf("invalid engine prefix %q", enginePrefix)
	}

	addr, ent, ok, err := s.findSatisfying(ctx, enginePrefix, peers, w)
	if !ok {
		return bucketindex.Entry{}, false, err
	}

	st, err := s.copyPart(ctx, addr, enginePrefix, ent.Prefix)
	s.account(st, err)

	if err != nil {
		return bucketindex.Entry{}, false, errors.Wrapf(err, "copy part %q from %q", ent.Prefix, addr)
	}

	zctx.From(ctx).Info("partsync: repaired part from peer",
		zap.String("prefix", enginePrefix), zap.String("peer", addr),
		zap.String("want", w.Prefix), zap.String("part", ent.Prefix),
		zap.Int("objects", st.Copied), zap.Int64("bytes", st.CopiedBytes))

	return ent, true, nil
}

// findSatisfying asks every peer for its bucket index and returns the best entry discharging w
// across them. An unreachable or corrupt peer is a transient failure: it is reported once every
// peer has been tried, so a satisfying entry on a reachable peer still wins.
func (s *Syncer) findSatisfying(
	ctx context.Context, enginePrefix string, peers []string, w bucketindex.Want,
) (addr string, best bucketindex.Entry, ok bool, err error) {
	indexKey := enginePrefix + "/" + bucketindex.Object

	var firstErr error

	for _, p := range peers {
		data, ferr := s.client.Fetch(ctx, p, indexKey)
		if ferr != nil {
			if !errors.Is(ferr, ErrNotExist) {
				// A peer with no index at all holds nothing; anything else is a peer we could not
				// ask, which is not evidence of absence.
				firstErr = cmpErr(firstErr, ferr)
			}

			continue
		}

		ix, derr := bucketindex.Decode(data)
		if derr != nil {
			firstErr = cmpErr(firstErr, errors.Wrapf(derr, "decode index from %q", p))

			continue
		}

		cand, found := ix.Satisfying(w)
		if !found {
			continue
		}

		if !ok || betterCandidate(cand, best) {
			addr, best, ok = p, cand, true
		}
	}

	if !ok {
		return "", bucketindex.Entry{}, false, firstErr
	}

	return addr, best, true, nil
}

func cmpErr(first, next error) error {
	if first != nil {
		return first
	}

	return next
}

// betterCandidate orders satisfying entries the way [bucketindex.Index.Satisfying] orders them
// within one index: the widest interval, then the deepest merge level, so a repair fetches the
// fewest objects for the most data.
func betterCandidate(a, b bucketindex.Entry) bool {
	switch {
	case a.Blocks.Len() != b.Blocks.Len():
		return a.Blocks.Len() > b.Blocks.Len()
	case a.Level != b.Level:
		return a.Level > b.Level
	default:
		return a.Prefix < b.Prefix
	}
}

// copyPart mirrors one part's objects from a peer, with the same ordering discipline as a whole
// prefix sync: the manifest lands after the objects it names, so a crash mid-copy leaves an
// unopenable — and therefore uncommitted — part rather than a manifest promising objects that are
// not there.
func (s *Syncer) copyPart(ctx context.Context, addr, enginePrefix, partPrefix string) (Stats, error) {
	var st Stats

	if !ValidKey(partPrefix) {
		return st, errors.Errorf("invalid part prefix %q", partPrefix)
	}

	remote, err := s.client.List(ctx, addr, partPrefix)
	if err != nil {
		return st, errors.Wrap(err, "list peer part")
	}

	if len(remote) == 0 {
		return st, errors.Errorf("peer %q lists no objects for part %q", addr, partPrefix)
	}

	local, err := s.local.List(ctx, partPrefix)
	if err != nil {
		return st, errors.Wrap(err, "list local part")
	}

	have := make(map[string]struct{}, len(local))
	for _, k := range local {
		have[k] = struct{}{}
	}

	indexKey := enginePrefix + "/" + bucketindex.Object
	immutable, manifests, mutable := classifyFetch(remote, enginePrefix, indexKey, have)

	for _, group := range [][]string{immutable, manifests, mutable} {
		for _, k := range group {
			data, err := s.client.Fetch(ctx, addr, k)
			if err != nil {
				// Unlike a whole-prefix sync, a missing object is fatal here: the caller is about to
				// commit an index entry naming this part, and a part it could not fully copy must
				// not become one.
				return st, errors.Wrapf(err, "fetch %q", k)
			}

			if err := s.local.Write(ctx, k, data); err != nil {
				return st, errors.Wrapf(err, "write %q", k)
			}

			st.Copied++
			st.CopiedBytes += int64(len(data))
		}
	}

	st.Synced = st.Copied > 0

	return st, nil
}
