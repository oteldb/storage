package partsync

import (
	"context"
	"slices"
	"sync"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/oteldb/storage/backend/bucketindex"
)

// repairCopyConcurrency bounds how many distinct parts one repair batch copies at once. A copy
// moves a whole part over the network, so the batch is deliberately narrow: repair is anti-entropy
// and runs again next cycle.
const repairCopyConcurrency = 2

// WantResult is one want's outcome in a repair batch, positionally matched to the want.
//
// OK false with a nil Err is definitive absence over the peers asked; a non-nil Err is a transient
// failure and says nothing about whether the part exists.
type WantResult struct {
	// Entry is the index entry of the part that came across, valid only when OK.
	Entry bucketindex.Entry
	// OK reports that a satisfying part is now local.
	OK bool
	// Err is a transient failure — a peer that could not be asked, or a copy that did not finish.
	Err error
}

// FetchWants makes local a part discharging each want — the part itself, or the largest part
// containing every block it covered at a higher level — by pulling its objects from whichever peer
// holds it, and returns one result per want in the same order.
//
// The whole cycle's wants are answered together because that is what bounds the work: each peer's
// bucket index is read exactly once for the batch, and a part discharging several wants is copied
// once. Nothing survives the call — a peer index cached across cycles would let repair act on a
// stale view of what peers hold.
//
// Peers are consulted in a shuffled order, so equal candidates spread the repair load instead of
// every recovering node converging on whichever peer sorts first.
//
// OK is false with a nil error only when every peer *given to it* answered and none of them named
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
// The index is deliberately not installed: the caller commits the entries through its own index
// commit, which is what discharges the wants.
func (s *Syncer) FetchWants(
	ctx context.Context, enginePrefix string, peers []string, wants []bucketindex.Want,
) []WantResult {
	out := make([]WantResult, len(wants))

	if enginePrefix == "" || !ValidKey(enginePrefix) {
		err := errors.Errorf("invalid engine prefix %q", enginePrefix)
		for i := range out {
			out[i].Err = err
		}

		return out
	}

	if len(wants) == 0 {
		return out
	}

	views, indexErr := s.peerIndexes(ctx, enginePrefix, peers)

	tasks := make(map[string]*copyTask)

	for i := range wants {
		addr, ent, ok := selectSatisfying(views, wants[i])
		if !ok {
			out[i].Err = indexErr

			continue
		}

		t, seen := tasks[ent.Prefix]
		if !seen {
			t = &copyTask{addr: addr, entry: ent}
			tasks[ent.Prefix] = t
		}

		t.wants = append(t.wants, i)
	}

	s.runCopies(ctx, enginePrefix, tasks)

	for _, t := range tasks {
		for _, i := range t.wants {
			if t.err != nil {
				out[i].Err = t.err

				continue
			}

			out[i].Entry, out[i].OK = t.entry, true
		}
	}

	return out
}

// FetchWant is [Syncer.FetchWants] for a single want.
func (s *Syncer) FetchWant(
	ctx context.Context, enginePrefix string, peers []string, w bucketindex.Want,
) (bucketindex.Entry, bool, error) {
	r := s.FetchWants(ctx, enginePrefix, peers, []bucketindex.Want{w})[0]

	return r.Entry, r.OK, r.Err
}

// copyTask is one part to copy, and the indices of the wants it discharges.
type copyTask struct {
	addr  string
	entry bucketindex.Entry
	wants []int

	err error
}

// runCopies copies each distinct part once, bounded by [repairCopyConcurrency].
func (s *Syncer) runCopies(ctx context.Context, enginePrefix string, tasks map[string]*copyTask) {
	var wg sync.WaitGroup

	sem := make(chan struct{}, repairCopyConcurrency)

	for _, t := range tasks {
		sem <- struct{}{}

		wg.Go(func() {
			defer func() { <-sem }()

			st, err := s.copyPart(ctx, t.addr, enginePrefix, t.entry.Prefix)
			s.account(st, err)

			if err != nil {
				t.err = errors.Wrapf(err, "copy part %q from %q", t.entry.Prefix, t.addr)

				return
			}

			zctx.From(ctx).Info("partsync: repaired part from peer",
				zap.String("prefix", enginePrefix), zap.String("peer", t.addr),
				zap.String("part", t.entry.Prefix),
				zap.Int("objects", st.Copied), zap.Int64("bytes", st.CopiedBytes))
		})
	}

	wg.Wait()
}

// peerView is one peer's bucket index as of this batch, or the reason it could not be read.
type peerView struct {
	addr string
	ix   *bucketindex.Index
}

// peerIndexes reads every peer's bucket index concurrently, once for the batch, and returns the
// readable ones in shuffled peer order together with the first failure among the rest.
//
// Results are collected into a slice aligned to the peer order rather than taken as they arrive:
// selection must not depend on which peer answers first, or "prefer the most satisfying part"
// stops holding. Nothing cancels a sibling fetch either — a canceled fetch would be reported as a
// failure this node did not actually suffer, and every failure here is load-bearing evidence that
// absence is *not* definitive.
func (s *Syncer) peerIndexes(
	ctx context.Context, enginePrefix string, peers []string,
) (views []peerView, firstErr error) {
	indexKey := enginePrefix + "/" + bucketindex.Object
	order := s.shuffled(peers)

	type result struct {
		ix  *bucketindex.Index
		err error
	}

	results := make([]result, len(order))

	var wg sync.WaitGroup

	for i, p := range order {
		wg.Go(func() {
			data, err := s.client.Fetch(ctx, p, indexKey)
			if err != nil {
				// A peer with no index at all holds nothing; anything else is a peer we could not
				// ask, which is not evidence of absence.
				if !errors.Is(err, ErrNotExist) {
					results[i].err = err
				}

				return
			}

			ix, err := bucketindex.Decode(data)
			if err != nil {
				results[i].err = errors.Wrapf(err, "decode index from %q", p)

				return
			}

			results[i].ix = ix
		})
	}

	wg.Wait()

	views = make([]peerView, 0, len(order))

	for i, r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}

		if r.ix != nil {
			views = append(views, peerView{addr: order[i], ix: r.ix})
		}
	}

	return views, firstErr
}

// shuffled returns peers in a random order, so two nodes repairing the same want from the same set
// of equally good peers do not both pull from the first one in list order.
func (s *Syncer) shuffled(peers []string) []string {
	out := slices.Clone(peers)

	s.rndMu.Lock()
	defer s.rndMu.Unlock()

	s.rnd.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })

	return out
}

// selectSatisfying picks the best entry discharging w across the peers' indexes. Peers are
// consulted in the given (shuffled) order and [betterCandidate] is strict, so equal candidates
// resolve to whichever peer the shuffle put first rather than to a fixed one.
func selectSatisfying(views []peerView, w bucketindex.Want) (addr string, best bucketindex.Entry, ok bool) {
	for _, v := range views {
		cand, found := v.ix.Satisfying(w)
		if !found {
			continue
		}

		if !ok || betterCandidate(cand, best) {
			addr, best, ok = v.addr, cand, true
		}
	}

	return addr, best, ok
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
