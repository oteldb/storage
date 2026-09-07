package partsync

import (
	"context"
	"math"
	"path"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/bucketindex"
)

// manifestName is the object a part writer lands last, so its presence under a prefix is what
// makes a copy complete.
const manifestName = "manifest"

// held is the local index's parts a peer's index does not name while this node still holds them —
// the local index names the part and its manifest is on disk — split by what the peer said about
// them. The peer's index is a copy of whichever owner's index last superseded it and says nothing
// about this disk, so neither a claim of loss nor a silent gap removes a part that is here.
type held struct {
	// claimed are the parts the peer reports lost, as a want or as a hole. The claim is carried
	// back beside the entry as a want: it is what the owner's repair pass discharges with a commit.
	claimed []bucketindex.Entry
	// omitted are the parts the peer neither names nor accounts for — no tombstone, no successor
	// containing them. It stated nothing, so nothing is carried beside the entry; keeping the entry
	// is the whole of it.
	omitted []bucketindex.Entry
}

func (h held) len() int { return len(h.claimed) + len(h.omitted) }

// heldEntries classifies the local index's parts against the peer's.
func (s *Syncer) heldEntries(
	ctx context.Context, localIndex *bucketindex.Index, acct peerAccount,
) (held, error) {
	var out held

	for i := range localIndex.Entries {
		e := &localIndex.Entries[i]
		if e.Hole {
			continue
		}

		_, claimed := acct.claimed[e.Prefix]
		if !claimed && acct.accountsFor(*e) {
			continue
		}

		ok, err := s.holdsManifest(ctx, e.Prefix)
		if err != nil {
			return held{}, err
		}

		if !ok {
			continue
		}

		if claimed {
			out.claimed = append(out.claimed, *e)
		} else {
			out.omitted = append(out.omitted, *e)
		}
	}

	return out, nil
}

// owedEntries is [Syncer.heldEntries] with the roles swapped: the parts the *peer's* index names
// live that the local index does not account for. It is the direction the claim path cannot reach —
// a part the local node never indexed can only ever be another node's `omitted`, and an owner
// cannot report losing something absent from its index, so nothing ever states the obligation.
//
// Silence is weaker evidence here than it is for a deletion, so two things bound it. An index that
// predates tombstones states nothing to reason from. And below the oldest data the local index
// still names, a retention drop whose tombstone has aged out ([bucketindex.MaxRemovals]) is
// indistinguishable from a part that never arrived — re-fetching one would resurrect deleted data.
func owedEntries(peer *bucketindex.Index, local peerAccount, horizon int64) []bucketindex.Entry {
	if !local.stated {
		return nil
	}

	var out []bucketindex.Entry

	for i := range peer.Entries {
		e := &peer.Entries[i]
		if e.Hole || local.accountsFor(*e) || e.MaxTime < horizon {
			continue
		}

		out = append(out, *e)
	}

	return out
}

// retentionHorizon is the oldest timestamp the index still names. An index naming nothing returns
// [math.MaxInt64], which owes nothing: a node with no parts of its own adopts a peer's index
// wholesale rather than repairing into it part by part.
func retentionHorizon(ix *bucketindex.Index) int64 {
	horizon := int64(math.MaxInt64)

	for i := range ix.Entries {
		if e := &ix.Entries[i]; !e.Hole && e.MinTime < horizon {
			horizon = e.MinTime
		}
	}

	return horizon
}

// peerAccount is what a peer's index states about the parts it does not hold.
type peerAccount struct {
	// present are the prefixes the index names at all, hole or not.
	present map[string]struct{}
	// claimed are the parts it reports lost: an outstanding want, or the hole committed for one.
	claimed map[string]struct{}
	// removed are the parts it says it deliberately took out.
	removed map[string]struct{}
	// stated is false for an index whose writer predates tombstones, where absence is all there is
	// to go on — the pre-tombstone behavior, kept for the transition.
	stated bool
	// live are its data entries, the successors an absence can be explained by.
	live []bucketindex.Entry
}

func accountOf(ix *bucketindex.Index) peerAccount {
	a := peerAccount{
		present: make(map[string]struct{}, len(ix.Entries)),
		claimed: make(map[string]struct{}, len(ix.Wanted)),
		removed: ix.Removals(),
		stated:  ix.RecordsRemovals(),
	}

	for i := range ix.Entries {
		e := &ix.Entries[i]
		a.present[e.Prefix] = struct{}{}

		if e.Hole {
			a.claimed[e.Prefix] = struct{}{}

			continue
		}

		a.live = append(a.live, *e)
	}

	for i := range ix.Wanted {
		a.claimed[ix.Wanted[i].Prefix] = struct{}{}
	}

	return a
}

// accountsFor reports whether the index explains where e's data is: it names the part itself, it
// says it removed it, or it holds a successor whose identity subsumes it.
//
// The successor test is the one an omission needs and a tombstone cannot always give: tombstones
// are bounded ([bucketindex.MaxRemovals]) and age out, while [bucketindex.Entry.Supersedes] is
// decidable from identity alone and never expires. A live entry covering e's blocks at a higher
// merge level was built from e and holds its rows: block numbers are allocated once per shard and
// never reused, and a merge output covers the union of the blocks its inputs covered and nothing
// else — so containment is a statement about which parts were consumed, not a guess from a range.
// It is the same evidence a repair accepts for a want ([bucketindex.Index.Satisfying]), minus that
// one's split-group case: a peer holding a want's rows only jointly, across the fragments of a
// split, does not account for e here, and the part is kept.
func (a peerAccount) accountsFor(e bucketindex.Entry) bool {
	if _, ok := a.present[e.Prefix]; ok {
		return true
	}

	if !a.stated {
		return true
	}

	if _, ok := a.removed[e.Prefix]; ok {
		return true
	}

	return a.supersedes(e)
}

func (a peerAccount) supersedes(e bucketindex.Entry) bool {
	for i := range a.live {
		if a.live[i].Supersedes(e) {
			return true
		}
	}

	return false
}

func (s *Syncer) holdsManifest(ctx context.Context, partPrefix string) (bool, error) {
	_, err := backend.ReadView(ctx, s.local, partPrefix+"/"+manifestName)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, backend.ErrNotExist) {
		return false, nil
	}

	return false, errors.Wrapf(err, "read manifest of %q", partPrefix)
}

// retainHeld is the peer's index with each held part back in Entries.
//
// For a part the peer claims lost the claim is carried beside it as a want, on purpose: it is what
// the owner's repair pass discharges with a commit, and that commit is what republishes the part at
// a generation the peers adopt. A hole is turned into the want it discharged, since an entry and a
// hole cannot share a prefix; LostParts is untouched, as it is on any revocation.
//
// For a part the peer merely omits no want is recorded. A want asserts that this node owes a repair
// for a part it cannot read, and this node reads it fine; the peer stated nothing to carry, and
// inventing an obligation would make the loss counters report the healthy node instead of the
// damaged one.
func retainHeld(peer *bucketindex.Index, h held) *bucketindex.Index {
	ix := *peer
	ix.Entries = append([]bucketindex.Entry(nil), peer.Entries...)
	ix.Wanted = append([]bucketindex.Want(nil), peer.Wanted...)

	wanted := peer.Wants()

	for i := range h.claimed {
		e := &h.claimed[i]
		ix.Add(*e)

		if _, ok := wanted[e.Prefix]; !ok {
			ix.RecordWant(bucketindex.WantOf(*e, peer.Generation))
		}
	}

	for i := range h.omitted {
		ix.Add(h.omitted[i])
	}

	return &ix
}

// manifestParts is the parts a listing shows a complete copy of.
func manifestParts(listed []string, enginePrefix string) map[string]struct{} {
	out := make(map[string]struct{})

	for _, k := range listed {
		if !ValidKey(k) || !strings.HasPrefix(k, enginePrefix+"/") || path.Base(k) != manifestName {
			continue
		}

		if part := partOf(k, enginePrefix); part != "" && k == part+"/"+manifestName {
			out[part] = struct{}{}
		}
	}

	return out
}
