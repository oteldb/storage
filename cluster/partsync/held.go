package partsync

import (
	"context"
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

	for _, e := range localIndex.Entries {
		if e.Hole {
			continue
		}

		_, claimed := acct.claimed[e.Prefix]
		if !claimed && acct.accountsFor(e) {
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
			out.claimed = append(out.claimed, e)
		} else {
			out.omitted = append(out.omitted, e)
		}
	}

	return out, nil
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

	for _, e := range ix.Entries {
		a.present[e.Prefix] = struct{}{}

		if e.Hole {
			a.claimed[e.Prefix] = struct{}{}

			continue
		}

		a.live = append(a.live, e)
	}

	for _, w := range ix.Wanted {
		a.claimed[w.Prefix] = struct{}{}
	}

	return a
}

// accountsFor reports whether the index explains where e's data is: it names the part itself, it
// says it removed it, or it holds a successor whose identity subsumes it.
//
// The successor test is the one an omission needs and a tombstone cannot always give: tombstones
// are bounded ([bucketindex.MaxRemovals]) and age out, while [bucketindex.Entry.Supersedes] is
// decidable from identity alone — block intervals are allocated once per shard, so a live entry
// covering e's blocks at a higher merge level was built from e and holds its rows. It is the same
// evidence a repair accepts for a want ([bucketindex.Index.Satisfying]).
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

	for _, e := range h.claimed {
		ix.Add(e)

		if _, ok := wanted[e.Prefix]; !ok {
			ix.RecordWant(bucketindex.WantOf(e, peer.Generation))
		}
	}

	for _, e := range h.omitted {
		ix.Add(e)
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
