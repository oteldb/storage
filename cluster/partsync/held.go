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

// heldEntries is the subset of the local index's parts that the peer's index reports lost — as a
// want or as a hole — while this node still holds them: the local index names the part and its
// manifest is on disk. The peer's index is a copy of whichever owner's index last superseded it and
// says nothing about this disk, so its claim of loss does not remove a part that is here.
func (s *Syncer) heldEntries(
	ctx context.Context, localIndex, peerIndex *bucketindex.Index,
) ([]bucketindex.Entry, error) {
	holes := peerIndex.Holes()
	if len(peerIndex.Wanted) == 0 && len(holes) == 0 {
		return nil, nil
	}

	claimed := make(map[string]struct{}, len(peerIndex.Wanted)+len(holes))
	for _, w := range peerIndex.Wanted {
		claimed[w.Prefix] = struct{}{}
	}

	for _, h := range holes {
		claimed[h.Prefix] = struct{}{}
	}

	var held []bucketindex.Entry

	for _, e := range localIndex.Entries {
		if e.Hole {
			continue
		}

		if _, lost := claimed[e.Prefix]; !lost {
			continue
		}

		ok, err := s.holdsManifest(ctx, e.Prefix)
		if err != nil {
			return nil, err
		}

		if ok {
			held = append(held, e)
		}
	}

	return held, nil
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

// retainHeld is the peer's index with each held part back in Entries, and the peer's claim of loss
// carried beside it as a want. The want is kept on purpose: it is what the owner's repair pass
// discharges with a commit, and that commit is what republishes the part at a generation the
// peers adopt. A hole is turned into the want it discharged, since an entry and a hole cannot share
// a prefix; LostParts is untouched, as it is on any revocation.
func retainHeld(peer *bucketindex.Index, held []bucketindex.Entry) *bucketindex.Index {
	ix := *peer
	ix.Entries = append([]bucketindex.Entry(nil), peer.Entries...)
	ix.Wanted = append([]bucketindex.Want(nil), peer.Wanted...)

	wanted := peer.Wants()

	for _, e := range held {
		ix.Add(e)

		if _, ok := wanted[e.Prefix]; !ok {
			ix.RecordWant(bucketindex.WantOf(e, peer.Generation))
		}
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
