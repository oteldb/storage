package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublicationReservationsRollback(t *testing.T) {
	t.Parallel()
	var reservations publicationReservations
	makeReservation := func(refs ...uint32) *preparedBatch {
		p := &preparedBatch{refs: refs}
		p.prepareReservation()
		return p
	}
	owner := makeReservation(63*64 + 7)
	candidate := makeReservation(0, 63*64+7)
	independent := makeReservation(0)
	require.True(t, reservations.acquire(owner))
	require.False(t, reservations.acquire(candidate))
	require.True(t, reservations.acquire(independent), "earlier stripes roll back after a later conflict")
	reservations.release(independent, len(independent.masks))
	reservations.release(owner, len(owner.masks))
	require.True(t, reservations.acquire(candidate))
	reservations.release(candidate, len(candidate.masks))
	for i := range reservations.stripes {
		require.Empty(t, reservations.stripes[i].words)
	}
}

func FuzzPublicationReservations(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0})
	f.Add([]byte{0, 63, 63, 1, 0, 0, 2, 63, 62, 3, 63, 63, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		var reservations publicationReservations
		var owners [4]preparedBatch
		var active [4]bool
		occupied := make(map[uint32]bool)
		data = data[:min(len(data), 768)]
		for i := 0; i+2 < len(data); i += 3 {
			owner := int(data[i] & 3)
			p := &owners[owner]
			if active[owner] {
				reservations.release(p, len(p.masks))
				active[owner] = false
				for _, ref := range p.refs {
					delete(occupied, ref)
				}
			} else {
				word, bit := uint32(data[i+1]), uint32(data[i+2]&63)
				p.refs = append(p.refs[:0], word*64+bit, (word^63)*64+bit)
				p.prepareReservation()
				want := true
				for _, ref := range p.refs {
					want = want && !occupied[ref]
				}
				if got := reservations.acquire(p); got != want {
					t.Fatalf("step %d acquire=%v want=%v", i, got, want)
				}
				if want {
					active[owner] = true
					for _, ref := range p.refs {
						occupied[ref] = true
					}
				}
			}
			actual := make(map[uint32]uint64)
			for ref := range occupied {
				actual[ref>>6] |= uint64(1) << (ref & 63)
			}
			entries := 0
			for stripe := range reservations.stripes {
				for word, mask := range reservations.stripes[stripe].words {
					if actual[word] != mask {
						t.Fatalf("step %d stale reservation word %d", i, word)
					}
					entries++
				}
			}
			if entries != len(actual) {
				t.Fatalf("step %d reservation count=%d want=%d", i, entries, len(actual))
			}
		}
	})
}
