package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/signal"
)

func TestPublicationReservationsRollback(t *testing.T) {
	t.Parallel()
	var reservations seriesOwnership
	makeReservation := func(refs ...uint32) *preparedBatch {
		p := &preparedBatch{}
		for _, ref := range refs {
			p.ownedIDs = append(p.ownedIDs, signal.SeriesID{Lo: uint64(ref)})
		}
		p.prepareOwnership()
		return p
	}
	owner := makeReservation(63*64 + 7)
	candidate := makeReservation(0, 63*64+7)
	independent := makeReservation(0)
	require.True(t, reservations.try(owner))
	require.False(t, reservations.try(candidate))
	require.True(t, reservations.try(independent), "earlier stripes roll back after a later conflict")
	reservations.release(independent)
	reservations.release(owner)
	require.True(t, reservations.try(candidate))
	reservations.release(candidate)
	for i := range reservations.stripes {
		require.Empty(t, reservations.stripes[i].busy)
	}
}

func FuzzPublicationReservations(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0})
	f.Add([]byte{0, 63, 63, 1, 0, 0, 2, 63, 62, 3, 63, 63, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		var reservations seriesOwnership
		var owners [4]preparedBatch
		var active [4]bool
		occupied := make(map[signal.SeriesID]bool)
		data = data[:min(len(data), 768)]
		for i := 0; i+2 < len(data); i += 3 {
			owner := int(data[i] & 3)
			p := &owners[owner]
			if active[owner] {
				reservations.release(p)
				active[owner] = false
				for _, ref := range p.ownedIDs {
					delete(occupied, ref)
				}
			} else {
				word, bit := uint32(data[i+1]), uint32(data[i+2]&63)
				p.ownedIDs = append(p.ownedIDs[:0], signal.SeriesID{Lo: uint64(word*64 + bit)}, signal.SeriesID{Lo: uint64((word^63)*64 + bit)})
				p.prepareOwnership()
				want := true
				for _, ref := range p.ownedIDs {
					want = want && !occupied[ref]
				}
				if got := reservations.try(p); got != want {
					t.Fatalf("step %d acquire=%v want=%v", i, got, want)
				}
				if want {
					active[owner] = true
					for _, ref := range p.ownedIDs {
						occupied[ref] = true
					}
				}
			}
			actual := make(map[signal.SeriesID]bool)
			for stripe := range reservations.stripes {
				for id := range reservations.stripes[stripe].busy {
					actual[id] = true
				}
			}
			if len(actual) != len(occupied) {
				t.Fatalf("step %d reservation count=%d want=%d", i, len(actual), len(occupied))
			}
			for id := range occupied {
				if !actual[id] {
					t.Fatal("missing owned identity")
				}
			}

		}
	})
}
