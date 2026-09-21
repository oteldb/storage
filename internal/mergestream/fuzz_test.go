package mergestream_test

import (
	"slices"
	"testing"

	"github.com/oteldb/storage/internal/mergestream/mergestreamtest"
	"github.com/oteldb/storage/signal"
)

// FuzzKeys checks the k-way union against sort-and-dedup. Keys are drawn from a 16×16 space so the
// inputs collide heavily — the dedup, not the ordering, is where a heap merge goes wrong.
func FuzzKeys(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{2, 0x01, 0x02, 0x03, 0x11, 0x11, 0x10})
	f.Add([]byte{7, 0xff, 0x00, 0x0f, 0xf0, 0x01, 0x10, 0x11, 0x11, 0x11})
	f.Add([]byte{1, 0x05, 0x05, 0x05, 0x05})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}

		n := int(data[0]%8) + 1

		src := make([][]signal.SeriesID, n)
		for i, b := range data[1:] {
			s := i % n
			src[s] = append(src[s], signal.SeriesID{Hi: uint64(b >> 4), Lo: uint64(b & 0xf)})
		}

		for _, s := range src {
			slices.SortFunc(s, signal.SeriesID.Compare)
		}

		mergestreamtest.CheckKeys(t, src)
	})
}
