package engine

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/oteldb/storage/signal"
)

func BenchmarkSeriesIndexSearch(b *testing.B) {
	for _, count := range []int{1024, 1 << 20} {
		b.Run("series="+strconv.Itoa(count), func(b *testing.B) {
			entries := make([]byte, 0, count*sidxEntryW)
			for i := range count {
				entries = binary.BigEndian.AppendUint64(entries, uint64(i))
				entries = binary.BigEndian.AppendUint64(entries, uint64(i*3))
				entries = binary.BigEndian.AppendUint32(entries, uint32(i))
			}
			p := pagedIndex{n: count, total: count}
			for _, mode := range []string{"tail", "fixed"} {
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					pos := 0
					for b.Loop() {
						pos = (pos + 104729) & (count - 1)
						id := signal.SeriesID{Hi: uint64(pos), Lo: uint64(pos * 3)}
						var got int
						var ok bool
						if mode == "tail" {
							got, ok = p.searchTailSlice(entries, id)
						} else {
							got, ok = p.search(entries, id)
						}
						if !ok || got != pos {
							b.Fatal("wrong lookup", got, pos)
						}
					}
				})
			}
		})
	}
}

func (p *pagedIndex) searchTailSlice(entries []byte, id signal.SeriesID) (int, bool) {
	lo, hi := 0, p.n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		entry := entries[mid*sidxEntryW:]
		high := binary.BigEndian.Uint64(entry)
		switch {
		case high < id.Hi:
			lo = mid + 1
		case high > id.Hi:
			hi = mid
		default:
			switch low := binary.BigEndian.Uint64(entry[8:]); {
			case low < id.Lo:
				lo = mid + 1
			case low > id.Lo:
				hi = mid
			default:
				return mid, true
			}
		}
	}

	return 0, false
}
