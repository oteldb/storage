package postings

import (
	"sort"

	"github.com/oteldb/storage/signal"
)

type densePostings struct {
	ids []uint32
	i   int
}

func fromDenseSlice(ids []uint32) Postings {
	if len(ids) == 0 {
		return Empty()
	}
	// Lists are sorted and unique before iterator construction.
	if uint64(ids[len(ids)-1])-uint64(ids[0])+1 == uint64(len(ids)) {
		return &denseRangePostings{first: ids[0], n: len(ids), i: -1}
	}
	return &densePostings{ids: ids, i: -1}
}

func (p *densePostings) Next() bool { p.i++; return p.i < len(p.ids) }

func (p *densePostings) Seek(id signal.SeriesID) bool {
	if id.Hi != 0 || id.Lo > uint64(^uint32(0)) {
		p.i = len(p.ids)
		return false
	}
	p.i = max(p.i, 0)
	if p.i >= len(p.ids) {
		return false
	}
	if uint64(p.ids[p.i]) >= id.Lo {
		return true
	}
	base := p.i
	p.i += sort.Search(len(p.ids)-base, func(i int) bool { return uint64(p.ids[base+i]) >= id.Lo })
	return p.i < len(p.ids)
}

func (p *densePostings) At() signal.SeriesID { return signal.SeriesID{Lo: uint64(p.ids[p.i])} }
func (p *densePostings) Err() error          { return nil }
