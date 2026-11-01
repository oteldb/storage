package postings

import "github.com/oteldb/storage/signal"

type denseRangePostings struct {
	first uint32
	n, i  int
}

func (p *denseRangePostings) Next() bool {
	p.i++
	return p.i < p.n
}

func (p *denseRangePostings) Seek(id signal.SeriesID) bool {
	if id.Hi != 0 || id.Lo >= uint64(p.first)+uint64(p.n) {
		p.i = p.n
		return false
	}
	p.i = max(p.i, 0)
	if p.i >= p.n {
		return false
	}
	if id.Lo > uint64(p.first)+uint64(p.i) {
		p.i = int(id.Lo - uint64(p.first))
	}
	return true
}

func (p *denseRangePostings) At() signal.SeriesID {
	return signal.SeriesID{Lo: uint64(p.first) + uint64(p.i)}
}

func (*denseRangePostings) Err() error { return nil }
