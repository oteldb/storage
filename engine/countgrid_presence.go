package engine

import (
	"math"
	"math/bits"
	"sort"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
)

type gridSpan struct{ lo, hi uint32 }

type gridPresence struct {
	grid  fetch.WindowGrid
	words []uint64
	count int
}

func newGridPresence(grid fetch.WindowGrid) (*gridPresence, error) {
	if grid.Step <= 0 || grid.Width <= 0 || grid.NumSteps <= 0 || uint64(grid.NumSteps) > math.MaxUint32 {
		return nil, errors.New("count grid needs positive dimensions within uint32 steps")
	}
	steps := int64(grid.NumSteps - 1)
	if steps > math.MaxInt64/grid.Step || grid.Start > math.MaxInt64-steps*grid.Step {
		return nil, errors.New("count grid endpoints overflow")
	}
	if grid.NumSteps == int(^uint(0)>>1) {
		return nil, errors.New("count grid result length overflows")
	}
	return &gridPresence{grid: grid, words: make([]uint64, (grid.NumSteps-1)/64+1)}, nil
}

func (p *gridPresence) reset()         { clear(p.words); p.count = 0 }
func (p *gridPresence) complete() bool { return p.count == p.grid.NumSteps }

func (p *gridPresence) covered(minTS, maxTS int64) gridSpan {
	lo := sort.Search(p.grid.NumSteps, func(i int) bool {
		return p.grid.Start+int64(i)*p.grid.Step >= maxTS
	})
	hi := sort.Search(p.grid.NumSteps, func(i int) bool {
		end := p.grid.Start + int64(i)*p.grid.Step
		return end >= math.MinInt64+p.grid.Width && end-p.grid.Width >= minTS
	})
	if lo > hi {
		lo = hi
	}
	return gridSpan{uint32(lo), uint32(hi)}
}

func (p *gridPresence) add(span gridSpan) {
	for lo, end := int(span.lo), int(span.hi); lo < end; {
		word, bit := lo/64, uint(lo%64)
		hi := min(end, (word+1)*64)
		mask := (^uint64(0) >> uint(64-(hi-lo))) << bit
		p.count += bits.OnesCount64(mask &^ p.words[word])
		p.words[word] |= mask
		lo = hi
	}
}

func (p *gridPresence) sample(ts int64) { p.add(p.covered(ts, ts)) }

func (p *gridPresence) spans(dst []gridSpan) []gridSpan {
	base := len(dst)
	for i, word := range p.words {
		for word != 0 {
			lo := bits.TrailingZeros64(word)
			hi := lo + bits.TrailingZeros64(^(word >> uint(lo)))
			span := gridSpan{uint32(i*64 + lo), uint32(min(i*64+hi, p.grid.NumSteps))}
			if len(dst) > base && dst[len(dst)-1].hi == span.lo {
				dst[len(dst)-1].hi = span.hi
			} else {
				dst = append(dst, span)
			}
			word &^= (^uint64(0) >> uint(64-(hi-lo))) << uint(lo)
		}
	}
	return dst
}
