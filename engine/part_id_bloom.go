package engine

import (
	"context"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/seriesindex"
	"github.com/oteldb/storage/signal"
)

const maxPartIDBloomWords = 1 << 17

type partIDBloom struct{ words []uint64 }

func partIDBloomWords(series int) int {
	if series <= 0 {
		return 0
	}
	words := 1
	for words < maxPartIDBloomWords && int64(words)*64/10 < int64(series) {
		words *= 2
	}
	return words
}

func partIDBloomBytes(series int) int64 {
	words := partIDBloomWords(series)
	if words == 0 {
		return 0
	}
	return memsize.Of[partIDBloom]() + int64(words)*8
}

func newPartIDBloom(series int) *partIDBloom {
	words := partIDBloomWords(series)
	if words == 0 {
		return nil
	}
	return &partIDBloom{words: make([]uint64, words)}
}

func partIDBloomHashes(id signal.SeriesID) (uint64, uint64) {
	// Global and per-part filters must not share hash locations: these probes already passed
	// the global filter, so its false positives would otherwise bias the part filters too.
	id.Hi ^= 0x243f6a8885a308d3
	id.Lo ^= 0x13198a2e03707344
	return diskBloomHashes(id)
}

func (b *partIDBloom) add(id signal.SeriesID) {
	h1, h2 := partIDBloomHashes(id)
	mask := uint64(len(b.words)*64 - 1)
	for i := range uint64(4) {
		bit := (h1 + i*h2) & mask
		b.words[bit/64] |= uint64(1) << (bit % 64)
	}
}

func (b *partIDBloom) mayContain(id signal.SeriesID) bool {
	if b == nil || len(b.words) == 0 {
		return true
	}
	h1, h2 := partIDBloomHashes(id)
	mask := uint64(len(b.words)*64 - 1)
	for i := range uint64(4) {
		bit := (h1 + i*h2) & mask
		if b.words[bit/64]&(uint64(1)<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

func (b *partIDBloom) sizeBytes() int64 {
	if b == nil {
		return 0
	}
	return memsize.Of[partIDBloom]() + memsize.Slice(b.words)
}

func (p *part) mayContainDiskID(id signal.SeriesID) bool {
	return p.index.mayContainID(id) && p.idBloom.Load().mayContain(id)
}

func (e *Engine) identityBytesLocked() int64 {
	n := e.head.identityBytes() + e.seriesIDs.SizeBytes() + e.identityOrder.bytes.Load()
	for _, p := range e.parts {
		n += p.idBloom.Load().sizeBytes()
	}
	for _, p := range e.foreignParts {
		if slices.Contains(e.parts, p) {
			continue
		}
		n += p.idBloom.Load().sizeBytes()
	}
	for i, p := range e.retiring {
		if slices.Contains(e.parts, p) || slices.Contains(e.retiring[:i], p) {
			continue
		}
		adopted := false
		for _, other := range e.foreignParts {
			if other == p {
				adopted = true
				break
			}
		}
		if adopted {
			continue
		}
		n += p.idBloom.Load().sizeBytes()
	}
	return n
}

func (w *partStreamWriter) seedPartIDBloom(ctx context.Context, p *part) error {
	if !w.e.cfg.DiskSeries {
		return nil
	}
	if w.seriesBuilder == nil || w.seriesBuilder.Count() != len(w.runs) || p.index.seriesCount() != len(w.runs) {
		return errors.Wrap(seriesindex.ErrCorrupt, "output identity count")
	}
	bloom := newPartIDBloom(len(w.runs))
	view := w.seriesBuilder.View()
	for i, run := range w.runs {
		id := u128ToID(run.Value)
		if view.ID(i) != id {
			return errors.Wrap(seriesindex.ErrCorrupt, "output identity ID")
		}
		bloom.add(id)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.idBloom.Store(bloom)
	return nil
}
