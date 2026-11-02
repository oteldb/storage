package engine

import (
	"encoding/binary"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/internal/buildbudget"
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/internal/mergestream"
	"github.com/oteldb/storage/signal"
)

type mergeFinishBudget struct {
	refs         []uint32
	symbols      series.EncodingBudget
	encoded      int64
	materialized int64
}

func (b *mergeFinishBudget) add(view series.View, ref uint32) {
	b.refs = append(b.refs, ref)
	b.encoded = buildbudget.Add(b.encoded, view.EncodingBytes(int(ref)))
	b.materialized = max(b.materialized, view.MaterializationBytes(int(ref)))
	b.symbols.Add(view, int(ref))
}

type mergeIdentityBatch struct {
	ids   [256]signal.SeriesID
	refs  [256]uint32
	found [256]bool
	view  series.View
	at, n int
}

func (b *mergeIdentityBatch) next(e *Engine, keys *mergestream.Keys) bool {
	if b.at == b.n {
		b.at, b.n = 0, 0
		b.view = series.View{}
		for b.n < len(b.ids) && keys.Next() {
			b.ids[b.n] = keys.Key()
			b.n++
		}
		if b.n == 0 {
			return false
		}
		e.mu.RLock()
		b.view = e.head.series.View()
		for i, id := range b.ids[:b.n] {
			b.refs[i], b.found[i] = e.head.series.Ref(id)
		}
		e.mu.RUnlock()
	}
	b.at++
	return true
}

func (p *partStreamWriter) appendMergeSeries(view series.View, ref uint32, found bool,
	id signal.SeriesID, ts []int64, values, sf []float64, covered int64,
) error {
	if len(ts) > 0 && found && (len(p.runs) == 0 || p.runs[len(p.runs)-1].Value != idToU128(id)) {
		p.mergeFinish.add(view, ref)
	}
	return p.appendSeries(idToU128(id), ts, values, sf, covered)
}

func (b *mergeFinishBudget) bytes() int64 {
	// EncodeSorted starts with 64 bytes per identity; its grown body and destination coexist.
	body := max(b.encoded, buildbudget.Mul(int64(len(b.refs)), 64))
	n := buildbudget.Add(memsize.Slice(b.refs), b.symbols.Bytes())
	n = buildbudget.Add(n, b.materialized)
	return buildbudget.Add(n, buildbudget.Mul(buildbudget.Add(body, binary.MaxVarintLen64), 4))
}

func (p *partStreamWriter) mergeResidentBytes() int64 {
	n := buildbudget.Add(p.residentBytes(), p.w.FinishBytes())
	if p.mergeFinish == nil {
		return n
	}
	n = buildbudget.Add(n, p.mergeFinish.bytes())
	identities := int64(len(p.runs))
	// Sidecars finish sequentially. Reopening may read a whole SIDx while its encoding survives.
	sidx := buildbudget.Add(5+2*binary.MaxVarintLen64+4, buildbudget.Mul(identities, sidxEntryW))
	watermarks := buildbudget.Add(8+binary.MaxVarintLen64, buildbudget.Mul(identities, 24))
	sidecar := max(sidx, watermarks)
	if p.withStats && !p.sampled {
		stats := buildbudget.Add(8+binary.MaxVarintLen64, buildbudget.Mul(identities, 16+binary.MaxVarintLen64+3*8))
		sidecar = max(sidecar, stats)
	}
	// Reader/manifest state and allocator growth around the largest encoded/read sidecar.
	return buildbudget.Add(n, buildbudget.Add(16<<10, buildbudget.Mul(sidecar, 4)))
}
