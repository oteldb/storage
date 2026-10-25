package recordengine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/encoding/compress"
)

type fetchDecodeSize struct{ resident, sourceWide, workspace int64 }

func (n *fetchDecodeSize) addColumn(part *part, name string, conditions bool) error {
	desc, ok := part.reader.ColumnDescByName(name)
	if !ok {
		return errors.Errorf("no column %q", name)
	}
	rows := int64(part.reader.RowCount())
	if desc.Const {
		charge := fetchMul(rows, 8)
		if desc.Kind == block.KindBytes {
			charge = fetchAdd(rows, 24)
			if desc.Codec == chunk.CodecBytesRaw && conditions {
				charge = fetchAdd(charge, fetchMul(rows, int64(len(desc.ConstBytes))))
			}
		}
		n.resident = fetchAdd(n.resident, charge)
		return nil
	}
	in, err := part.reader.ColumnInputSize(name)
	if err != nil {
		return err
	}
	if in.SourceWide {
		n.sourceWide = max(n.sourceWide, in.WholeResident)
	} else {
		n.resident = fetchAdd(n.resident, in.WholeResident)
	}
	n.resident = fetchAdd(n.resident, fetchAdd(fetchMul(2, desc.Bytes), in.DirBytes))
	transient := fetchAdd(fetchMul(2, in.MaxFrameRaw), in.MaxFrameBytes)
	if desc.Compress != compress.AlgorithmNone {
		transient = fetchAdd(transient, compress.DecodeWorkspace)
	}
	n.workspace = max(n.workspace, transient)
	return nil
}

// fetchDecodeBytes bounds selected whole-column decodes, not expanded byte-cell copies.
func (p *fetchPlan) fetchDecodeBytes(part *part, sel colSel, timestamps bool) (int64, error) {
	var n fetchDecodeSize
	if timestamps {
		if err := n.addColumn(part, colTs, len(p.conds) > 0); err != nil {
			return 0, err
		}
	}
	for k, selected := range sel.ints {
		if selected {
			if err := n.addColumn(part, p.e.cfg.Schema.intColumn(k).Name, len(p.conds) > 0); err != nil {
				return 0, err
			}
		}
	}
	for k, selected := range sel.bytes {
		if selected {
			if err := n.addColumn(part, p.e.cfg.Schema.byteColumn(k).Name, len(p.conds) > 0); err != nil {
				return 0, err
			}
		}
	}
	return fetchAdd(n.resident, fetchAdd(n.sourceWide, n.workspace)), nil
}

func (p *fetchPlan) readBudgetedParts(ctx context.Context) error {
	if len(p.conds) == 0 && p.limit > 0 {
		p.orderPartsForLimit(p.reverse)
		p.wm = &tsHeap{limit: p.limit, reverse: p.reverse}
		for _, id := range p.ids {
			if acc := p.accs[id]; acc != nil {
				p.wm.add(acc.ts)
			}
		}
	}
	for i, part := range p.liveParts {
		if err := p.readBudgetedPart(ctx, part); err != nil {
			return err
		}
		if p.stopAfter(i) {
			p.partsSkippedLimit = len(p.liveParts) - i - 1
			break
		}
	}
	return nil
}

func (p *fetchPlan) readBudgetedPart(ctx context.Context, part *part) error {
	var temporary int64
	defer func() {
		// Compiled predicates retain their dictionaries; do not carry one part's decode into
		// the next after returning its temporary reservation.
		p.evalScratch, p.hits, p.hitRuns = nil, nil, nil
		clear(p.memoScratch)
		p.memory.release(temporary)
	}()
	reserve := func(n int64) error {
		if err := p.memory.reserve(n); err != nil {
			return err
		}
		temporary = fetchAdd(temporary, n)
		return nil
	}
	first, err := p.fetchDecodeBytes(part, p.condSel, true)
	if err != nil {
		return err
	}
	granules := int64(part.reader.RowCount())
	if size := int64(part.reader.Manifest().GranuleSize); size > 0 {
		granules = granules/size + 1
	}
	first = fetchAdd(first, fetchMul(int64(len(p.conds)), int64(part.reader.RowCount())))
	if err := reserve(fetchAdd(first, fetchMul(granules, 64))); err != nil {
		return err
	}
	blocks := part.windowGranules(ctx, p.sortedIDs, p.start, p.end)
	// Fresh buffers make the reservation match this decode rather than a larger pooled buffer.
	lz, err := part.readLazyConds(ctx, p.condSel, p.conds, nil, blocks)
	if err != nil {
		return err
	}
	p.matched = part.heldStreams(p.matched[:0], p.sortedIDs)
	var candidates int64
	for _, sr := range p.matched {
		w := tsWindow(lz.ts, sr.rowRange, p.start, p.end)
		candidates = fetchAdd(candidates, int64(w.end-w.start))
	}
	matched, err := p.budgetedMatches(part, lz, candidates, reserve)
	if err != nil {
		return err
	}
	if !matched {
		return nil
	}

	remaining := p.remainingProjection()
	second, err := p.fetchDecodeBytes(part, remaining, false)
	if err != nil {
		return err
	}
	if err := reserve(second); err != nil {
		return err
	}
	if err := part.decodeProjected(ctx, lz, p.sel, p.condSel, nil, blocks); err != nil {
		return err
	}
	if len(p.conds) == 0 {
		return p.gatherBudgetedRanges(lz)
	}
	return p.gatherBudgetedHits(lz)
}

func (p *fetchPlan) remainingProjection() colSel {
	remaining := colSel{ints: make([]bool, len(p.sel.ints)), bytes: make([]bool, len(p.sel.bytes))}
	for k := range remaining.ints {
		remaining.ints[k] = p.sel.ints[k] && !p.condSel.ints[k]
	}
	for k := range remaining.bytes {
		remaining.bytes[k] = p.sel.bytes[k] && !p.condSel.bytes[k]
	}
	return remaining
}

func (p *fetchPlan) gatherBudgetedHits(lz *lazyCols) error {
	for _, run := range p.hitRuns {
		acc := p.accs[run.id]
		indices := p.hits[run.lo:run.hi]
		cells := p.hitCellBytes(lz, indices)
		if err := p.prepareRows(acc, len(indices), cells); err != nil {
			return err
		}
		n := acc.len()
		if indices[len(indices)-1]-indices[0]+1 == len(indices) {
			acc.appendLazyRange(lz, indices[0], indices[len(indices)-1]+1)
		} else {
			for _, i := range indices {
				if err := acc.appendLazyRow(lz, i); err != nil {
					return err
				}
			}
		}
		if p.wm != nil {
			p.wm.add(acc.ts[n:])
		}
	}
	return nil
}

func (c *recordCols) appendLazyRange(lz *lazyCols, lo, hi int) {
	c.ts = append(c.ts, lz.ts[lo:hi]...)
	c.noteTS(lz.ts[lo])
	c.noteTS(lz.ts[hi-1])
	for k, selected := range c.sel.ints {
		if selected {
			c.ints[k] = append(c.ints[k], lz.ints[k][lo:hi]...)
		}
	}
	for k, selected := range c.sel.bytes {
		if selected {
			c.appendLazyByteRange(k, lz, lo, hi)
		}
	}
}

func (p *fetchPlan) conditionMemoBytes(lz *lazyCols) int64 {
	entries := int64(maxIntMemo)
	for _, dc := range lz.bytes {
		if dc != nil {
			entries = max(entries, int64(len(dc.Entries)))
		}
	}
	return fetchMul(fetchMul(2, int64(len(p.conds))), entries)
}

func (p *fetchPlan) budgetedMatches(part *part, lz *lazyCols, candidates int64, reserve func(int64) error) (bool, error) {
	if len(p.conds) == 0 {
		return candidates > 0, nil
	}
	if err := reserve(fetchAdd(fetchMul(candidates, 16), p.conditionMemoBytes(lz))); err != nil {
		return false, err
	}
	p.hits = make([]int, 0, int(candidates))
	return p.scanMatches(part, lz), nil
}

func fixedCellBytes(lz *lazyCols, k int) (int, bool) {
	if rb := lz.rawBlob[k]; rb.blob != nil {
		return rb.width, true
	}
	if dc := lz.bytes[k]; dc != nil && dc.IDWidth != 0 && len(dc.Entries) == 1 {
		return len(dc.Entries[0]), true
	}
	return 0, false
}

func (p *fetchPlan) rangeCellBytes(lz *lazyCols, lo, hi int) []int64 {
	cells := make([]int64, len(p.sel.bytes))
	for k, selected := range p.sel.bytes {
		if !selected {
			continue
		}
		if size, fixed := fixedCellBytes(lz, k); fixed {
			cells[k] = fetchMul(int64(hi-lo), int64(size))
			continue
		}
		for i := lo; i < hi; i++ {
			cells[k] = fetchAdd(cells[k], int64(len(lz.cell(k, i))))
		}
	}
	return cells
}

func (p *fetchPlan) hitCellBytes(lz *lazyCols, indices []int) []int64 {
	cells := make([]int64, len(p.sel.bytes))
	for k, selected := range p.sel.bytes {
		if !selected {
			continue
		}
		if size, fixed := fixedCellBytes(lz, k); fixed {
			cells[k] = fetchMul(int64(len(indices)), int64(size))
			continue
		}
		for _, i := range indices {
			cells[k] = fetchAdd(cells[k], int64(len(lz.cell(k, i))))
		}
	}
	return cells
}

func (p *fetchPlan) gatherBudgetedRanges(lz *lazyCols) error {
	for _, sr := range p.matched {
		w := tsWindow(lz.ts, sr.rowRange, p.start, p.end)
		if w.start == w.end {
			continue
		}
		acc := p.accs[sr.id]
		if err := p.prepareRows(acc, w.end-w.start, p.rangeCellBytes(lz, w.start, w.end)); err != nil {
			return err
		}
		n := acc.len()
		acc.appendLazyRange(lz, w.start, w.end)
		if p.wm != nil {
			p.wm.add(acc.ts[n:])
		}
	}
	return nil
}

func (c *recordCols) appendLazyByteRange(k int, lz *lazyCols, lo, hi int) {
	if dc := lz.bytes[k]; dc != nil && dc.IDWidth != 0 && len(dc.Entries) == 1 {
		for range hi - lo {
			c.bytes[k].appendCell(dc.Entries[0])
		}
		return
	}
	for i := lo; i < hi; i++ {
		c.bytes[k].appendCell(lz.cell(k, i))
	}
}
