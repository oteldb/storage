package recordengine

import (
	"context"
	"math"
	"slices"
	"sync"

	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/readbudget"
)

type fetchMemory struct {
	budget *readbudget.Budget
	held   int64
	once   sync.Once
}

func newFetchMemory(ctx context.Context) *fetchMemory {
	if b := readbudget.From(ctx); b != nil {
		return &fetchMemory{budget: b}
	}
	return nil
}

func (m *fetchMemory) reserve(n int64) error {
	if m == nil || n <= 0 {
		return nil
	}
	if err := m.budget.Reserve(n); err != nil {
		return err
	}
	m.held += n
	return nil
}

func (m *fetchMemory) release(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.budget.Release(n)
	m.held -= n
}

func (m *fetchMemory) done() {
	if m == nil {
		return
	}
	m.once.Do(func() { m.release(m.held) })
}

func fetchAdd(a, b int64) int64 {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

func fetchMul(a, b int64) int64 {
	if a > 0 && b > math.MaxInt64/a {
		return math.MaxInt64
	}
	return a * b
}

func fetchPlanBytes(schema *Schema, streams, parts, conditions int) int64 {
	// Per-stream accumulators, maps, selected-column headers and matched/hit-run metadata.
	perStream := int64(1024) + int64(schema.numInts())*memsize.Of[[]int64]() + int64(schema.numBytes())*memsize.Of[byteCol]()
	return fetchAdd(fetchMul(int64(streams), perStream), fetchAdd(fetchMul(int64(parts), 32), fetchMul(int64(conditions), 256)))
}

// reserveRows covers selected output, append growth/copy, stable sorting, byte views and top-N
// scratch. It counts actual surviving cells rather than a source part's average row width.
func (p *fetchPlan) reserveRows(rows, bytes int64) error {
	fixed, views := int64(8), int64(0)
	for _, selected := range p.sel.ints {
		if selected {
			fixed += 8
		}
	}
	for _, selected := range p.sel.bytes {
		if selected {
			fixed += 4
			views += 24
		}
	}
	// An append can hold old capacity plus its doubled replacement. Sorting needs the same
	// three-copy allowance; indices, filtering and the two top-N heaps add at most 48 B/row.
	n := fetchAdd(fetchMul(rows, fetchAdd(fetchMul(3, fixed), views+48)), fetchMul(3, bytes))
	return p.memory.reserve(n)
}

func (p *fetchPlan) seedBudgetedHead() error {
	if p.condSel.ints == nil {
		p.condSel = colSel{ints: make([]bool, p.e.cfg.Schema.numInts()), bytes: make([]bool, p.e.cfg.Schema.numBytes())}
	}
	for _, id := range p.sortedIDs {
		acc := newRecordCols(p.e.cfg.Schema, 0, p.sel)
		if err := p.seedBudgeted(acc, p.e.head.records[id]); err != nil {
			return err
		}
		if err := p.seedBudgeted(acc, p.e.flushing[id]); err != nil {
			return err
		}
		p.accs[id] = acc
		if p.headRows != nil {
			p.headRows[id] = acc.len()
		}
		if series, ok := p.e.head.series.Get(id); ok {
			p.series[id] = series
		}
	}
	return nil
}

func (p *fetchPlan) seedBudgeted(acc, src *recordCols) error {
	if src == nil {
		return nil
	}
	rows := 0
	cells := make([]int64, len(p.sel.bytes))
	for i, ts := range src.ts {
		if ts < p.start || ts > p.end {
			continue
		}
		rows++
		for k, selected := range p.sel.bytes {
			if selected {
				cells[k] = fetchAdd(cells[k], int64(len(src.bytes[k].at(i))))
			}
		}
	}
	if err := p.prepareRows(acc, rows, cells); err != nil {
		return err
	}
	return appendColsWindow(src, acc, p.start, p.end)
}

func (p *fetchPlan) prepareRows(acc *recordCols, rows int, cells []int64) error {
	var bytes int64
	for k, n := range cells {
		if n > byteColCap-int64(len(acc.bytes[k].data)) {
			return errColumnTooLarge(acc.schema.byteColumn(k).Name)
		}
		bytes = fetchAdd(bytes, n)
	}
	if err := p.reserveRows(int64(rows), bytes); err != nil {
		return err
	}
	acc.ts = slices.Grow(acc.ts, rows)
	for k, selected := range acc.sel.ints {
		if selected {
			acc.ints[k] = slices.Grow(acc.ints[k], rows)
		}
	}
	for k, selected := range acc.sel.bytes {
		if selected {
			acc.bytes[k].offsets = slices.Grow(acc.bytes[k].offsets, rows)
			acc.bytes[k].data = slices.Grow(acc.bytes[k].data, int(cells[k]))
		}
	}
	return nil
}
