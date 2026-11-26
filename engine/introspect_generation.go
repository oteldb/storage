package engine

import (
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/signal"
)

func (h *head) localCardinality() (int, []LabelCard) {
	var cards []LabelCard
	h.post.ForEachName(func(nameID uint32, distinctValues, totalSeries int) {
		name, _ := h.sym.Get(symbols.ID(nameID))
		cards = append(cards, LabelCard{Name: string(name), Series: int64(totalSeries), DistinctValues: distinctValues})
	})
	return h.sym.Len(), cards
}

type generationLabelCard struct {
	series int64
	values map[string]struct{}
}

// Cardinality introspection needs exact value membership across generations.
func (h *head) cardinalityPostings() *head {
	if len(h.encodedRefs) == 0 {
		return h
	}
	scratch := newHead()
	scratch.series = h.series
	view := h.series.View()
	var attrs signal.Attributes
	for i := range view.Len() {
		s := view.At(i, attrs)
		attrs = s.Attributes
		scratch.indexLabels(view.ID(i), s)
	}
	scratch.post.EnsureSorted()
	return scratch
}

func (h *head) residentCardinality() (int, []LabelCard) {
	if h.frozen == nil {
		return h.cardinalityPostings().localCardinality()
	}
	syms := make(map[string]struct{})
	labels := make(map[string]*generationLabelCard)
	seen := make(map[signal.SeriesID]struct{})
	for source := h; source != nil; source = source.frozen {
		owner := source.cardinalityPostings()
		shared := make(map[uint32]struct{})
		view := owner.series.View()
		for i := range view.Len() {
			id := view.ID(i)
			if _, ok := seen[id]; ok {
				shared[uint32(i)] = struct{}{}
			}
			seen[id] = struct{}{}
		}
		for _, symbol := range owner.sym.Snapshot() {
			syms[string(symbol)] = struct{}{}
		}
		owner.post.ForEachName(func(nameID uint32, _ int, total int) {
			name, _ := owner.sym.Get(symbols.ID(nameID))
			card := labels[string(name)]
			if card == nil {
				card = &generationLabelCard{values: make(map[string]struct{})}
				labels[string(name)] = card
			}
			card.series += int64(total)
			for _, valueID := range owner.post.LabelValues(nameID) {
				value, _ := owner.sym.Get(symbols.ID(valueID))
				card.values[string(value)] = struct{}{}
				if len(shared) > 0 {
					card.series -= sharedPostingCount(owner, nameID, valueID, shared)
				}
			}
		})
	}
	out := make([]LabelCard, 0, len(labels))
	for name, card := range labels {
		out = append(out, LabelCard{Name: name, Series: card.series, DistinctValues: len(card.values)})
	}
	return len(syms), out
}

func sharedPostingCount(h *head, name, value uint32, shared map[uint32]struct{}) int64 {
	var count int64
	it := h.post.Get(name, value)
	for it.Next() {
		if _, ok := shared[uint32(it.At().Lo)]; ok {
			count++
		}
	}
	return count
}
