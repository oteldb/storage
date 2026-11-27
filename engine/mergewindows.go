package engine

import (
	"container/heap"
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/internal/timebucket"
)

const (
	mergeSeriesWindow = 512
	mergeSmallSeries  = 8 * mergeSeriesWindow
)

func plainMergeInputs(parts []*part, tiers []DownsampleTier) bool {
	if len(tiers) != 0 {
		return false
	}
	for _, p := range parts {
		if p.hasSF || len(p.rollup) != 0 {
			return false
		}
	}
	return true
}

func largeMergeSeries(ranges []rowRange) bool {
	var rows int
	for _, r := range ranges {
		rows += r.end - r.start
		if rows > mergeSmallSeries {
			return true
		}
	}
	return false
}

type mergeWindowSource struct {
	buf    rangeBuf
	row    rowRange
	at     int
	ts     []int64
	values []float64
}

func (s *mergeWindowSource) advance(stream *partStream) error {
	s.at++
	if s.at < len(s.ts) || s.row.start == s.row.end {
		return nil
	}
	end := min(s.row.start+mergeSeriesWindow, s.row.end)
	ts, values, _, err := stream.decodeRange(rowRange{start: s.row.start, end: end}, &s.buf)
	if err != nil {
		return err
	}
	s.ts, s.values, s.at, s.row.start = ts, values, 0, end
	return nil
}

type mergeSampleHeap struct {
	sources []mergeWindowSource
	order   []int
}

func (h *mergeSampleHeap) Len() int      { return len(h.order) }
func (h *mergeSampleHeap) Swap(i, j int) { h.order[i], h.order[j] = h.order[j], h.order[i] }
func (h *mergeSampleHeap) Less(i, j int) bool {
	a, b := h.order[i], h.order[j]
	x, y := &h.sources[a], &h.sources[b]
	return x.ts[x.at] < y.ts[y.at] || x.ts[x.at] == y.ts[y.at] && a < b
}
func (h *mergeSampleHeap) Push(v any) { h.order = append(h.order, v.(int)) }
func (h *mergeSampleHeap) Pop() any {
	i := len(h.order) - 1
	v := h.order[i]
	h.order = h.order[:i]
	return v
}

func (h *mergeSampleHeap) next(streams []*partStream) (int64, float64, error) {
	first := &h.sources[h.order[0]]
	ts := first.ts[first.at]
	var value float64
	// Consume ties even across windows. Later sources and later rows in a source win.
	for h.Len() != 0 {
		i := h.order[0]
		s := &h.sources[i]
		next := s.ts[s.at]
		if next != ts {
			if next < ts {
				return 0, 0, errors.New("engine: unsorted metric merge input")
			}
			break
		}
		value = s.values[s.at]
		if err := s.advance(streams[i]); err != nil {
			return 0, 0, err
		}
		if s.at == len(s.ts) {
			heap.Pop(h)
		} else {
			heap.Fix(h, 0)
		}
	}
	return ts, value, nil
}

// mergeSeriesWindows uses one decoded window per input for raw, unweighted series.
type mergeSeriesWindows struct {
	heap mergeSampleHeap
	ts   []int64
	vals []float64
}

func (m *mergeSeriesWindows) merge(ctx context.Context, streams []*partStream, ranges []rowRange,
	start, covered int64, emit func([]int64, []float64, int64) error,
) error {
	if m.heap.sources == nil {
		m.heap.sources = make([]mergeWindowSource, len(streams))
		m.heap.order = make([]int, 0, len(streams))
		m.ts, m.vals = make([]int64, 0, mergeSeriesWindow), make([]float64, 0, mergeSeriesWindow)
	}
	m.heap.order, m.ts, m.vals = m.heap.order[:0], m.ts[:0], m.vals[:0]
	for i, r := range ranges {
		s := &m.heap.sources[i]
		s.row, s.ts, s.values, s.at = r, nil, nil, -1
		if r.start == r.end {
			continue
		}
		if err := s.advance(streams[i]); err != nil {
			return err
		}
		m.heap.order = append(m.heap.order, i)
	}
	heap.Init(&m.heap)
	for m.heap.Len() != 0 {
		if len(m.ts) == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		ts, value, err := m.heap.next(streams)
		if err != nil {
			return err
		}
		if ts < start {
			continue
		}
		n := len(m.ts)
		if n != 0 && (n == cap(m.ts) || timebucket.Of(m.ts[0], timebucket.Top()) != timebucket.Of(ts, timebucket.Top())) {
			if err := emit(m.ts, m.vals, m.ts[n-1]); err != nil {
				return err
			}
			m.ts, m.vals = m.ts[:0], m.vals[:0]
		}
		m.ts = append(m.ts, ts)
		m.vals = append(m.vals, value)
	}
	if n := len(m.ts); n != 0 {
		return emit(m.ts, m.vals, max(covered, m.ts[n-1]))
	}
	return nil
}
