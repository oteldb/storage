package recordengine

import (
	"slices"

	"github.com/oteldb/storage/encoding/chunk"
	"github.com/oteldb/storage/signal"
)

// runHeap merges one stream's runs, one per source holding it, by (timestamp, source): the order a
// stable sort of the runs concatenated in source order gives, without holding the stream.
type runHeap struct {
	items []heapRun
}

type heapRun struct {
	ts  int64
	src int
	run mergeRun
}

func (a heapRun) less(b heapRun) bool {
	return a.ts < b.ts || (a.ts == b.ts && a.src < b.src)
}

// reset arms the heap with every source's run of stream id from start on.
func (h *runHeap) reset(sources []mergeSource, id signal.SeriesID, start int64) error {
	clear(h.items)
	h.items = h.items[:0]

	for si, s := range sources {
		r, ok, err := s.run(id, start)
		if err != nil {
			return sourceErr(si, err)
		}

		if !ok {
			continue
		}

		if err := h.push(si, r); err != nil {
			return err
		}
	}

	for i := len(h.items)/2 - 1; i >= 0; i-- {
		h.down(i)
	}

	return nil
}

func (h *runHeap) push(si int, r mergeRun) error {
	ts, err := r.ahead()
	if err != nil {
		return err
	}

	if len(ts) > 0 {
		h.items = append(h.items, heapRun{ts: ts[0], src: si, run: r})
	}

	return nil
}

func (h *runHeap) len() int { return len(h.items) }

// fill appends the stream's next rows in merge order to w, at most up to w's next granule boundary
// and — when runBytes > 0 — until runBytes of decoded rows, and only rows up to dayEnd: the rows the
// router may hand one day's writer before checking it for sealing.
func (h *runHeap) fill(w *recordPartStreamWriter, u chunk.U128, dayEnd, runBytes int64) error {
	rows := w.granule - w.rows%w.granule

	var bytes int64

	for rows > 0 && len(h.items) > 0 && (runBytes <= 0 || bytes < runBytes) {
		top := &h.items[0]
		if top.ts > dayEnd {
			return nil
		}

		ts, err := top.run.ahead()
		if err != nil {
			return err
		}

		n := min(len(ts), rows, upTo(ts, dayEnd, true))
		if len(h.items) > 1 {
			n = min(n, h.before(ts))
		}

		var size int64

		m := 0
		for m < n && (runBytes <= 0 || bytes+size < runBytes) {
			size += top.run.rowBytes(m)
			m++
		}

		if err := w.appendRows(u, ts[:m], size); err != nil {
			return err
		}

		if err := top.run.emit(w, top.src, m); err != nil {
			return sourceErr(top.src, err)
		}

		rows -= m
		bytes += size

		if err := h.advance(); err != nil {
			return err
		}
	}

	return nil
}

// before returns how many of ts, the root run's rows ahead, order before the next-best run's head.
// The root orders before it, so the count is at least one.
func (h *runHeap) before(ts []int64) int {
	next := h.items[1]
	if len(h.items) > 2 && h.items[2].less(next) {
		next = h.items[2]
	}

	// Equal timestamps go to the lower source first.
	return upTo(ts, next.ts, h.items[0].src < next.src)
}

// upTo returns how many of the ascending ts are below bound, or at most bound when inclusive.
func upTo(ts []int64, bound int64, inclusive bool) int {
	n, _ := slices.BinarySearchFunc(ts, bound, func(t, b int64) int {
		if t < b || (inclusive && t == b) {
			return -1
		}

		return 1
	})

	return n
}

// advance re-seats the root after it emitted rows, dropping it once drained.
func (h *runHeap) advance() error {
	top := &h.items[0]

	ts, err := top.run.ahead()
	if err != nil {
		return err
	}

	if len(ts) > 0 {
		top.ts = ts[0]
	} else {
		last := len(h.items) - 1
		h.items[0] = h.items[last]
		h.items[last] = heapRun{}
		h.items = h.items[:last]
	}

	if len(h.items) > 0 {
		h.down(0)
	}

	return nil
}

func (h *runHeap) down(i int) {
	it := h.items

	for {
		l := 2*i + 1
		if l >= len(it) {
			return
		}

		m := l
		if r := l + 1; r < len(it) && it[r].less(it[l]) {
			m = r
		}

		if !it[m].less(it[i]) {
			return
		}

		it[i], it[m] = it[m], it[i]
		i = m
	}
}
