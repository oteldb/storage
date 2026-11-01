package engine

// The heap retains arrival order on timestamp ties; a range never overtakes another source.
type memorySampleHeap struct {
	runs      []tsRun
	positions []int
	indices   []int
}

func (h *memorySampleHeap) reset(runs []tsRun, positions []int) {
	h.runs, h.positions = runs, positions[:len(runs)]
	clear(h.positions)
	h.indices = h.indices[:0]
	for i := range runs {
		if len(runs[i].ts) > 0 {
			h.indices = append(h.indices, i)
		}
	}
	for i := len(h.indices)/2 - 1; i >= 0; i-- {
		h.down(i)
	}
}

func (h *memorySampleHeap) less(a, b int) bool {
	i, j := h.indices[a], h.indices[b]
	x, y := h.runs[i].ts[h.positions[i]], h.runs[j].ts[h.positions[j]]
	return x < y || x == y && i < j
}

func (h *memorySampleHeap) down(i int) {
	for {
		child := i*2 + 1
		if child >= len(h.indices) {
			return
		}
		if child+1 < len(h.indices) && h.less(child+1, child) {
			child++
		}
		if !h.less(child, i) {
			return
		}
		h.indices[i], h.indices[child] = h.indices[child], h.indices[i]
		i = child
	}
}

func (h *memorySampleHeap) next() (index, first, end int, ok bool) {
	if len(h.indices) == 0 {
		return 0, 0, 0, false
	}
	index = h.indices[0]
	first = h.positions[index]
	ts := h.runs[index].ts
	end = len(ts)
	if len(h.indices) > 1 {
		rival := 1
		if len(h.indices) > 2 && h.less(2, 1) {
			rival = 2
		}
		rivalIndex := h.indices[rival]
		boundary := h.runs[rivalIndex].ts[h.positions[rivalIndex]]
		if ts[first] == boundary {
			end = first + upperBound(ts[first:], boundary)
		} else {
			end = first + lowerBound(ts[first:], boundary)
		}
	}
	h.positions[index] = end
	if end == len(ts) {
		last := len(h.indices) - 1
		h.indices[0] = h.indices[last]
		h.indices = h.indices[:last]
	}
	h.down(0)
	return index, first, end, true
}
