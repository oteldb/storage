package recordengine

import (
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

const (
	headReuseBytes = 1 << 20
	headReuseCount = 1024
)

func (c *recordCols) headCapacity() int64 {
	n := memsize.Of[recordCols]() + memsize.Slice(c.ts) + memsize.Slice(c.ints) + memsize.Slice(c.bytes) +
		memsize.Slice(c.sel.ints) + memsize.Slice(c.sel.bytes)
	for _, col := range c.ints {
		n += memsize.Slice(col)
	}
	for _, col := range c.bytes {
		n += memsize.Slice(col.data) + memsize.Slice(col.offsets)
	}
	return n
}

func (h *head) takeRecordCols() *recordCols {
	c := h.reusable
	if c == nil {
		return newRecordCols(h.schema, 0, fullSel(h.schema))
	}
	h.reusable = c.reuseNext
	c.reuseNext = nil
	h.reusableBytes -= c.headCapacity()
	h.reusableCount--
	return c
}

func (h *head) clearReusable() {
	h.reusable, h.reusableBytes, h.reusableCount = nil, 0, 0
}

func (h *head) reusableSize() int64 { return h.reusableBytes }

// recycleRecordCols runs only after publication excludes readers of the frozen generation.
func (h *head) recycleRecordCols(records map[signal.SeriesID]*recordCols) {
	h.clearReusable()
	for _, c := range records {
		capacity := c.headCapacity()
		if h.reusableCount >= headReuseCount || capacity > headReuseBytes-h.reusableBytes ||
			capacity > max(4096, 4*c.byteSize()) {
			continue
		}
		c.prepare(h.schema, 0, c.sel)
		c.rowScratch, c.orderScratch, c.viewBufs, c.permBufs = nil, nil, nil, nil
		c.reuseNext = h.reusable
		h.reusable = c
		h.reusableCount++
		h.reusableBytes += capacity
	}
}

func (h *head) admissionBytes(maxBytes, pending int64) int64 {
	n := h.inFlightBytes() + pending + h.reusableBytes
	if h.reusableBytes > 0 && (n >= headByteCap || (maxBytes > 0 && n >= maxBytes)) {
		h.clearReusable()
		n = h.inFlightBytes() + pending
	}
	return n
}
