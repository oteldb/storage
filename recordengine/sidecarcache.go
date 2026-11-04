package recordengine

import "github.com/oteldb/storage/encoding/chunk"

// sidecarCache stores token counts plus one, so zero means an entry has not been emitted.
type sidecarCache struct {
	tokens []uint32
	rows   int
}

func (c *sidecarCache) reset() {
	clear(c.tokens)
	c.tokens = c.tokens[:0]
	c.rows = 0
}

func (c *sidecarCache) residentBytes() int64 { return int64(cap(c.tokens)) * 4 }

func (c *sidecarCache) add(s *columnSink, dc *chunk.DictColumn, lo, hi int) {
	if dc.IDWidth != 0 && len(c.tokens) == 0 {
		c.rows += hi - lo
		// Wait for reuse before reserving one token count per dictionary entry.
		if n := len(dc.Entries); c.rows > 2*n {
			if cap(c.tokens) < n {
				c.tokens = make([]uint32, n)
			} else {
				c.tokens = c.tokens[:n]
			}
		}
	}
	if dc.IDWidth == 0 || len(c.tokens) == 0 {
		for row := lo; row < hi; row++ {
			s.add(dc.At(row))
		}
		return
	}
	for row := lo; row < hi; row++ {
		id := dictEntryID(dc, row)
		v := dc.Entries[id]
		count := c.tokens[id]
		if count == 0 {
			count = s.observe(v) + 1
			c.tokens[id] = count
		}
		s.account(len(v), count-1)
	}
}
