package seriesindex

import "github.com/oteldb/storage/backend"

func (c *Cursor) readPage(dst []byte, off, n int64) ([]byte, error) {
	if cache, ok := c.table.b.(backend.PageViewer); ok {
		data, err := cache.ReadPageView(c.ctx, c.table.key, off, n)
		if err != nil {
			return dst, err
		}
		// Cursor buffers are writable; cache views remain immutable across cursor reuse.
		return append(dst, data...), nil
	}
	return backend.ReadAtAppend(c.ctx, c.table.b, dst, c.table.key, off, n)
}
