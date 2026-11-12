package seriesindex

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

const (
	decodedHeader   = 16
	decodedExpanded = 1
	decodedFormat   = "seriesindex/page-v1/offsets-v1"
)

// DecodePage implements [backend.PageDecoder]. The cursor is used only on cache misses.
func (c *Cursor) DecodePage(packed []byte) ([]byte, error) {
	p := c.decoding
	if len(packed) != int(p.packed) {
		return nil, errors.Wrap(ErrCorrupt, "page read length")
	}
	if crc32.Checksum(packed[:len(packed)-4], checksumTable) != binary.BigEndian.Uint32(packed[len(packed)-4:]) {
		return nil, errors.Wrap(ErrCorrupt, "page checksum")
	}
	raw, err := c.table.codec.DecompressLimit(nil, packed[:len(packed)-4], int(p.raw))
	if err != nil {
		return nil, errors.Wrap(ErrCorrupt, "page decompression")
	}
	if len(raw) != int(p.raw) {
		return nil, errors.Wrap(ErrCorrupt, "page length")
	}
	v := Cursor{table: c.table, raw: raw, left: p.count}
	size := int64(decodedHeader) + int64(p.count)*8
	for v.left > 0 {
		if !v.advance() {
			return nil, v.err
		}
		if v.left == p.count-1 && !bytes.Equal(v.key, p.first) {
			return nil, errors.Wrap(ErrCorrupt, "first page key")
		}
		size += int64(len(v.key)) + int64(len(v.value))
	}
	// Prefix expansion is bounded before allocation; pathological shared keys stay compact.
	if size > int64(p.raw)*3+decodedHeader {
		out := make([]byte, decodedHeader+len(v.key)+len(raw))
		binary.BigEndian.PutUint32(out[4:], p.count)
		binary.BigEndian.PutUint32(out[8:], p.raw)
		binary.BigEndian.PutUint32(out[12:], uint32(len(v.key)))
		copy(out[decodedHeader:], v.key)
		copy(out[decodedHeader+len(v.key):], raw)
		return out, nil
	}
	out := make([]byte, decodedHeader+int(p.count)*8, int(size))
	out[0] = decodedExpanded
	binary.BigEndian.PutUint32(out[4:], p.count)
	binary.BigEndian.PutUint32(out[8:], p.raw)
	v.key, v.off, v.left = v.key[:0], 0, p.count
	for i := range p.count {
		if !v.advance() {
			return nil, v.err
		}
		out = append(out, v.key...)
		binary.BigEndian.PutUint32(out[decodedHeader+int(i)*8:], uint32(len(out)))
		out = append(out, v.value...)
		binary.BigEndian.PutUint32(out[decodedHeader+int(i)*8+4:], uint32(len(out)))
	}
	return out, nil
}

func (c *Cursor) loadDecoded(cache backend.DecodedPageViewer, i uint32, p pageInfo) bool {
	c.decoding = p
	data, err := cache.ReadDecodedPage(c.ctx, c.table.key, p.offset, int64(p.packed), decodedFormat, c)
	if err != nil {
		c.err = err
		return false
	}
	if len(data) < decodedHeader || binary.BigEndian.Uint32(data[4:]) != p.count || binary.BigEndian.Uint32(data[8:]) != p.raw {
		c.err = errors.Wrap(ErrCorrupt, "decoded page bounds")
		return false
	}
	if c.page >= 0 && int64(i) == c.page+1 && bytes.Compare(c.last, p.first) >= 0 {
		c.err = errors.Wrap(ErrCorrupt, "page keys are not ordered")
		return false
	}
	// None of these borrowed slices may become writable scratch on the next load/reset.
	c.key, c.value, c.last, c.raw, c.packed = nil, nil, nil, nil, nil
	c.decoded, c.page, c.left, c.off = data, int64(i), p.count, 0
	if data[0] == decodedExpanded {
		c.last = c.decodedKey(p.count - 1)
		c.selectDecoded(0)
	} else {
		end := decodedHeader + int(binary.BigEndian.Uint32(data[12:]))
		c.last, c.raw = data[decodedHeader:end:end], data[end:len(data):len(data)]
		if !c.advance() {
			return false
		}
	}
	if !bytes.Equal(c.key, p.first) {
		c.err = errors.Wrap(ErrCorrupt, "first page key")
		return false
	}
	return true
}

func (c *Cursor) decodedKey(i uint32) []byte {
	start := decodedHeader + int(c.decoding.count)*8
	if i > 0 {
		start = int(binary.BigEndian.Uint32(c.decoded[decodedHeader+int(i-1)*8+4:]))
	}
	end := int(binary.BigEndian.Uint32(c.decoded[decodedHeader+int(i)*8:]))
	return c.decoded[start:end:end]
}

func (c *Cursor) selectDecoded(i uint32) {
	c.decodedIndex = i
	c.key = c.decodedKey(i)
	endKey := int(binary.BigEndian.Uint32(c.decoded[decodedHeader+int(i)*8:]))
	endValue := int(binary.BigEndian.Uint32(c.decoded[decodedHeader+int(i)*8+4:]))
	c.value = c.decoded[endKey:endValue:endValue]
	c.left = c.decoding.count - i - 1
}

func (c *Cursor) seekDecoded(key []byte) bool {
	if len(c.decoded) > 0 && c.decoded[0] == decodedExpanded &&
		bytes.Compare(key, c.decodedKey(0)) >= 0 && bytes.Compare(key, c.last) <= 0 {
		lo, hi := uint32(0), c.decoding.count
		for lo < hi {
			mid := lo + (hi-lo)/2
			if bytes.Compare(c.decodedKey(mid), key) < 0 {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		c.selectDecoded(lo)
		return true
	}
	return false
}
