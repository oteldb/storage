package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/encoding/compress"
)

const (
	tableMagic    = 0x4f534958
	footerSize    = 40
	directorySize = 40
	pageSize      = 64 << 10
	maxPageSize   = 64 << 20
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt denotes malformed or checksum-invalid index data.
var ErrCorrupt = errors.New("seriesindex: corrupt index")

type pageInfo struct {
	first              []byte
	offset             int64
	packed, raw, count uint32
}

// TableWriter appends strictly ordered keys to an immutable, prefix-compressed table.
type TableWriter struct {
	dst                      io.Writer
	codec                    *compress.Compressor
	pages                    []pageInfo
	buf, packed, last, first []byte
	offset                   int64
	count, total, maxRaw     uint32
	seriesCount              uint32
	limit                    int
	err                      error
}

// NewTableWriter constructs a streaming table writer; Finish does not close dst.
func NewTableWriter(dst io.Writer) *TableWriter {
	return &TableWriter{dst: dst, limit: pageSize,
		codec: compress.NewCompressor(compress.AlgorithmLZ4, compress.LevelDefault)}
}

// Append consumes key/value before returning. Keys must be strictly increasing.
func (w *TableWriter) Append(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if len(key)+len(value)+3*binary.MaxVarintLen64 > maxPageSize {
		return errors.New("seriesindex: record exceeds page limit")
	}
	if w.total == math.MaxUint32 {
		return errors.New("seriesindex: too many records")
	}
	if w.total > 0 && bytes.Compare(w.last, key) >= 0 {
		return errors.New("seriesindex: keys are not strictly increasing")
	}
	if w.count > 0 && len(w.buf)+len(key)+len(value)+30 > w.limit {
		if err := w.flush(); err != nil {
			return err
		}
	}
	prefix := 0
	if w.count == 0 {
		w.first = append(w.first[:0], key...)
	} else {
		for prefix < min(len(w.last), len(key)) && w.last[prefix] == key[prefix] {
			prefix++
		}
	}
	w.buf = binary.AppendUvarint(w.buf, uint64(prefix))
	w.buf = binary.AppendUvarint(w.buf, uint64(len(key)-prefix))
	w.buf = append(w.buf, key[prefix:]...)
	w.buf = binary.AppendUvarint(w.buf, uint64(len(value)))
	w.buf = append(w.buf, value...)
	w.last = append(w.last[:0], key...)
	w.count++
	w.total++
	return nil
}

// Finish writes the sparse directory and checksummed footer.
func (w *TableWriter) Finish() error {
	if w.err != nil {
		return w.err
	}
	if err := w.flush(); err != nil {
		return err
	}
	dir := make([]byte, 0, len(w.pages)*directorySize)
	for _, p := range w.pages {
		entry := len(dir)
		dir = binary.BigEndian.AppendUint64(dir, uint64(w.offset))
		dir = binary.BigEndian.AppendUint32(dir, uint32(len(p.first)))
		dir = binary.BigEndian.AppendUint32(dir, crc32.Checksum(p.first, checksumTable))
		dir = binary.BigEndian.AppendUint64(dir, uint64(p.offset))
		dir = binary.BigEndian.AppendUint32(dir, p.packed)
		dir = binary.BigEndian.AppendUint32(dir, p.raw)
		dir = binary.BigEndian.AppendUint32(dir, p.count)
		dir = binary.BigEndian.AppendUint32(dir, crc32.Checksum(dir[entry:], checksumTable))
		if err := w.write(p.first); err != nil {
			return err
		}
	}
	dirOffset := w.offset
	if err := w.write(dir); err != nil {
		return err
	}
	var footer [footerSize]byte
	binary.BigEndian.PutUint32(footer[:], tableMagic)
	binary.BigEndian.PutUint32(footer[4:], 1)
	binary.BigEndian.PutUint64(footer[8:], uint64(dirOffset))
	binary.BigEndian.PutUint32(footer[16:], uint32(len(w.pages)))
	binary.BigEndian.PutUint32(footer[20:], w.total)
	binary.BigEndian.PutUint32(footer[24:], w.maxRaw)
	binary.BigEndian.PutUint32(footer[28:], w.seriesCount)
	binary.BigEndian.PutUint32(footer[36:], crc32.Checksum(footer[:36], checksumTable))
	return w.write(footer[:])
}

// Table retains only a footer; directories and records are fetched through ranged reads.
type Table struct {
	b                    backend.Backend
	key                  string
	dir                  int64
	pages, count, maxRaw uint32
	seriesCount          uint32
	codec                *compress.Compressor
}

// OpenTable validates the fixed footer without loading a per-key directory.
func OpenTable(ctx context.Context, b backend.Backend, key string) (*Table, error) {
	size, known, err := backend.NativeSizeOf(ctx, b, key)
	if err != nil {
		return nil, err
	}
	if !known {
		size, err = backend.SizeOf(ctx, b, key)
		if err != nil {
			return nil, err
		}
	}
	if size < footerSize {
		return nil, errors.Wrap(ErrCorrupt, "short footer")
	}
	data, err := backend.ReadAt(ctx, b, key, size-footerSize, footerSize)
	if err != nil {
		return nil, err
	}
	if len(data) != footerSize || binary.BigEndian.Uint32(data) != tableMagic ||
		binary.BigEndian.Uint32(data[4:]) != 1 ||
		crc32.Checksum(data[:36], checksumTable) != binary.BigEndian.Uint32(data[36:]) {
		return nil, errors.Wrap(ErrCorrupt, "footer")
	}
	dir := binary.BigEndian.Uint64(data[8:])
	pages, count, raw := binary.BigEndian.Uint32(data[16:]), binary.BigEndian.Uint32(data[20:]), binary.BigEndian.Uint32(data[24:])
	if dir > uint64(size-footerSize) || uint64(pages)*directorySize != uint64(size-footerSize)-dir ||
		(pages == 0) != (count == 0) || pages > count || raw > maxPageSize || (pages > 0 && raw == 0) {
		return nil, errors.Wrap(ErrCorrupt, "directory bounds")
	}
	return &Table{b: b, key: key, dir: int64(dir), pages: pages, count: count, maxRaw: raw,
		seriesCount: binary.BigEndian.Uint32(data[28:]),
		codec:       compress.NewCompressor(compress.AlgorithmLZ4, compress.LevelDefault)}, nil
}

// Count returns the number of stored keys.
func (t *Table) Count() uint32 { return t.count }

// MaxPageBytes bounds the encoded/decoded buffers retained by one cursor.
func (t *Table) MaxPageBytes() int64 { return int64(t.maxRaw)*2 + 64 }

// Cursor returns a forward cursor with caller-owned reusable buffers.
func (t *Table) Cursor(ctx context.Context) *Cursor { return &Cursor{table: t, ctx: ctx, page: -1} }

// Cursor yields key/value views valid until its next operation.
type Cursor struct {
	table                                     *Table
	ctx                                       context.Context //nolint:containedctx // The cursor owns its scoped I/O lifetime.
	page                                      int64
	left                                      uint32
	off                                       int
	key, value, packed, raw, first, directory []byte
	err                                       error
}

// Next advances to the next record, including the first before any seek.
func (c *Cursor) Next() bool {
	if c.err != nil {
		return false
	}
	if c.page >= 0 && c.left > 0 {
		return c.advance()
	}
	i := c.page + 1
	if i >= int64(c.table.pages) {
		return false
	}
	return c.load(uint32(i))
}

// Seek positions at the first key greater than or equal to key.
func (c *Cursor) Seek(key []byte) bool {
	if c.err != nil || c.table.pages == 0 {
		return false
	}
	lo, hi := uint32(0), c.table.pages
	for lo < hi {
		mid := lo + (hi-lo)/2
		p, err := c.entry(mid)
		if err != nil {
			c.err = err
			return false
		}
		if bytes.Compare(p.first, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	i := uint32(0)
	if lo > 0 {
		i = lo - 1
	}
	if c.page != int64(i) || bytes.Compare(c.key, key) > 0 {
		if !c.load(i) {
			return false
		}
	}
	if c.page < 0 {
		return false
	}
	for bytes.Compare(c.key, key) < 0 {
		if !c.Next() {
			return false
		}
	}
	return true
}

// Key returns the current borrowed key.
func (c *Cursor) Key() []byte { return c.key }

// Value returns the current borrowed value.
func (c *Cursor) Value() []byte { return c.value }

// Err reports a terminal I/O or format error.
func (c *Cursor) Err() error { return c.err }

func (w *TableWriter) write(data []byte) error {
	n, err := w.dst.Write(data)
	if n < 0 || n > len(data) {
		err = io.ErrShortWrite
		n = 0
	}
	w.offset += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	w.err = err
	return err
}

func (w *TableWriter) flush() error {
	if w.count == 0 {
		return nil
	}
	w.packed = w.codec.Compress(w.packed[:0], w.buf)
	crc := crc32.Checksum(w.packed, checksumTable)
	w.packed = binary.BigEndian.AppendUint32(w.packed, crc)
	w.pages = append(w.pages, pageInfo{first: bytes.Clone(w.first), offset: w.offset,
		packed: uint32(len(w.packed)), raw: uint32(len(w.buf)), count: w.count})
	w.maxRaw = max(w.maxRaw, uint32(len(w.buf)))
	if err := w.write(w.packed); err != nil {
		return err
	}
	w.buf, w.count = w.buf[:0], 0
	return nil
}

func (c *Cursor) entry(i uint32) (pageInfo, error) {
	var err error
	c.directory, err = backend.ReadAtAppend(c.ctx, c.table.b, c.directory[:0], c.table.key,
		c.table.dir+int64(i)*directorySize, directorySize)
	if err != nil {
		return pageInfo{}, err
	}
	d := c.directory
	if len(d) != directorySize || crc32.Checksum(d[:36], checksumTable) != binary.BigEndian.Uint32(d[36:]) {
		return pageInfo{}, errors.Wrap(ErrCorrupt, "directory entry")
	}
	keyOff, keyLen := binary.BigEndian.Uint64(d), binary.BigEndian.Uint32(d[8:])
	off := binary.BigEndian.Uint64(d[16:])
	packed, raw, count := binary.BigEndian.Uint32(d[24:]), binary.BigEndian.Uint32(d[28:]), binary.BigEndian.Uint32(d[32:])
	if keyOff > uint64(c.table.dir) || uint64(keyLen) > uint64(c.table.dir)-keyOff || keyLen > maxPageSize ||
		off > uint64(c.table.dir) || uint64(packed) > uint64(c.table.dir)-off || packed < 5 ||
		raw > c.table.maxRaw || raw == 0 || packed > raw+5 || count == 0 || count > raw/3 {
		return pageInfo{}, errors.Wrap(ErrCorrupt, "page bounds")
	}
	c.first, err = backend.ReadAtAppend(c.ctx, c.table.b, c.first[:0], c.table.key, int64(keyOff), int64(keyLen))
	if err != nil {
		return pageInfo{}, err
	}
	if len(c.first) != int(keyLen) || crc32.Checksum(c.first, checksumTable) != binary.BigEndian.Uint32(d[12:]) {
		return pageInfo{}, errors.Wrap(ErrCorrupt, "directory key")
	}
	return pageInfo{first: c.first, offset: int64(off), packed: packed, raw: raw, count: count}, nil
}

func (c *Cursor) load(i uint32) bool {
	p, err := c.entry(i)
	if err != nil {
		c.err = err
		return false
	}
	c.packed, err = backend.ReadAtAppend(c.ctx, c.table.b, c.packed[:0], c.table.key, p.offset, int64(p.packed))
	if err != nil {
		c.err = err
		return false
	}
	if len(c.packed) != int(p.packed) {
		c.err = errors.Wrap(ErrCorrupt, "page read length")
		return false
	}
	if crc32.Checksum(c.packed[:len(c.packed)-4], checksumTable) != binary.BigEndian.Uint32(c.packed[len(c.packed)-4:]) {
		c.err = errors.Wrap(ErrCorrupt, "page checksum")
		return false
	}
	c.raw, err = c.table.codec.DecompressLimit(c.raw[:0], c.packed[:len(c.packed)-4], int(p.raw))
	if err != nil {
		c.err = errors.Wrap(ErrCorrupt, "page decompression")
		return false
	}
	if len(c.raw) != int(p.raw) {
		c.err = errors.Wrap(ErrCorrupt, "page length")
		return false
	}
	c.page, c.left, c.off = int64(i), p.count, 0
	c.key = c.key[:0]
	if !c.advance() {
		return false
	}
	if !bytes.Equal(c.key, p.first) {
		c.err = errors.Wrap(ErrCorrupt, "first page key")
		return false
	}
	return true
}

func (c *Cursor) number() (uint64, bool) {
	n, size := binary.Uvarint(c.raw[c.off:])
	if size <= 0 {
		c.err = errors.Wrap(ErrCorrupt, "record integer")
		return 0, false
	}
	c.off += size
	return n, true
}

func (c *Cursor) advance() bool {
	prefix, ok := c.number()
	if !ok {
		return false
	}
	suffix, ok := c.number()
	if !ok {
		return false
	}
	if prefix > uint64(len(c.key)) || suffix > uint64(len(c.raw)-c.off) {
		c.err = errors.Wrap(ErrCorrupt, "record key")
		return false
	}
	c.key = append(c.key[:int(prefix)], c.raw[c.off:c.off+int(suffix)]...)
	c.off += int(suffix)
	n, ok := c.number()
	if !ok {
		return false
	}
	if n > uint64(len(c.raw)-c.off) {
		c.err = errors.Wrap(ErrCorrupt, "record value")
		return false
	}
	c.value = c.raw[c.off : c.off+int(n)]
	c.off += int(n)
	c.left--
	if c.left == 0 && c.off != len(c.raw) {
		c.err = errors.Wrap(ErrCorrupt, "page trailing bytes")
		return false
	}
	return true
}
