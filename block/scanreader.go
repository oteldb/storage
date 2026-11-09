package block

import (
	"context"
	"io"
	"math/bits"
	"sync"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// OpenColumnScan reuses a file handle and pooled read-ahead buffer. The caller must Close the decoder.
func (r *PartReader) OpenColumnScan(ctx context.Context, name string, window int64) (*Decoder, error) {
	ctx = r.ReaderContext(ctx)
	d, err := r.ColumnScan(ctx, name, window)
	if err != nil {
		return nil, err
	}
	src := d.streams.dir.src
	if src == nil || src.window <= 0 {
		return d, nil
	}
	bytes, known := backend.ReaderMemory(src.b, src.key)
	if !known {
		return d, nil
	}
	reader, supported, err := backend.OpenReader(ctx, src.b, src.key)
	if err != nil {
		return nil, errors.Wrap(err, "open column reader")
	}
	if !supported {
		return d, nil
	}
	if reader == nil {
		return nil, errors.New("block: backend returned a nil object reader")
	}
	if desc, ok := r.ColumnDescByName(name); ok && desc.Bytes > 0 && desc.Bytes != reader.Size() {
		_ = reader.Close()
		return nil, errors.Wrap(ErrCorrupt, "opened column size changed")
	}
	src.reader, src.readerBytes = reader, bytes
	return d, nil
}

// Close releases an opened scan's handle and buffer; decoded views must no longer be used.
func (d *Decoder) Close() error {
	if d == nil || d.streams.dir.src == nil {
		return nil
	}
	s := d.streams.dir.src
	if s.closed {
		return nil
	}
	s.closed = true
	d.granules.retire()
	if s.reader == nil {
		return nil
	}
	reader := s.reader
	s.reader, s.readerBytes = nil, 0
	putRangeBuffer(s.ahead)
	s.ahead, s.lo, s.hi = nil, 0, 0
	return reader.Close()
}

const maxPooledRangeBuffer = 4 << 20

var rangeBuffers [23]sync.Pool

func getRangeBuffer(n, limit int64) []byte {
	if n <= 0 {
		return nil
	}
	capacity := int64(1) << bits.Len64(uint64(n-1))
	if capacity > limit || capacity > maxPooledRangeBuffer {
		return make([]byte, 0, int(n))
	}
	if buf, ok := rangeBuffers[bits.TrailingZeros64(uint64(capacity))].Get().(*[]byte); ok {
		return (*buf)[:0]
	}
	return make([]byte, 0, int(capacity))
}

func putRangeBuffer(buf []byte) {
	n := cap(buf)
	if n == 0 || n > maxPooledRangeBuffer || n&(n-1) != 0 {
		return
	}
	buf = buf[:0]
	rangeBuffers[bits.TrailingZeros(uint(n))].Put(&buf)
}

func (s *frameSource) fillReader(off, n int64) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	if int64(cap(s.ahead)) < n {
		putRangeBuffer(s.ahead)
		limit := min(max(s.window, n), int64(s.frameOff[len(s.frameOff)-1]))
		s.ahead = getRangeBuffer(n, limit)
	}
	s.ahead = s.ahead[:int(n)]
	read, err := s.reader.ReadAt(s.ahead, s.base+off)
	if err != nil && !errors.Is(err, io.EOF) {
		return errors.Wrapf(err, "read frames at [%d,+%d)", s.base+off, n)
	}
	if int64(read) != n {
		return errors.Wrapf(ErrCorrupt, "frames at [%d,+%d) are %d bytes", s.base+off, n, read)
	}
	return nil
}
