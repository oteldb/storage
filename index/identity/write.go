package identity

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/index/series"
	"github.com/oteldb/storage/index/symbols"
	"github.com/oteldb/storage/signal"
)

// WriteSorted streams n sorted identities in the existing format. A buffered dst coalesces records.
func WriteSorted(dst io.Writer, n int, entry func(int, signal.Attributes) series.Entry) (int64, error) {
	if n < 0 {
		return 0, errors.New("identity: negative entry count")
	}
	tab := symbols.New()
	defer tab.Release()
	enc := encoder{tab: tab}
	w := sectionWriter{dst: dst}
	var header [headerLen]byte
	binary.BigEndian.PutUint32(header[:], magic)
	header[4] = version
	if _, err := w.Write(header[:]); err != nil {
		return w.written, err
	}
	w.crc = 0
	var scratch [512]byte
	if _, err := w.Write(binary.AppendUvarint(scratch[:0], uint64(n))); err != nil {
		return w.written, err
	}
	var attrs signal.Attributes
	record := scratch[:0]
	for i := range n {
		ent := entry(i, attrs)
		record = enc.appendSeries(record[:0], ent)
		if _, err := w.Write(record); err != nil {
			return w.written, err
		}
		attrs = ent.Series.Attributes
	}
	bodyLen, bodyCRC := w.written-headerLen, w.crc
	w.crc = 0
	if _, err := tab.WriteTo(&w); err != nil {
		return w.written, err
	}
	symbolLen, symbolCRC := w.written-headerLen-bodyLen, w.crc
	tocOff := w.written
	if tocOff > math.MaxUint32 {
		return w.written, errors.New("identity: object exceeds TOC offset limit")
	}
	toc := binary.AppendUvarint(scratch[:0], 2)
	toc = appendTOC(toc, sectionSeries, headerLen, bodyLen, bodyCRC)
	toc = appendTOC(toc, sectionSymbols, headerLen+bodyLen, symbolLen, symbolCRC)
	trailer := crc32.Checksum(toc, castagnoli)
	toc = binary.BigEndian.AppendUint32(toc, uint32(tocOff))
	toc = binary.BigEndian.AppendUint32(toc, trailer)
	_, err := w.Write(toc)
	return w.written, err
}

func appendTOC(dst []byte, kind byte, off, length int64, checksum uint32) []byte {
	dst = append(dst, kind)
	dst = binary.AppendUvarint(dst, uint64(off))
	dst = binary.AppendUvarint(dst, uint64(length))
	return binary.BigEndian.AppendUint32(dst, checksum)
}

type sectionWriter struct {
	dst     io.Writer
	written int64
	crc     uint32
}

func (w *sectionWriter) Write(data []byte) (int, error) {
	n, err := w.dst.Write(data)
	if n < 0 || n > len(data) {
		return 0, io.ErrShortWrite
	}
	w.written += int64(n)
	w.crc = crc32.Update(w.crc, castagnoli, data[:n])
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}
